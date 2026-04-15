from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.domain.statuses import ReviewedTradeStatus, WorkflowActor
from vinayak.messaging.events import (
    EVENT_REVIEWED_TRADE_CREATED,
    EVENT_REVIEWED_TRADE_STATUS_UPDATED,
)
from vinayak.messaging.outbox import OutboxService
from vinayak.services.trade_execution_workflow import TradeExecutionWorkflowService


@dataclass(slots=True)
class ReviewedTradeCreateCommand:
    signal_id: int | None = None
    strategy_name: str | None = None
    symbol: str | None = None
    side: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    quantity: int = 1
    lots: int = 1
    status: str = "REVIEWED"
    notes: str | None = None


@dataclass(slots=True)
class ReviewedTradeStatusUpdateCommand:
    reviewed_trade_id: int
    status: str
    notes: str | None = None
    quantity: int | None = None
    lots: int | None = None


class ReviewedTradeService:
    def __init__(
        self,
        session: Session,
        *,
        reviewed_trade_repository: ReviewedTradeRepository | None = None,
        signal_repository: SignalRepository | None = None,
        outbox: OutboxService | None = None,
    ) -> None:
        self.session = session
        self.reviewed_trade_repository = reviewed_trade_repository or ReviewedTradeRepository(
            session
        )
        self.signal_repository = signal_repository or SignalRepository(session)
        self.outbox = outbox or OutboxService(session)
        self.workflow = TradeExecutionWorkflowService(
            session,
            reviewed_trade_repository=self.reviewed_trade_repository,
            outbox=self.outbox,
        )

    def list_reviewed_trades(self) -> list[ReviewedTradeRecord]:
        return self.reviewed_trade_repository.list_reviewed_trades()

    def create_reviewed_trade(
        self,
        command: ReviewedTradeCreateCommand,
    ) -> ReviewedTradeRecord:
        payload = self._resolve_create_payload(command)
        record = self.reviewed_trade_repository.create_reviewed_trade(**payload)
        self._enqueue_reviewed_created_event(record)
        self.session.commit()
        self.session.refresh(record)
        return record

    def create_reviewed_trade_from_signal(
        self,
        *,
        signal_id: int,
        quantity: int = 1,
        lots: int = 1,
        status: str = "REVIEWED",
        notes: str | None = None,
    ) -> ReviewedTradeRecord:
        return self.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                signal_id=signal_id,
                quantity=quantity,
                lots=lots,
                status=status,
                notes=notes,
            )
        )

    def update_reviewed_trade_status(
        self,
        command: ReviewedTradeStatusUpdateCommand,
    ) -> ReviewedTradeRecord:
        record = self.reviewed_trade_repository.get_reviewed_trade(command.reviewed_trade_id)
        if record is None:
            raise ValueError(f"Reviewed trade {command.reviewed_trade_id} was not found.")

        status = str(command.status or "").upper().strip()
        try:
            target_status = ReviewedTradeStatus(status)
        except ValueError as exc:
            raise ValueError(f"Unsupported reviewed trade status: {command.status}") from exc

        if target_status == ReviewedTradeStatus.APPROVED:
            updated = self.workflow.approve_reviewed_trade(
                record,
                actor=WorkflowActor.REVIEW_SERVICE.value,
                reason=command.notes,
                notes=command.notes,
                quantity=command.quantity,
                lots=command.lots,
            )
        elif target_status == ReviewedTradeStatus.REJECTED:
            updated = self.workflow.reject_reviewed_trade(
                record,
                actor=WorkflowActor.REVIEW_SERVICE.value,
                reason=command.notes,
                notes=command.notes,
            )
        else:
            updated = self.workflow.transition_reviewed_trade(
                record,
                target_status,
                context=None,
                notes=command.notes,
                quantity=command.quantity,
                lots=command.lots,
            )
        self._enqueue_reviewed_status_updated_event(updated)
        self.session.commit()
        self.session.refresh(updated)
        return updated

    def mark_executed(
        self,
        reviewed_trade_id: int,
        *,
        notes: str | None = None,
        auto_commit: bool = True,
    ) -> ReviewedTradeRecord:
        record = self.reviewed_trade_repository.get_reviewed_trade(reviewed_trade_id)
        if record is None:
            raise ValueError(f"Reviewed trade {reviewed_trade_id} was not found.")

        updated = self.workflow.transition_reviewed_trade(
            record,
            ReviewedTradeStatus.EXECUTED,
            notes=notes if notes is not None else record.notes,
            context=None,
        )
        self._enqueue_reviewed_status_updated_event(updated)
        if auto_commit:
            self.session.commit()
            self.session.refresh(updated)
        return updated

    def get_block_reasons(
        self,
        trade: dict[str, Any],
        *,
        context: dict[str, Any] | None = None,
    ) -> list[str]:
        if not trade:
            return ["missing_trade_payload"]

        reasons: list[str] = []
        ctx = context or {}

        reviewed_trade_status = str(trade.get("reviewed_trade_status") or "").upper().strip()
        if reviewed_trade_status and reviewed_trade_status != "APPROVED":
            reasons.append(f"reviewed_trade_status_{reviewed_trade_status.lower()}")

        validation_status = str(trade.get("validation_status") or "").upper().strip()
        if validation_status and validation_status != "PASS":
            reasons.append(f"validation_status_{validation_status.lower()}")

        execution_allowed = trade.get("execution_allowed")
        if execution_allowed is False:
            reasons.append("execution_gate_blocked")

        duplicate_reason = str(trade.get("duplicate_reason") or "").strip()
        if duplicate_reason:
            reasons.append(duplicate_reason.lower())

        if bool(trade.get("setup_already_used")):
            reasons.append("setup_already_used")

        max_trades_per_day = ctx.get("max_trades_per_day")
        trades_taken_today = ctx.get("trades_taken_today", ctx.get("executed_today"))
        try:
            if max_trades_per_day is not None and trades_taken_today is not None:
                if int(trades_taken_today) >= int(max_trades_per_day):
                    reasons.append("max_trades_per_day_reached")
        except (TypeError, ValueError):
            pass

        max_daily_loss = ctx.get("max_daily_loss")
        realized_pnl = ctx.get("realized_pnl_today", ctx.get("daily_pnl"))
        try:
            if max_daily_loss is not None and realized_pnl is not None:
                if float(realized_pnl) <= -abs(float(max_daily_loss)):
                    reasons.append("max_daily_loss_reached")
        except (TypeError, ValueError):
            pass

        if bool(ctx.get("kill_switch_enabled")):
            reasons.append("kill_switch_active")

        if bool(ctx.get("active_trade_exists")):
            reasons.append("active_trade_exists")

        if bool(ctx.get("cooldown_active")):
            reasons.append("cooldown_active")

        deduped_reasons: list[str] = []
        for reason in reasons:
            if reason and reason not in deduped_reasons:
                deduped_reasons.append(reason)
        return deduped_reasons

    def _resolve_create_payload(
        self,
        command: ReviewedTradeCreateCommand,
    ) -> dict[str, Any]:
        signal = None
        if command.signal_id is not None:
            signal = self.signal_repository.get_signal(command.signal_id)
            if signal is None:
                raise ValueError(f"Signal {command.signal_id} was not found.")

        strategy_name = command.strategy_name or (signal.strategy_name if signal is not None else None)
        symbol = command.symbol or (signal.symbol if signal is not None else None)
        side = command.side or (signal.side if signal is not None else None)
        entry_price = command.entry_price if command.entry_price is not None else (
            signal.entry_price if signal is not None else None
        )
        stop_loss = command.stop_loss if command.stop_loss is not None else (
            signal.stop_loss if signal is not None else None
        )
        target_price = command.target_price if command.target_price is not None else (
            signal.target_price if signal is not None else None
        )

        missing_fields = [
            name
            for name, value in (
                ("strategy_name", strategy_name),
                ("symbol", symbol),
                ("side", side),
                ("entry_price", entry_price),
                ("stop_loss", stop_loss),
                ("target_price", target_price),
            )
            if value is None or value == ""
        ]
        if missing_fields:
            raise ValueError(
                f"Missing reviewed trade fields: {', '.join(missing_fields)}"
            )

        status = str(command.status or "REVIEWED").upper().strip()
        try:
            ReviewedTradeStatus(status)
        except ValueError as exc:
            raise ValueError(f"Unsupported reviewed trade status: {command.status}") from exc

        return {
            "signal_id": command.signal_id,
            "strategy_name": str(strategy_name),
            "symbol": str(symbol),
            "side": str(side).upper(),
            "entry_price": float(entry_price),
            "stop_loss": float(stop_loss),
            "target_price": float(target_price),
            "quantity": int(command.quantity),
            "lots": int(command.lots),
            "status": status,
            "notes": command.notes,
        }

    def _enqueue_reviewed_created_event(self, record: ReviewedTradeRecord) -> None:
        self.outbox.enqueue(
            event_name=EVENT_REVIEWED_TRADE_CREATED,
            payload={
                "reviewed_trade_id": record.id,
                "signal_id": record.signal_id,
                "strategy_name": record.strategy_name,
                "symbol": record.symbol,
                "status": record.status,
            },
            source="reviewed_trade_service",
        )

    def _enqueue_reviewed_status_updated_event(self, record: ReviewedTradeRecord) -> None:
        self.outbox.enqueue(
            event_name=EVENT_REVIEWED_TRADE_STATUS_UPDATED,
            payload={
                "reviewed_trade_id": record.id,
                "signal_id": record.signal_id,
                "strategy_name": record.strategy_name,
                "symbol": record.symbol,
                "status": record.status,
            },
            source="reviewed_trade_service",
        )


__all__ = [
    "ReviewedTradeCreateCommand",
    "ReviewedTradeService",
    "ReviewedTradeStatusUpdateCommand",
]


