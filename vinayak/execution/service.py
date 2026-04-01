from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Iterable, Protocol

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.execution_audit_log_repository import (
    ExecutionAuditLogRepository,
)
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.events import (
    EVENT_TRADE_EXECUTED,
    EVENT_TRADE_EXECUTE_REQUESTED,
)
from vinayak.execution.outbox_service import OutboxService
from vinayak.execution.paper_execution_adapter import PaperExecutionAdapter
from vinayak.execution.reviewed_trade_service import ReviewedTradeService
from vinayak.execution.live_execution_adapter import LiveExecutionAdapter


# ============================================================
# STATUS
# ============================================================

class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    SKIPPED = "SKIPPED"
    EXECUTED = "EXECUTED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


# ============================================================
# BATCH DTOs
# ============================================================

@dataclass(slots=True)
class ExecutionBatchItem:
    trade_id: str
    strategy_name: str
    symbol: str
    broker: str
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    status: str = ExecutionStatus.PENDING
    executed_price: float | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, trade: dict[str, Any]) -> "ExecutionBatchItem":
        return cls(
            trade_id=str(trade.get("trade_id", "")),
            strategy_name=str(trade.get("strategy_name", "")),
            symbol=str(trade.get("symbol", "")),
            broker=str(trade.get("broker", "")),
            signal_id=trade.get("signal_id"),
            reviewed_trade_id=trade.get("reviewed_trade_id"),
            status=str(trade.get("status", ExecutionStatus.PENDING)),
            executed_price=trade.get("executed_price"),
            payload=trade,
        )


@dataclass(slots=True)
class ExecutionDecision:
    trade_id: str
    strategy_name: str
    symbol: str
    status: str
    mode: str
    block_reasons: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    execution_id: int | None = None
    broker_reference: str | None = None


@dataclass(slots=True)
class ExecutionBatchResult:
    decisions: list[ExecutionDecision] = field(default_factory=list)

    def add(self, decision: ExecutionDecision) -> None:
        self.decisions.append(decision)

    @property
    def total(self) -> int:
        return len(self.decisions)

    @property
    def executed_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.EXECUTED)

    @property
    def blocked_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.BLOCKED)

    @property
    def skipped_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.SKIPPED)

    @property
    def rejected_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.REJECTED)

    @property
    def failed_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.FAILED)


# ============================================================
# ADAPTER CONTRACT
# ============================================================

class AdapterResultLike(Protocol):
    broker: str
    status: str
    executed_price: float | None
    executed_at: Any
    broker_reference: str | None
    notes: str | None
    audit_request_payload: dict[str, Any] | None
    audit_response_payload: dict[str, Any] | None


# ============================================================
# SERVICE
# ============================================================

class ExecutionService:
    def __init__(
        self,
        session: Session,
        *,
        execution_repository: ExecutionRepository | None = None,
        execution_audit_log_repository: ExecutionAuditLogRepository | None = None,
        reviewed_trade_repository: ReviewedTradeRepository | None = None,
        signal_repository: SignalRepository | None = None,
        reviewed_trade_service: ReviewedTradeService | None = None,
        paper_adapter: PaperExecutionAdapter | None = None,
        live_adapter: LiveExecutionAdapter | None = None,
        outbox: OutboxService | None = None,
    ) -> None:
        self.session = session
        self.execution_repository = execution_repository or ExecutionRepository(session)
        self.execution_audit_log_repository = (
            execution_audit_log_repository or ExecutionAuditLogRepository(session)
        )
        self.reviewed_trade_repository = (
            reviewed_trade_repository or ReviewedTradeRepository(session)
        )
        self.signal_repository = signal_repository or SignalRepository(session)
        self.reviewed_trade_service = reviewed_trade_service or ReviewedTradeService(session)
        self.paper_adapter = paper_adapter or PaperExecutionAdapter()
        self.live_adapter = live_adapter or LiveExecutionAdapter()
        self.outbox = outbox or OutboxService(session)

    def list_executions(self) -> list[ExecutionRecord]:
        return self.execution_repository.list_executions()

    def create_execution(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        mode = self._normalize_mode(command.mode)
        reviewed_trade, signal_id, signal = self._resolve_execution_dependencies(command)

        self.outbox.enqueue(
            event_name=EVENT_TRADE_EXECUTE_REQUESTED,
            payload={
                "mode": mode,
                "broker": command.broker,
                "signal_id": signal_id,
                "reviewed_trade_id": command.reviewed_trade_id,
            },
            source="execution_service",
        )

        adapter = self._get_adapter(mode)
        adapter_result = adapter.execute(
            command=ExecutionCreateCommand(
                mode=mode,
                broker=command.broker,
                signal_id=signal_id,
                reviewed_trade_id=command.reviewed_trade_id,
                status=command.status,
                executed_price=command.executed_price,
            ),
            reviewed_trade=reviewed_trade,
            signal=signal,
        )

        self._validate_adapter_result(adapter_result)

        try:
            record = self.execution_repository.create_execution(
                signal_id=signal_id,
                reviewed_trade_id=command.reviewed_trade_id,
                mode=mode,
                broker=adapter_result.broker,
                status=adapter_result.status,
                executed_price=adapter_result.executed_price,
                executed_at=adapter_result.executed_at,
                broker_reference=adapter_result.broker_reference,
                notes=adapter_result.notes,
            )

            if mode == "LIVE" and adapter_result.audit_request_payload is not None:
                self.execution_audit_log_repository.create_audit_log(
                    execution_id=record.id,
                    broker=adapter_result.broker,
                    request_payload=adapter_result.audit_request_payload,
                    response_payload=adapter_result.audit_response_payload,
                    status=adapter_result.status,
                )

            if reviewed_trade is not None and str(adapter_result.status).upper() not in {
                ExecutionStatus.BLOCKED,
                ExecutionStatus.REJECTED,
                ExecutionStatus.FAILED,
            }:
                self.reviewed_trade_service.mark_executed(
                    reviewed_trade.id,
                    notes=f"Execution recorded via {mode} mode.",
                    auto_commit=False,
                )

            self.outbox.enqueue(
                event_name=EVENT_TRADE_EXECUTED,
                payload={
                    "execution_id": record.id,
                    "signal_id": record.signal_id,
                    "reviewed_trade_id": record.reviewed_trade_id,
                    "mode": record.mode,
                    "broker": record.broker,
                    "status": record.status,
                    "broker_reference": record.broker_reference,
                },
                source="execution_service",
            )

            self.session.commit()
            self.session.refresh(record)
            return record

        except Exception:
            self.session.rollback()
            raise

    def execute_batch(
        self,
        trades: Iterable[ExecutionBatchItem | dict[str, Any]],
        *,
        mode: str = "PAPER",
        context: dict[str, Any] | None = None,
        continue_on_error: bool = True,
    ) -> ExecutionBatchResult:
        result = ExecutionBatchResult()
        normalized_mode = self._normalize_mode(mode)
        ctx = context or {}

        for raw_item in trades:
            item = raw_item if isinstance(raw_item, ExecutionBatchItem) else ExecutionBatchItem.from_dict(raw_item)

            try:
                reasons = self.reviewed_trade_service.get_block_reasons(item.payload, context=ctx)
                if reasons:
                    result.add(
                        ExecutionDecision(
                            trade_id=item.trade_id,
                            strategy_name=item.strategy_name,
                            symbol=item.symbol,
                            status=ExecutionStatus.BLOCKED,
                            mode=normalized_mode,
                            block_reasons=[str(r) for r in reasons],
                            payload=item.payload,
                        )
                    )
                    continue

                command = ExecutionCreateCommand(
                    mode=normalized_mode,
                    broker=item.broker,
                    signal_id=item.signal_id,
                    reviewed_trade_id=item.reviewed_trade_id,
                    status=item.status,
                    executed_price=item.executed_price,
                )

                record = self.create_execution(command)
                record_status = str(record.status).upper()

                mapped_status = (
                    record_status
                    if record_status in {
                        ExecutionStatus.EXECUTED,
                        ExecutionStatus.BLOCKED,
                        ExecutionStatus.SKIPPED,
                        ExecutionStatus.REJECTED,
                        ExecutionStatus.FAILED,
                    }
                    else ExecutionStatus.EXECUTED
                )

                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=mapped_status,
                        mode=normalized_mode,
                        block_reasons=[],
                        payload={**item.payload, "execution_record_id": record.id},
                        execution_id=record.id,
                        broker_reference=record.broker_reference,
                    )
                )

            except ValueError as exc:
                message = str(exc)
                lowered = message.lower()

                if "empty result" in lowered or "missing adapter result" in lowered:
                    status = ExecutionStatus.SKIPPED
                    reasons = ["gateway_returned_empty_result"]
                else:
                    status = ExecutionStatus.BLOCKED
                    reasons = [message]

                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=status,
                        mode=normalized_mode,
                        block_reasons=reasons,
                        payload=item.payload,
                    )
                )

                if not continue_on_error:
                    break

            except Exception as exc:
                self.session.rollback()
                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=ExecutionStatus.FAILED,
                        mode=normalized_mode,
                        block_reasons=[f"unexpected_error: {exc}"],
                        payload=item.payload,
                    )
                )

                if not continue_on_error:
                    break

        return result

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or "").upper().strip()
        if normalized not in {"PAPER", "LIVE"}:
            raise ValueError(f"Unsupported execution mode: {mode}")
        return normalized

    def _get_adapter(self, mode: str) -> PaperExecutionAdapter | LiveExecutionAdapter:
        return self.live_adapter if mode == "LIVE" else self.paper_adapter

    def _resolve_execution_dependencies(
        self,
        command: ExecutionCreateCommand,
    ) -> tuple[ReviewedTradeRecord | None, int | None, Any | None]:
        reviewed_trade: ReviewedTradeRecord | None = None
        if command.reviewed_trade_id is not None:
            reviewed_trade = self.reviewed_trade_repository.get_reviewed_trade(
                command.reviewed_trade_id
            )
            if reviewed_trade is None:
                raise ValueError(
                    f"Reviewed trade {command.reviewed_trade_id} was not found."
                )
            if str(reviewed_trade.status).upper() != "APPROVED":
                raise ValueError(
                    f"Reviewed trade {command.reviewed_trade_id} must be APPROVED before execution. "
                    f"Current status: {reviewed_trade.status}."
                )

        signal_id = command.signal_id
        if signal_id is None and reviewed_trade is not None:
            signal_id = reviewed_trade.signal_id

        signal = None
        if signal_id is not None:
            signal = self.signal_repository.get_signal(signal_id)
            if signal is None:
                raise ValueError(f"Signal {signal_id} was not found.")

        return reviewed_trade, signal_id, signal

    def _validate_adapter_result(self, result: AdapterResultLike | None) -> None:
        if result is None:
            raise ValueError("Execution adapter returned empty result.")

        missing_fields: list[str] = []

        if not getattr(result, "broker", None):
            missing_fields.append("broker")

        if not getattr(result, "status", None):
            missing_fields.append("status")

        if getattr(result, "executed_at", None) is None:
            missing_fields.append("executed_at")

        if missing_fields:
            raise ValueError(
                f"Execution adapter returned invalid result. Missing fields: {', '.join(missing_fields)}"
            )