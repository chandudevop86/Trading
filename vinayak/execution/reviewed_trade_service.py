from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Iterable, Protocol

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.execution_audit_log_repository import (
    ExecutionAuditLogRepository,
)
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.execution.events import (
    EVENT_TRADE_EXECUTED,
    EVENT_TRADE_EXECUTE_REQUESTED,
)
from vinayak.execution.live_execution_adapter import LiveExecutionAdapter
from vinayak.execution.outbox_service import OutboxService
from vinayak.execution.paper_execution_adapter import PaperExecutionAdapter
from vinayak.execution.reviewed_trade_service import ReviewedTradeService


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
    SUBMITTED = "SUBMITTED"
    PLACED = "PLACED"


ACTIVE_EXECUTION_STATUSES: tuple[str, ...] = (
    ExecutionStatus.PENDING,
    ExecutionStatus.SUBMITTED,
    ExecutionStatus.PLACED,
    ExecutionStatus.EXECUTED,
)


# ============================================================
# COMMANDS / DTOs
# ============================================================


@dataclass(slots=True)
class ExecutionCreateCommand:
    mode: str
    broker: str
    side: str
    entry_price: float
    stop_loss: float
    target_price: float
    quantity: int
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    reviewed_trade_status: str | None = None
    validation_status: str | None = None
    strategy_name: str | None = None
    symbol: str | None = None
    trade_id: str | None = None
    status: str = ExecutionStatus.PENDING
    executed_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionBatchItem:
    trade_id: str
    strategy_name: str
    symbol: str
    broker: str
    side: str
    entry_price: float
    stop_loss: float
    target_price: float
    quantity: int
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    reviewed_trade_status: str | None = None
    validation_status: str | None = None
    status: str = ExecutionStatus.PENDING
    executed_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, trade: dict[str, Any]) -> "ExecutionBatchItem":
        return cls(
            trade_id=str(trade.get("trade_id", "")),
            strategy_name=str(trade.get("strategy_name", "")),
            symbol=str(trade.get("symbol", "")),
            broker=str(trade.get("broker", "")),
            side=str(trade.get("side", "")),
            entry_price=_require_float(trade.get("entry_price"), "entry_price"),
            stop_loss=_require_float(trade.get("stop_loss"), "stop_loss"),
            target_price=_require_float(trade.get("target_price"), "target_price"),
            quantity=_require_int(trade.get("quantity"), "quantity"),
            signal_id=trade.get("signal_id"),
            reviewed_trade_id=trade.get("reviewed_trade_id"),
            reviewed_trade_status=_optional_str(trade.get("reviewed_trade_status")),
            validation_status=_optional_str(trade.get("validation_status")),
            status=str(trade.get("status", ExecutionStatus.PENDING)),
            executed_price=_to_float(trade.get("executed_price")),
            metadata=dict(trade),
        )

    def to_command(self, *, mode: str) -> ExecutionCreateCommand:
        return ExecutionCreateCommand(
            mode=mode,
            broker=self.broker,
            side=self.side,
            entry_price=self.entry_price,
            stop_loss=self.stop_loss,
            target_price=self.target_price,
            quantity=self.quantity,
            signal_id=self.signal_id,
            reviewed_trade_id=self.reviewed_trade_id,
            reviewed_trade_status=self.reviewed_trade_status,
            validation_status=self.validation_status,
            strategy_name=self.strategy_name,
            symbol=self.symbol,
            trade_id=self.trade_id,
            status=self.status,
            executed_price=self.executed_price,
            metadata=self.metadata,
        )

    def to_trade_payload(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "strategy_name": self.strategy_name,
            "symbol": self.symbol,
            "broker": self.broker,
            "side": self.side,
            "entry_price": self.entry_price,
            "stop_loss": self.stop_loss,
            "target_price": self.target_price,
            "quantity": self.quantity,
            "signal_id": self.signal_id,
            "reviewed_trade_id": self.reviewed_trade_id,
            "reviewed_trade_status": self.reviewed_trade_status,
            "validation_status": self.validation_status,
            "status": self.status,
            "executed_price": self.executed_price,
            **self.metadata,
        }


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
    reason_code: str | None = None


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
# REPOSITORY
# ============================================================


class ExecutionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_executions(self) -> list[ExecutionRecord]:
        stmt = select(ExecutionRecord).order_by(
            ExecutionRecord.executed_at.desc(),
            ExecutionRecord.id.desc(),
        )
        return list(self.session.scalars(stmt).all())

    def get_execution(self, execution_id: int) -> ExecutionRecord | None:
        stmt = select(ExecutionRecord).where(ExecutionRecord.id == execution_id)
        return self.session.scalar(stmt)

    def create_execution(
        self,
        *,
        signal_id: int | None,
        reviewed_trade_id: int | None,
        mode: str,
        broker: str,
        status: str,
        executed_price: float | None,
        executed_at: Any,
        broker_reference: str | None = None,
        notes: str | None = None,
    ) -> ExecutionRecord:
        record = ExecutionRecord(
            signal_id=signal_id,
            reviewed_trade_id=reviewed_trade_id,
            mode=mode,
            broker=broker,
            status=status,
            executed_price=executed_price,
            executed_at=executed_at,
            broker_reference=broker_reference,
            notes=notes,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def exists_for_reviewed_trade(
        self,
        *,
        reviewed_trade_id: int,
        mode: str,
        statuses: tuple[str, ...] = ACTIVE_EXECUTION_STATUSES,
    ) -> bool:
        stmt = (
            select(func.count())
            .select_from(ExecutionRecord)
            .where(ExecutionRecord.reviewed_trade_id == reviewed_trade_id)
            .where(ExecutionRecord.mode == mode)
            .where(ExecutionRecord.status.in_(statuses))
        )
        count = self.session.scalar(stmt) or 0
        return count > 0

    def exists_for_signal(
        self,
        *,
        signal_id: int,
        mode: str,
        statuses: tuple[str, ...] = ACTIVE_EXECUTION_STATUSES,
    ) -> bool:
        stmt = (
            select(func.count())
            .select_from(ExecutionRecord)
            .where(ExecutionRecord.signal_id == signal_id)
            .where(ExecutionRecord.mode == mode)
            .where(ExecutionRecord.status.in_(statuses))
        )
        count = self.session.scalar(stmt) or 0
        return count > 0

    def exists_for_broker_reference(self, *, broker_reference: str) -> bool:
        stmt = (
            select(func.count())
            .select_from(ExecutionRecord)
            .where(ExecutionRecord.broker_reference == broker_reference)
        )
        count = self.session.scalar(stmt) or 0
        return count > 0

    def find_latest_for_reviewed_trade(
        self,
        *,
        reviewed_trade_id: int,
        mode: str | None = None,
    ) -> ExecutionRecord | None:
        stmt = select(ExecutionRecord).where(
            ExecutionRecord.reviewed_trade_id == reviewed_trade_id
        )
        if mode is not None:
            stmt = stmt.where(ExecutionRecord.mode == mode)
        stmt = stmt.order_by(
            ExecutionRecord.executed_at.desc(),
            ExecutionRecord.id.desc(),
        )
        return self.session.scalar(stmt)

    def find_latest_for_signal(
        self,
        *,
        signal_id: int,
        mode: str | None = None,
    ) -> ExecutionRecord | None:
        stmt = select(ExecutionRecord).where(ExecutionRecord.signal_id == signal_id)
        if mode is not None:
            stmt = stmt.where(ExecutionRecord.mode == mode)
        stmt = stmt.order_by(
            ExecutionRecord.executed_at.desc(),
            ExecutionRecord.id.desc(),
        )
        return self.session.scalar(stmt)


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

    def create_execution(
        self,
        command: ExecutionCreateCommand,
        *,
        context: dict[str, Any] | None = None,
    ) -> ExecutionRecord:
        mode = self._normalize_mode(command.mode)
        self._validate_command(command)

        reviewed_trade, signal_id, signal = self._resolve_dependencies(command)
        trade_payload = self._build_trade_payload_for_block_check(
            command=command,
            reviewed_trade=reviewed_trade,
            signal_id=signal_id,
        )

        reasons = self.reviewed_trade_service.get_block_reasons(
            trade_payload,
            context=context,
        )
        if reasons:
            raise ValueError(f"Execution blocked: {', '.join(reasons)}")

        self._ensure_not_already_executed(
            reviewed_trade_id=command.reviewed_trade_id,
            signal_id=signal_id,
            mode=mode,
        )

        self.outbox.enqueue(
            event_name=EVENT_TRADE_EXECUTE_REQUESTED,
            payload={
                "mode": mode,
                "broker": command.broker,
                "signal_id": signal_id,
                "reviewed_trade_id": command.reviewed_trade_id,
                "trade_id": command.trade_id,
                "symbol": command.symbol,
                "strategy_name": command.strategy_name,
            },
            source="execution_service",
        )

        adapter = self._get_adapter(mode)
        adapter_result = adapter.execute(
            command=command,
            reviewed_trade=reviewed_trade,
            signal=signal,
        )

        self._validate_adapter_result(adapter_result)

        if mode == "LIVE" and adapter_result.broker_reference:
            if self.execution_repository.exists_for_broker_reference(
                broker_reference=adapter_result.broker_reference
            ):
                raise ValueError(
                    f"Duplicate broker execution detected for reference {adapter_result.broker_reference}."
                )

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
                    reviewed_trade_id=reviewed_trade.id,
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
                    "trade_id": command.trade_id,
                    "symbol": command.symbol,
                    "strategy_name": command.strategy_name,
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
            try:
                item = (
                    raw_item
                    if isinstance(raw_item, ExecutionBatchItem)
                    else ExecutionBatchItem.from_dict(raw_item)
                )
                trade_payload = item.to_trade_payload()

                reasons = self.reviewed_trade_service.get_block_reasons(
                    trade_payload,
                    context=ctx,
                )
                if reasons:
                    result.add(
                        ExecutionDecision(
                            trade_id=item.trade_id,
                            strategy_name=item.strategy_name,
                            symbol=item.symbol,
                            status=ExecutionStatus.BLOCKED,
                            mode=normalized_mode,
                            block_reasons=reasons,
                            payload=trade_payload,
                            reason_code="blocked_by_trade_rules",
                        )
                    )
                    continue

                record = self.create_execution(
                    item.to_command(mode=normalized_mode),
                    context=ctx,
                )
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
                        payload={**trade_payload, "execution_record_id": record.id},
                        execution_id=record.id,
                        broker_reference=record.broker_reference,
                        reason_code="execution_record_created",
                    )
                )

            except ValueError as exc:
                message = str(exc)
                lowered = message.lower()

                status = ExecutionStatus.REJECTED
                reasons = [message]
                reason_code = "business_validation_error"

                if lowered.startswith("execution blocked:"):
                    status = ExecutionStatus.BLOCKED
                    reasons = [part.strip() for part in message.split(":", 1)[1].split(",")]
                    reason_code = "blocked_by_execution_gate"
                elif "already executed" in lowered or "duplicate execution" in lowered:
                    status = ExecutionStatus.REJECTED
                    reason_code = "duplicate_execution"
                elif "empty result" in lowered or "invalid result" in lowered:
                    status = ExecutionStatus.SKIPPED
                    reason_code = "adapter_invalid_result"

                safe_trade_id, safe_strategy_name, safe_symbol, safe_payload = self._safe_item_fields(raw_item)
                result.add(
                    ExecutionDecision(
                        trade_id=safe_trade_id,
                        strategy_name=safe_strategy_name,
                        symbol=safe_symbol,
                        status=status,
                        mode=normalized_mode,
                        block_reasons=reasons,
                        payload=safe_payload,
                        reason_code=reason_code,
                    )
                )

                if not continue_on_error:
                    break

            except Exception as exc:
                self.session.rollback()
                safe_trade_id, safe_strategy_name, safe_symbol, safe_payload = self._safe_item_fields(raw_item)
                result.add(
                    ExecutionDecision(
                        trade_id=safe_trade_id,
                        strategy_name=safe_strategy_name,
                        symbol=safe_symbol,
                        status=ExecutionStatus.FAILED,
                        mode=normalized_mode,
                        block_reasons=[f"unexpected_error: {exc}"],
                        payload=safe_payload,
                        reason_code="unexpected_system_error",
                    )
                )

                if not continue_on_error:
                    break

        return result

    def _validate_command(self, command: ExecutionCreateCommand) -> None:
        if not str(command.broker or "").strip():
            raise ValueError("broker is required")
        if not str(command.side or "").strip():
            raise ValueError("side is required")
        if command.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if command.entry_price <= 0:
            raise ValueError("entry_price must be greater than zero")
        if command.stop_loss <= 0:
            raise ValueError("stop_loss must be greater than zero")
        if command.target_price <= 0:
            raise ValueError("target_price must be greater than zero")

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or "").upper().strip()
        if normalized not in {"PAPER", "LIVE"}:
            raise ValueError(f"Unsupported execution mode: {mode}")
        return normalized

    def _get_adapter(self, mode: str) -> PaperExecutionAdapter | LiveExecutionAdapter:
        return self.live_adapter if mode == "LIVE" else self.paper_adapter

    def _resolve_dependencies(
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

    def _build_trade_payload_for_block_check(
        self,
        *,
        command: ExecutionCreateCommand,
        reviewed_trade: ReviewedTradeRecord | None,
        signal_id: int | None,
    ) -> dict[str, Any]:
        reviewed_trade_status = command.reviewed_trade_status
        if reviewed_trade_status is None and reviewed_trade is not None:
            reviewed_trade_status = str(reviewed_trade.status).upper()

        return {
            "trade_id": command.trade_id,
            "strategy_name": command.strategy_name,
            "symbol": command.symbol,
            "broker": command.broker,
            "side": command.side,
            "entry_price": command.entry_price,
            "stop_loss": command.stop_loss,
            "target_price": command.target_price,
            "quantity": command.quantity,
            "signal_id": signal_id,
            "reviewed_trade_id": command.reviewed_trade_id,
            "reviewed_trade_status": reviewed_trade_status,
            "validation_status": command.validation_status,
            "status": command.status,
            "executed_price": command.executed_price,
            **command.metadata,
        }

    def _ensure_not_already_executed(
        self,
        *,
        reviewed_trade_id: int | None,
        signal_id: int | None,
        mode: str,
    ) -> None:
        if reviewed_trade_id is not None:
            if self.execution_repository.exists_for_reviewed_trade(
                reviewed_trade_id=reviewed_trade_id,
                mode=mode,
            ):
                raise ValueError(
                    f"Reviewed trade {reviewed_trade_id} already executed in {mode} mode."
                )

        if signal_id is not None:
            if self.execution_repository.exists_for_signal(
                signal_id=signal_id,
                mode=mode,
            ):
                raise ValueError(
                    f"Duplicate execution detected for signal {signal_id} in {mode} mode."
                )

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

    def _safe_item_fields(
        self,
        raw_item: ExecutionBatchItem | dict[str, Any],
    ) -> tuple[str, str, str, dict[str, Any]]:
        if isinstance(raw_item, ExecutionBatchItem):
            return (
                raw_item.trade_id,
                raw_item.strategy_name,
                raw_item.symbol,
                raw_item.to_trade_payload(),
            )
        return (
            str(raw_item.get("trade_id", "")),
            str(raw_item.get("strategy_name", "")),
            str(raw_item.get("symbol", "")),
            dict(raw_item),
        )


# ============================================================
# HELPERS
# ============================================================


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _require_float(value: Any, field_name: str) -> float:
    parsed = _to_float(value)
    if parsed is None:
        raise ValueError(f"{field_name} is required and must be numeric")
    return parsed


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _require_int(value: Any, field_name: str) -> int:
    parsed = _to_int(value)
    if parsed is None:
        raise ValueError(f"{field_name} is required and must be an integer")
    return parsed


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
