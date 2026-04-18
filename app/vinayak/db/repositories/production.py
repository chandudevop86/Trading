from __future__ import annotations

import json
from collections.abc import Sequence
from decimal import Decimal

from sqlalchemy.orm import Session

from vinayak.db.models.production.trading import (
    AuditLogRecord,
    BacktestReportRecord,
    ExecutionRecordV2,
    ExecutionRequestRecord,
    PositionRecord,
    SignalRecordV2,
    StrategyRunRecord,
    ValidationLogRecord,
)
from vinayak.domain.models import AuditEvent, ExecutionRequest, ExecutionResult
from vinayak.execution.repositories import AuditRepositoryPort, ExecutionRepositoryPort


class SqlAlchemyExecutionRepository(ExecutionRepositoryPort):
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_idempotency_key(self, idempotency_key: str) -> ExecutionResult | None:
        record = (
            self.session.query(ExecutionRequestRecord, ExecutionRecordV2)
            .join(ExecutionRecordV2, ExecutionRecordV2.request_id == ExecutionRequestRecord.id)
            .filter(ExecutionRequestRecord.idempotency_key == idempotency_key)
            .one_or_none()
        )
        if record is None:
            return None
        _, execution = record
        return ExecutionResult(
            execution_id=execution.id,
            request_id=execution.request_id,
            status=execution.status,
            failure_reason=execution.failure_reason,
            processed_at=execution.processed_at,
            order_reference=execution.order_reference,
            message=execution.message,
        )

    def save_request(self, request: ExecutionRequest) -> None:
        self.session.merge(
            SignalRecordV2(
                id=str(request.signal.signal_id),
                strategy_run_id=None,
                idempotency_key=request.signal.idempotency_key,
                strategy_name=request.signal.strategy_name,
                symbol=request.signal.symbol,
                timeframe=request.signal.timeframe.value,
                signal_type=request.signal.signal_type.value,
                status=request.signal.status.value,
                side=request.signal.side.value if request.signal.side is not None else None,
                entry_price=float(request.signal.entry_price) if request.signal.entry_price is not None else None,
                stop_loss=float(request.signal.stop_loss) if request.signal.stop_loss is not None else None,
                target_price=float(request.signal.target_price) if request.signal.target_price is not None else None,
                quantity=float(request.signal.quantity) if request.signal.quantity is not None else None,
                confidence=float(request.signal.confidence),
                rationale=request.signal.rationale,
                generated_at=request.signal.generated_at,
                candle_timestamp=request.signal.candle_timestamp,
            )
        )
        self.session.merge(
            ExecutionRequestRecord(
                id=str(request.request_id),
                signal_id=str(request.signal.signal_id),
                idempotency_key=request.idempotency_key,
                mode=request.mode.value,
                account_id=request.account_id,
                requested_at=request.requested_at,
            )
        )
        self.session.flush()

    def save_result(self, result: ExecutionResult) -> ExecutionResult:
        self.session.merge(
            ExecutionRecordV2(
                id=str(result.execution_id),
                request_id=str(result.request_id),
                status=result.status.value,
                failure_reason=result.failure_reason.value,
                order_reference=result.order_reference,
                message=result.message,
                processed_at=result.processed_at,
            )
        )
        self.session.flush()
        return result


class SqlAlchemyAuditRepository(AuditRepositoryPort):
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_event(self, event: AuditEvent) -> None:
        self.session.merge(
            AuditLogRecord(
                id=str(event.event_id),
                correlation_id=str(event.correlation_id),
                event_type=event.event_type.value,
                payload_json=json.dumps(event.payload, sort_keys=True, default=str),
                occurred_at=event.occurred_at,
            )
        )
        self.session.flush()

    def list_events(self, *, limit: int = 100) -> list[AuditEvent]:
        records = (
            self.session.query(AuditLogRecord)
            .order_by(AuditLogRecord.occurred_at.desc())
            .limit(limit)
            .all()
        )
        return [
            AuditEvent(
                event_id=record.id,
                correlation_id=record.correlation_id,
                event_type=record.event_type,
                occurred_at=record.occurred_at,
                payload=json.loads(record.payload_json),
            )
            for record in records
        ]


class ProductionReadRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def list_open_positions(self) -> Sequence[PositionRecord]:
        return (
            self.session.query(PositionRecord)
            .filter(PositionRecord.is_open.is_(True))
            .order_by(PositionRecord.snapshot_at.desc())
            .all()
        )

    def list_recent_strategy_runs(self, *, limit: int = 50) -> Sequence[StrategyRunRecord]:
        return (
            self.session.query(StrategyRunRecord)
            .order_by(StrategyRunRecord.created_at.desc())
            .limit(limit)
            .all()
        )

    def list_recent_validations(self, *, limit: int = 100) -> Sequence[ValidationLogRecord]:
        return (
            self.session.query(ValidationLogRecord)
            .order_by(ValidationLogRecord.validated_at.desc())
            .limit(limit)
            .all()
        )

    def list_recent_signals(self, *, limit: int = 100) -> Sequence[SignalRecordV2]:
        return (
            self.session.query(SignalRecordV2)
            .order_by(SignalRecordV2.created_at.desc())
            .limit(limit)
            .all()
        )

    def list_recent_backtest_reports(self, *, limit: int = 50) -> Sequence[BacktestReportRecord]:
        return (
            self.session.query(BacktestReportRecord)
            .order_by(BacktestReportRecord.generated_at.desc())
            .limit(limit)
            .all()
        )

    def total_realized_pnl(self) -> Decimal:
        positions = self.session.query(PositionRecord).filter(PositionRecord.is_open.is_(False)).all()
        total = Decimal('0')
        for position in positions:
            total += Decimal(str(position.mark_price)) - Decimal(str(position.average_price))
        return total


__all__ = [
    'ProductionReadRepository',
    'SqlAlchemyAuditRepository',
    'SqlAlchemyExecutionRepository',
]
