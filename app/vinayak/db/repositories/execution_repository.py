from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord


class ExecutionRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_execution(
        self,
        mode: str,
        broker: str,
        status: str,
        signal_id: int | None = None,
        reviewed_trade_id: int | None = None,
        executed_price: float | None = None,
        executed_at: datetime | None = None,
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

    def update_execution(
        self,
        execution: ExecutionRecord,
        *,
        broker: str | None = None,
        status: str | None = None,
        executed_price: float | None = None,
        executed_at: datetime | None = None,
        broker_reference: str | None = None,
        notes: str | None = None,
    ) -> ExecutionRecord:
        if broker is not None:
            execution.broker = broker
        if status is not None:
            execution.status = status
        execution.executed_price = executed_price
        execution.executed_at = executed_at
        execution.broker_reference = broker_reference
        execution.notes = notes
        self.session.add(execution)
        self.session.flush()
        return execution

    def get_by_reviewed_trade_mode(
        self,
        *,
        reviewed_trade_id: int,
        mode: str,
    ) -> ExecutionRecord | None:
        return (
            self.session.query(ExecutionRecord)
            .filter(
                ExecutionRecord.reviewed_trade_id == reviewed_trade_id,
                ExecutionRecord.mode == mode,
            )
            .order_by(ExecutionRecord.id.desc())
            .first()
        )

    def get_by_signal_mode(
        self,
        *,
        signal_id: int,
        mode: str,
    ) -> ExecutionRecord | None:
        return (
            self.session.query(ExecutionRecord)
            .filter(
                ExecutionRecord.signal_id == signal_id,
                ExecutionRecord.mode == mode,
            )
            .order_by(ExecutionRecord.id.desc())
            .first()
        )

    def get_by_broker_reference(
        self,
        *,
        broker: str,
        broker_reference: str,
    ) -> ExecutionRecord | None:
        normalized_broker = str(broker or '').strip().upper()
        normalized_reference = str(broker_reference or '').strip()
        if not normalized_broker or not normalized_reference:
            return None
        return (
            self.session.query(ExecutionRecord)
            .filter(
                ExecutionRecord.broker == normalized_broker,
                ExecutionRecord.broker_reference == normalized_reference,
            )
            .order_by(ExecutionRecord.id.desc())
            .first()
        )

    def list_executions(self) -> list[ExecutionRecord]:
        return list(self.session.query(ExecutionRecord).order_by(ExecutionRecord.id.desc()).all())

    def get_latest_execution(self) -> ExecutionRecord | None:
        return (
            self.session.query(ExecutionRecord)
            .order_by(ExecutionRecord.executed_at.desc().nullslast(), ExecutionRecord.id.desc())
            .first()
        )
