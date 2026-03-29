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

    def list_executions(self) -> list[ExecutionRecord]:
        return list(self.session.query(ExecutionRecord).order_by(ExecutionRecord.id.desc()).all())
