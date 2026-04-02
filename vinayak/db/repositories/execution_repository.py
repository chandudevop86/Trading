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

    def get_by_broker_reference(self, broker_reference: str) -> ExecutionRecord | None:
        normalized = str(broker_reference or '').strip()
        if not normalized:
            return None
        return (
            self.session.query(ExecutionRecord)
            .filter(ExecutionRecord.broker_reference == normalized)
            .order_by(ExecutionRecord.id.desc())
            .first()
        )

    def list_executions(self) -> list[ExecutionRecord]:
        return list(self.session.query(ExecutionRecord).order_by(ExecutionRecord.id.desc()).all())
