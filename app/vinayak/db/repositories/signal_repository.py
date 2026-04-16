from __future__ import annotations

from sqlalchemy.orm import Session

from vinayak.db.models.signal import SignalRecord
from vinayak.strategies.common.base import StrategySignal


class SignalRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_signal(self, signal: StrategySignal, status: str = 'NEW') -> SignalRecord:
        record = SignalRecord(
            strategy_name=signal.strategy_name,
            symbol=signal.symbol,
            side=signal.side,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            target_price=signal.target_price,
            signal_time=signal.signal_time,
            status=status,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_signal(self, signal_id: int) -> SignalRecord | None:
        return self.session.get(SignalRecord, signal_id)

    def list_signals(self) -> list[SignalRecord]:
        return list(self.session.query(SignalRecord).order_by(SignalRecord.id.desc()).all())

    def get_latest_signal(self) -> SignalRecord | None:
        return self.session.query(SignalRecord).order_by(SignalRecord.signal_time.desc(), SignalRecord.id.desc()).first()
