from __future__ import annotations

"""DB-backed execution state queries for workspace guard evaluation."""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord


_EXECUTED_STATUSES = {"FILLED", "EXECUTED", "SENT", "ACCEPTED"}
_CLOSED_REVIEWED_STATUSES = {"CLOSED", "EXITED", "CANCELLED", "REJECTED"}


class ExecutionStateRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def has_duplicate_signal(
        self,
        *,
        symbol: str,
        side: str,
        strategy_name: str,
        signal_time: datetime,
        bucket_minutes: int,
    ) -> bool:
        bucket_start = self._bucket_start(signal_time, bucket_minutes)
        bucket_end = bucket_start + timedelta(minutes=max(int(bucket_minutes), 1))
        return (
            self.session.query(SignalRecord.id)
            .filter(
                SignalRecord.symbol == str(symbol or "").upper(),
                SignalRecord.side == str(side or "").upper(),
                SignalRecord.strategy_name == str(strategy_name or "").strip(),
                SignalRecord.signal_time >= bucket_start,
                SignalRecord.signal_time < bucket_end,
            )
            .first()
            is not None
        )

    def executed_trade_times(self, *, signal_date: datetime, mode: str) -> list[datetime]:
        rows = (
            self.session.query(SignalRecord.signal_time, ExecutionRecord.executed_at)
            .join(ExecutionRecord, ExecutionRecord.signal_id == SignalRecord.id)
            .filter(
                ExecutionRecord.mode == str(mode or "").upper(),
                ExecutionRecord.status.in_(_EXECUTED_STATUSES),
                func.date(SignalRecord.signal_time) == signal_date.date().isoformat(),
            )
            .all()
        )
        times: list[datetime] = []
        for signal_time, executed_at in rows:
            if signal_time is not None:
                times.append(signal_time)
            elif executed_at is not None:
                times.append(executed_at)
        return times

    def executed_trade_count(self, *, signal_date: datetime, mode: str) -> int:
        return int(
            self.session.query(func.count(ExecutionRecord.id))
            .join(SignalRecord, ExecutionRecord.signal_id == SignalRecord.id)
            .filter(
                ExecutionRecord.mode == str(mode or "").upper(),
                ExecutionRecord.status.in_(_EXECUTED_STATUSES),
                func.date(SignalRecord.signal_time) == signal_date.date().isoformat(),
            )
            .scalar()
            or 0
        )

    def active_trade_exists(self, *, mode: str) -> bool:
        return (
            self.session.query(ExecutionRecord.id)
            .outerjoin(ReviewedTradeRecord, ExecutionRecord.reviewed_trade_id == ReviewedTradeRecord.id)
            .filter(
                ExecutionRecord.mode == str(mode or "").upper(),
                ExecutionRecord.status.in_(_EXECUTED_STATUSES),
                or_(
                    ReviewedTradeRecord.id.is_(None),
                    ~ReviewedTradeRecord.status.in_(_CLOSED_REVIEWED_STATUSES),
                ),
            )
            .first()
            is not None
        )

    def list_open_position_rows(self, *, mode: str) -> list[dict[str, Any]]:
        rows = (
            self.session.query(
                ExecutionRecord.status,
                ReviewedTradeRecord.status.label("reviewed_trade_status"),
                ReviewedTradeRecord.symbol,
                ReviewedTradeRecord.entry_price,
                ReviewedTradeRecord.stop_loss,
                ReviewedTradeRecord.quantity,
            )
            .join(ReviewedTradeRecord, ExecutionRecord.reviewed_trade_id == ReviewedTradeRecord.id)
            .filter(
                ExecutionRecord.mode == str(mode or "").upper(),
                ExecutionRecord.status.in_(_EXECUTED_STATUSES),
                ~ReviewedTradeRecord.status.in_(_CLOSED_REVIEWED_STATUSES),
            )
            .all()
        )
        return [
            {
                "execution_status": status,
                "trade_status": reviewed_trade_status or status,
                "symbol": symbol,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "quantity": quantity,
            }
            for status, reviewed_trade_status, symbol, entry_price, stop_loss, quantity in rows
        ]

    def realized_pnl_for_day(self, *, signal_date: datetime, mode: str) -> float:
        return 0.0

    @staticmethod
    def _bucket_start(signal_time: datetime, bucket_minutes: int) -> datetime:
        size = max(int(bucket_minutes), 1)
        minute_bucket = (signal_time.minute // size) * size
        return signal_time.replace(minute=minute_bucket, second=0, microsecond=0)

