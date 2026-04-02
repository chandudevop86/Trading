from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from vinayak.db.session import Base


class ExecutionRecord(Base):
    __tablename__ = 'executions'
    __table_args__ = (
        UniqueConstraint('reviewed_trade_id', 'mode', name='uq_reviewed_trade_execution'),
        Index('idx_signal_mode', 'signal_id', 'mode'),
        Index('idx_reviewed_trade_id', 'reviewed_trade_id'),
        Index('idx_reviewed_trade_mode', 'reviewed_trade_id', 'mode'),
        Index('idx_broker_ref', 'broker_reference'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[int | None] = mapped_column(Integer, ForeignKey('signals.id'), nullable=True)
    reviewed_trade_id: Mapped[int | None] = mapped_column(Integer, ForeignKey('reviewed_trades.id'), nullable=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False)
    broker: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    executed_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    broker_reference: Mapped[str | None] = mapped_column(String(80), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
