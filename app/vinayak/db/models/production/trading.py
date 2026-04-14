from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from vinayak.db.session import Base


def _utcnow() -> datetime:
    return datetime.now(UTC)


class StrategyRunRecord(Base):
    __tablename__ = 'strategy_runs_v2'
    __table_args__ = (
        Index('idx_strategy_runs_v2_symbol_timeframe', 'symbol', 'timeframe'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class SignalRecordV2(Base):
    __tablename__ = 'signals_v2'
    __table_args__ = (
        UniqueConstraint('idempotency_key', name='uq_signals_v2_idempotency_key'),
        Index('idx_signals_v2_symbol_timeframe', 'symbol', 'timeframe'),
        Index('idx_signals_v2_strategy_status', 'strategy_name', 'status'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_run_id: Mapped[str | None] = mapped_column(ForeignKey('strategy_runs_v2.id'), nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    signal_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8), nullable=True)
    entry_price: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    stop_loss: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    target_price: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    quantity: Mapped[float | None] = mapped_column(Numeric(18, 6), nullable=True)
    confidence: Mapped[float] = mapped_column(Numeric(10, 6), nullable=False)
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    candle_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class ExecutionRequestRecord(Base):
    __tablename__ = 'execution_requests_v2'
    __table_args__ = (
        UniqueConstraint('idempotency_key', name='uq_execution_requests_v2_idempotency_key'),
        Index('idx_execution_requests_v2_mode_created', 'mode', 'created_at'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    signal_id: Mapped[str] = mapped_column(ForeignKey('signals_v2.id'), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    mode: Mapped[str] = mapped_column(String(16), nullable=False)
    account_id: Mapped[str] = mapped_column(String(64), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class ExecutionRecordV2(Base):
    __tablename__ = 'executions_v2'
    __table_args__ = (
        UniqueConstraint('request_id', name='uq_executions_v2_request_id'),
        Index('idx_executions_v2_status_mode', 'status', 'mode'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    request_id: Mapped[str] = mapped_column(ForeignKey('execution_requests_v2.id'), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    failure_reason: Mapped[str] = mapped_column(String(64), nullable=False)
    order_reference: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False, default='')
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class PositionRecord(Base):
    __tablename__ = 'positions_v2'
    __table_args__ = (
        Index('idx_positions_v2_symbol_open', 'symbol', 'is_open'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    average_price: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    mark_price: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ValidationLogRecord(Base):
    __tablename__ = 'validation_logs_v2'
    __table_args__ = (
        Index('idx_validation_logs_v2_signal', 'signal_id'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    signal_id: Mapped[str] = mapped_column(ForeignKey('signals_v2.id'), nullable=False)
    is_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail: Mapped[str] = mapped_column(Text, nullable=False, default='')
    validated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AuditLogRecord(Base):
    __tablename__ = 'audit_logs_v2'
    __table_args__ = (
        Index('idx_audit_logs_v2_event_type_created', 'event_type', 'occurred_at'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    correlation_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class BacktestReportRecord(Base):
    __tablename__ = 'backtest_reports_v2'
    __table_args__ = (
        Index('idx_backtest_reports_v2_strategy_symbol', 'strategy_name', 'symbol'),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(16), nullable=False)
    trade_count: Mapped[int] = mapped_column(nullable=False)
    hit_ratio: Mapped[float] = mapped_column(Numeric(10, 6), nullable=False)
    profit_factor: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    max_drawdown: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    average_r_multiple: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
