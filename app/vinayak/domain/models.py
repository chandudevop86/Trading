from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Timestamp must be timezone-aware.')
    return value.astimezone(UTC)


class Timeframe(str, Enum):
    M1 = '1m'
    M5 = '5m'
    M15 = '15m'
    M30 = '30m'
    H1 = '1h'
    D1 = '1d'


class ExecutionSide(str, Enum):
    BUY = 'BUY'
    SELL = 'SELL'


class TradeSignalType(str, Enum):
    ENTRY = 'ENTRY'
    EXIT = 'EXIT'
    NO_TRADE = 'NO_TRADE'


class TradeSignalStatus(str, Enum):
    CREATED = 'CREATED'
    VALIDATED = 'VALIDATED'
    REJECTED = 'REJECTED'
    EXECUTION_REQUESTED = 'EXECUTION_REQUESTED'
    EXECUTED = 'EXECUTED'


class ExecutionMode(str, Enum):
    PAPER = 'PAPER'
    LIVE = 'LIVE'


class ExecutionStatus(str, Enum):
    ACCEPTED = 'ACCEPTED'
    REJECTED = 'REJECTED'
    EXECUTED = 'EXECUTED'
    FAILED = 'FAILED'


class ExecutionFailureReason(str, Enum):
    NONE = 'NONE'
    DUPLICATE_REQUEST = 'DUPLICATE_REQUEST'
    COOLDOWN_ACTIVE = 'COOLDOWN_ACTIVE'
    MAX_TRADES_REACHED = 'MAX_TRADES_REACHED'
    DAILY_LOSS_LIMIT = 'DAILY_LOSS_LIMIT'
    SESSION_CLOSED = 'SESSION_CLOSED'
    LIVE_MODE_LOCKED = 'LIVE_MODE_LOCKED'
    INVALID_SIGNAL = 'INVALID_SIGNAL'
    INVALID_RISK = 'INVALID_RISK'


class AuditEventType(str, Enum):
    SIGNAL_GENERATED = 'SIGNAL_GENERATED'
    SIGNAL_VALIDATION_FAILED = 'SIGNAL_VALIDATION_FAILED'
    EXECUTION_REQUESTED = 'EXECUTION_REQUESTED'
    EXECUTION_REJECTED = 'EXECUTION_REJECTED'
    EXECUTION_COMPLETED = 'EXECUTION_COMPLETED'


class VinayakModel(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)


class Candle(VinayakModel):
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: Timeframe
    timestamp: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    vwap: Decimal | None = Field(default=None, gt=0)

    @field_validator('symbol')
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return str(value or '').strip().upper()

    @field_validator('timestamp')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode='after')
    def _validate_ohlc(self) -> 'Candle':
        if self.high < self.low:
            raise ValueError('high must be greater than or equal to low')
        if self.high < max(self.open, self.close):
            raise ValueError('high must be greater than or equal to open/close')
        if self.low > min(self.open, self.close):
            raise ValueError('low must be less than or equal to open/close')
        return self


class CandleBatch(VinayakModel):
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: Timeframe
    candles: tuple[Candle, ...] = Field(min_length=1)

    @field_validator('symbol')
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return str(value or '').strip().upper()

    @model_validator(mode='after')
    def _validate_batch(self) -> 'CandleBatch':
        timestamps = [item.timestamp for item in self.candles]
        if timestamps != sorted(timestamps):
            raise ValueError('candles must be sorted by timestamp ascending')
        if {item.symbol for item in self.candles} != {self.symbol}:
            raise ValueError('candle batch symbol mismatch')
        if {item.timeframe for item in self.candles} != {self.timeframe}:
            raise ValueError('candle batch timeframe mismatch')
        return self


class RiskConfig(VinayakModel):
    risk_per_trade_pct: Decimal = Field(gt=0, le=5)
    max_daily_loss_pct: Decimal = Field(gt=0, le=20)
    max_trades_per_day: int = Field(ge=1, le=100)
    cooldown_minutes: int = Field(ge=0, le=1440)
    allow_live_trading: bool = False
    live_unlock_token_required: bool = True


class StrategyConfig(VinayakModel):
    strategy_name: str = Field(min_length=1, max_length=64)
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: Timeframe
    parameters: dict[str, Any] = Field(default_factory=dict)
    risk: RiskConfig

    @field_validator('strategy_name', 'symbol')
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return str(value or '').strip().upper()


class ValidationResult(VinayakModel):
    is_valid: bool
    reason: str = Field(min_length=1, max_length=128)
    detail: str = Field(default='', max_length=1000)
    validated_at: datetime

    @field_validator('validated_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class TradeSignal(VinayakModel):
    signal_id: UUID = Field(default_factory=uuid4)
    idempotency_key: str = Field(min_length=16, max_length=128)
    strategy_name: str = Field(min_length=1, max_length=64)
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: Timeframe
    signal_type: TradeSignalType
    status: TradeSignalStatus = TradeSignalStatus.CREATED
    generated_at: datetime
    candle_timestamp: datetime
    side: ExecutionSide | None = None
    entry_price: Decimal | None = Field(default=None, gt=0)
    stop_loss: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    quantity: Decimal | None = Field(default=None, gt=0)
    confidence: Decimal = Field(default=Decimal('0'), ge=0, le=1)
    rationale: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator('strategy_name', 'symbol')
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return str(value or '').strip().upper()

    @field_validator('generated_at', 'candle_timestamp')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode='after')
    def _validate_signal(self) -> 'TradeSignal':
        if self.signal_type == TradeSignalType.NO_TRADE:
            if any(value is not None for value in (self.side, self.entry_price, self.stop_loss, self.target_price, self.quantity)):
                raise ValueError('NO_TRADE signal cannot carry execution fields')
            return self
        if self.side is None:
            raise ValueError('signal side is required for executable signals')
        if self.entry_price is None or self.stop_loss is None or self.target_price is None or self.quantity is None:
            raise ValueError('entry_price, stop_loss, target_price, and quantity are required')
        if self.side == ExecutionSide.BUY and not (self.stop_loss < self.entry_price < self.target_price):
            raise ValueError('BUY signal must satisfy stop_loss < entry_price < target_price')
        if self.side == ExecutionSide.SELL and not (self.target_price < self.entry_price < self.stop_loss):
            raise ValueError('SELL signal must satisfy target_price < entry_price < stop_loss')
        return self


class StrategySignalBatch(VinayakModel):
    run_id: UUID = Field(default_factory=uuid4)
    generated_at: datetime
    signals: tuple[TradeSignal, ...] = Field(default_factory=tuple)

    @field_validator('generated_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class ExecutionRequest(VinayakModel):
    request_id: UUID = Field(default_factory=uuid4)
    idempotency_key: str = Field(min_length=16, max_length=128)
    requested_at: datetime
    mode: ExecutionMode
    signal: TradeSignal
    risk: RiskConfig
    account_id: str = Field(min_length=1, max_length=64)

    @field_validator('requested_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode='after')
    def _validate_live_policy(self) -> 'ExecutionRequest':
        if self.signal.signal_type == TradeSignalType.NO_TRADE:
            raise ValueError('NO_TRADE signals cannot be submitted for execution')
        if self.mode == ExecutionMode.LIVE and not self.risk.allow_live_trading:
            raise ValueError('live execution is disabled by risk config')
        return self


class ExecutionResult(VinayakModel):
    execution_id: UUID = Field(default_factory=uuid4)
    request_id: UUID
    status: ExecutionStatus
    failure_reason: ExecutionFailureReason = ExecutionFailureReason.NONE
    processed_at: datetime
    order_reference: str | None = Field(default=None, max_length=128)
    message: str = Field(default='', max_length=500)

    @field_validator('processed_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class PositionSnapshot(VinayakModel):
    position_id: UUID = Field(default_factory=uuid4)
    symbol: str = Field(min_length=1, max_length=64)
    side: ExecutionSide
    quantity: Decimal = Field(gt=0)
    average_price: Decimal = Field(gt=0)
    mark_price: Decimal = Field(gt=0)
    snapshot_at: datetime

    @field_validator('symbol')
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return str(value or '').strip().upper()

    @field_validator('snapshot_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class BacktestTrade(VinayakModel):
    trade_id: UUID = Field(default_factory=uuid4)
    signal_id: UUID
    symbol: str = Field(min_length=1, max_length=64)
    side: ExecutionSide
    entry_time: datetime
    exit_time: datetime
    entry_price: Decimal = Field(gt=0)
    exit_price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    net_pnl: Decimal
    r_multiple: Decimal

    @field_validator('entry_time', 'exit_time')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)

    @model_validator(mode='after')
    def _validate_trade_window(self) -> 'BacktestTrade':
        if self.exit_time < self.entry_time:
            raise ValueError('exit_time cannot be before entry_time')
        return self


class BacktestReport(VinayakModel):
    report_id: UUID = Field(default_factory=uuid4)
    generated_at: datetime
    strategy_name: str = Field(min_length=1, max_length=64)
    symbol: str = Field(min_length=1, max_length=64)
    timeframe: Timeframe
    trade_count: int = Field(ge=0)
    hit_ratio: Decimal = Field(ge=0, le=1)
    profit_factor: Decimal = Field(ge=0)
    max_drawdown: Decimal = Field(ge=0)
    average_r_multiple: Decimal
    trades: tuple[BacktestTrade, ...] = Field(default_factory=tuple)

    @field_validator('generated_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)


class AuditEvent(VinayakModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: AuditEventType
    correlation_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator('occurred_at')
    @classmethod
    def _validate_timestamp(cls, value: datetime) -> datetime:
        return _ensure_utc(value)
