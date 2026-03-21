from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class CandleInput(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class BreakoutRunRequest(BaseModel):
    symbol: str = Field(default='^NSEI')
    capital: float = Field(default=100000.0, gt=0)
    risk_pct: float = Field(default=0.01, gt=0)
    rr_ratio: float = Field(default=2.0, gt=0)
    save_signals: bool = Field(default=False)
    candles: list[CandleInput]


class DemandSupplyRunRequest(BaseModel):
    symbol: str = Field(default='^NSEI')
    capital: float = Field(default=100000.0, gt=0)
    risk_pct: float = Field(default=0.01, gt=0)
    rr_ratio: float = Field(default=2.0, gt=0)
    include_fvg: bool = Field(default=True)
    include_bos: bool = Field(default=True)
    save_signals: bool = Field(default=False)
    candles: list[CandleInput]


class OneTradeDayRunRequest(BaseModel):
    symbol: str = Field(default='^NSEI')
    capital: float = Field(default=100000.0, gt=0)
    risk_pct: float = Field(default=0.01, gt=0)
    rr_ratio: float = Field(default=2.0, gt=0)
    entry_cutoff_hhmm: str = Field(default='')
    save_signals: bool = Field(default=False)
    candles: list[CandleInput]


class MtfRunRequest(BaseModel):
    symbol: str = Field(default='^NSEI')
    capital: float = Field(default=100000.0, gt=0)
    risk_pct: float = Field(default=0.01, gt=0)
    rr_ratio: float = Field(default=2.0, gt=0)
    ema_period: int = Field(default=3, ge=2)
    setup_mode: str = Field(default='either')
    require_retest_strength: bool = Field(default=True)
    save_signals: bool = Field(default=False)
    candles: list[CandleInput]


class ReviewedTradeCreateRequest(BaseModel):
    signal_id: int | None = Field(default=None, gt=0)
    strategy_name: str | None = None
    symbol: str | None = None
    side: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    quantity: int = Field(default=1, gt=0)
    lots: int = Field(default=1, gt=0)
    status: str = Field(default='REVIEWED', min_length=1)
    notes: str | None = None


class SignalReviewCreateRequest(BaseModel):
    quantity: int = Field(default=1, gt=0)
    lots: int = Field(default=1, gt=0)
    status: str = Field(default='REVIEWED', min_length=1)
    notes: str | None = None


class ReviewedTradeStatusUpdateRequest(BaseModel):
    status: str = Field(min_length=1)
    notes: str | None = None
    quantity: int | None = Field(default=None, gt=0)
    lots: int | None = Field(default=None, gt=0)


class ExecutionCreateRequest(BaseModel):
    signal_id: int | None = Field(default=None, gt=0)
    reviewed_trade_id: int | None = Field(default=None, gt=0)
    mode: str = Field(min_length=1)
    broker: str = Field(min_length=1)
    status: str | None = None
    executed_price: float | None = None

    @model_validator(mode='after')
    def validate_reference(self) -> 'ExecutionCreateRequest':
        if self.signal_id is None and self.reviewed_trade_id is None:
            raise ValueError('signal_id or reviewed_trade_id is required')
        return self
