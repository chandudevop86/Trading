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


class LiveAnalysisRequest(BaseModel):
    symbol: str = Field(default='^NSEI')
    interval: str = Field(default='5m')
    period: str = Field(default='1d')
    strategy: str = Field(default='Breakout')
    capital: float = Field(default=100000.0, gt=0)
    risk_pct: float = Field(default=1.0, gt=0)
    rr_ratio: float = Field(default=2.0, gt=0)
    trailing_sl_pct: float = Field(default=0.5, ge=0)
    strike_step: int = Field(default=50, gt=0)
    moneyness: str = Field(default='ATM')
    strike_steps: int = Field(default=0, ge=0)
    fetch_option_metrics: bool = Field(default=False)
    send_telegram: bool = Field(default=False)
    telegram_token: str = Field(default='')
    telegram_chat_id: str = Field(default='')
    auto_execute: bool = Field(default=False)
    execution_type: str = Field(default='NONE')
    lot_size: int = Field(default=0, ge=0)
    lots: int = Field(default=0, ge=0)
    security_map_path: str = Field(default='data/dhan_security_map.csv')
    paper_log_path: str = Field(default='vinayak/data/paper_trading_logs_all.csv')
    live_log_path: str = Field(default='vinayak/data/live_trading_logs_all.csv')
    mtf_ema_period: int = Field(default=3, ge=2)
    mtf_setup_mode: str = Field(default='either')
    mtf_retest_strength: bool = Field(default=True)
    mtf_max_trades_per_day: int = Field(default=3, ge=1)
    entry_cutoff_hhmm: str = Field(default='')
    cost_bps: float = Field(default=0.0, ge=0)
    fixed_cost_per_trade: float = Field(default=0.0, ge=0)
    max_daily_loss: float | None = Field(default=None, ge=0)
    max_trades_per_day: int | None = Field(default=None, ge=1)

