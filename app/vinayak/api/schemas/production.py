from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from vinayak.domain.models import (
    AuditEvent,
    Candle,
    CandleBatch,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    RiskConfig,
    StrategyConfig,
    StrategySignalBatch,
)


class SignalRunRequest(BaseModel):
    symbol: str = Field(min_length=1, default='NIFTY')
    timeframe: str = Field(min_length=2, default='5m')
    lookback: int = Field(ge=20, le=1000, default=200)
    strategy: str = Field(min_length=1, default='BREAKOUT')
    risk_per_trade_pct: Decimal = Field(gt=0, le=5, default=Decimal('1'))
    max_daily_loss_pct: Decimal = Field(gt=0, le=20, default=Decimal('3'))
    max_trades_per_day: int = Field(ge=1, le=100, default=5)
    cooldown_minutes: int = Field(ge=0, le=1440, default=15)


class SignalRunResponse(BaseModel):
    candles: CandleBatch
    signals: StrategySignalBatch


class ExecutionSubmitResponse(BaseModel):
    result: ExecutionResult


class AdminAuditResponse(BaseModel):
    payload: dict[str, object]
