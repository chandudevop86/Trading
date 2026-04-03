from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class TradeRecord:
    trade_id: str
    symbol: str
    strategy: str
    side: str
    entry_time: datetime
    exit_time: datetime | None = None
    entry_price: float = 0.0
    exit_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    quantity: int | float = 0
    pnl: float | None = None
    gross_pnl: float | None = None
    fees: float | None = None
    slippage: float | None = None
    status: str = ''
    execution_mode: str = 'paper'
    signal_time: datetime | None = None
    execution_time: datetime | None = None
    validation_passed: bool | None = None
    rejection_reason: str | None = None
    zone_score: float | None = None
    vwap_alignment: bool | None = None
    adx_value: float | None = None
    trend_ok: bool | None = None
    volatility_ok: bool | None = None
    chop_ok: bool | None = None
    duplicate_blocked: bool | None = None
    retest_confirmed: bool | None = None
    move_away_score: float | None = None
    freshness_score: float | None = None
    rejection_strength: float | None = None
    structure_clarity: float | None = None


@dataclass(slots=True)
class CandleRecord:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    rsi: float | None = None
    adx: float | None = None
    macd: float | None = None


@dataclass(slots=True)
class SystemHealthSnapshot:
    timestamp: datetime
    data_latency_ms: float | None = None
    api_latency_ms: float | None = None
    signal_generation_success: bool | None = None
    execution_success: bool | None = None
    pipeline_ok: bool | None = None
    telegram_ok: bool | None = None
    broker_ok: bool | None = None
    error_message: str | None = None


@dataclass(slots=True)
class ReadinessReport:
    overall_status: str
    score: float
    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: str = ''
    recommended_actions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            'overall_status': self.overall_status,
            'score': round(float(self.score), 2),
            'passed_checks': list(self.passed_checks),
            'failed_checks': list(self.failed_checks),
            'warnings': list(self.warnings),
            'summary': self.summary,
            'recommended_actions': list(self.recommended_actions),
        }
