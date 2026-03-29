from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.runtime_defaults import DEFAULT_PERIOD


@dataclass(slots=True)
class TradingActionRequest:
    strategy: str
    symbol: str
    timeframe: str
    capital: float
    risk_pct: float
    rr_ratio: float
    mode: str
    broker_choice: str
    run_requested: bool = False
    backtest_requested: bool = False


@dataclass(slots=True)
class TradingActionResult:
    candles: pd.DataFrame
    trades: list[dict[str, object]]
    period: str
    status: str
    broker_status: str
    active_summary: dict[str, object]
    backtest_summary: dict[str, object]
    paper_summary: dict[str, object]
    market_data_summary: dict[str, object]
    todays_trades: int
    execution_messages: list[tuple[str, str]]


def period_for_interval(interval: str, *, default_period: str = DEFAULT_PERIOD) -> str:
    mapping = {
        '1m': '7d',
        '5m': '60d',
        '15m': '60d',
        '30m': '60d',
        '1h': '730d',
        '1d': '1y',
    }
    return mapping.get(interval, default_period)
