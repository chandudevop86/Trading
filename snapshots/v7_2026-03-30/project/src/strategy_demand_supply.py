from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

import pandas as pd

from src.breakout_bot import Candle, _coerce_candles
from src.demand_supply_bot import (
    DemandSupplyConfig,
    generate_trades as _generate_trades,
    mark_zone_retest_state as _mark_zone_retest_state,
)
from src.strategy_common import session_window
from src.trading_core import ScoreThresholds


_SESSION_ALLOWED = {'OPENING', 'OPENING_BUFFER', 'MORNING'}


def _coerce_config(config: DemandSupplyConfig | Mapping[str, Any] | None) -> DemandSupplyConfig:
    """Normalize strategy config without breaking the existing generator signature."""
    if isinstance(config, DemandSupplyConfig):
        return config
    cfg = DemandSupplyConfig()
    if isinstance(config, Mapping):
        raw = dict(config)
        threshold = raw.pop('score_threshold', None)
        for key, value in raw.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)
        if threshold is not None:
            score_value = float(threshold)
            cfg.scoring.thresholds = ScoreThresholds(
                conservative=score_value,
                balanced=score_value,
                aggressive=score_value,
            )
    return cfg


def is_session_valid(ts: object, config: DemandSupplyConfig | Mapping[str, Any] | None = None) -> bool:
    """Return True only for the intended Nifty 5m morning session."""
    cfg = _coerce_config(config)
    timestamp = pd.Timestamp(ts).to_pydatetime() if not isinstance(ts, datetime) else ts
    window = session_window(
        timestamp,
        morning_start=cfg.morning_session_start,
        morning_end=cfg.morning_session_end,
        midday_start=cfg.midday_start,
        midday_end=cfg.midday_end,
        allow_afternoon_session=False,
        afternoon_start=cfg.afternoon_session_start,
        afternoon_end=cfg.afternoon_session_end,
    )
    return window in _SESSION_ALLOWED or str(window).upper() == 'OPEN'


def is_vwap_valid(price: float, vwap: float, side: str, config: DemandSupplyConfig | Mapping[str, Any] | None = None) -> bool:
    """Apply the directional VWAP filter used by the strict demand/supply strategy."""
    cfg = _coerce_config(config)
    buffer_pct = max(float(cfg.vwap_reclaim_buffer_pct), 0.0)
    normalized_side = str(side or '').strip().upper()
    if normalized_side == 'BUY':
        return float(price) >= float(vwap) * (1.0 + buffer_pct)
    if normalized_side == 'SELL':
        return float(price) <= float(vwap) * (1.0 - buffer_pct)
    return False


def is_retest(df: Any, i: int, zone_low: float, zone_high: float, lookback: int = 8) -> bool:
    """Detect whether price left a zone and has now returned within the allowed lookback."""
    frame = pd.DataFrame(df).copy()
    if frame.empty or i <= 0 or i >= len(frame):
        return False
    row = frame.iloc[i]
    touched_now = float(row['low']) <= float(zone_high) and float(row['high']) >= float(zone_low)
    if not touched_now:
        return False
    start = max(0, int(i) - max(int(lookback), 1))
    prior = frame.iloc[start:i]
    if prior.empty:
        return False
    left_zone = (
        (prior['low'].astype(float) > float(zone_high)).any()
        or (prior['high'].astype(float) < float(zone_low)).any()
    )
    return bool(left_zone)


def rejection_candle(row: Mapping[str, Any], side: str, config: DemandSupplyConfig | Mapping[str, Any] | None = None) -> bool:
    """Check for a directional rejection candle suitable for retest confirmation."""
    cfg = _coerce_config(config)
    candle_range = max(float(row['high']) - float(row['low']), 0.0001)
    body = max(abs(float(row['close']) - float(row['open'])), 0.01)
    lower_wick = max(min(float(row['open']), float(row['close'])) - float(row['low']), 0.0)
    upper_wick = max(float(row['high']) - max(float(row['open']), float(row['close'])), 0.0)
    wick_body_ratio = (lower_wick / body) if str(side or '').strip().upper() == 'BUY' else (upper_wick / body)
    wick_dominance = (lower_wick / max(upper_wick, 0.01)) if str(side or '').strip().upper() == 'BUY' else (upper_wick / max(lower_wick, 0.01))
    close_pos = (float(row['close']) - float(row['low'])) / candle_range
    normalized_side = str(side or '').strip().upper()
    if normalized_side == 'BUY':
        min_wick_body = min(float(cfg.strict_rejection_wick_body_ratio), 1.2)
        min_dominance = min(float(cfg.strict_wick_dominance_ratio), 1.2)
        min_close = min(float(cfg.strict_close_position_buy), 0.65)
        return wick_body_ratio >= min_wick_body and wick_dominance >= min_dominance and close_pos >= min_close
    if normalized_side == 'SELL':
        min_wick_body = min(float(cfg.strict_rejection_wick_body_ratio), 1.2)
        min_dominance = min(float(cfg.strict_wick_dominance_ratio), 1.2)
        max_close = max(float(cfg.strict_close_position_sell), 0.35)
        return wick_body_ratio >= min_wick_body and wick_dominance >= min_dominance and close_pos <= max_close
    return False


def mark_zone_retest_state(state: Mapping[str, Any], *, event: str, candle_idx: int) -> dict[str, object]:
    """Track the current retest-cycle state for one zone."""
    return _mark_zone_retest_state(dict(state), event=event, candle_idx=int(candle_idx))

def generate_trades(df: Any, capital: float, risk_pct: float, rr_ratio: float, config: DemandSupplyConfig | Mapping[str, Any] | None = None):
    """Compatibility wrapper around the production demand/supply generator."""
    return _generate_trades(df, capital=capital, risk_pct=risk_pct, rr_ratio=rr_ratio, config=_coerce_config(config))


__all__ = [
    'DemandSupplyConfig',
    'generate_trades',
    'is_retest',
    'is_session_valid',
    'is_vwap_valid',
    'rejection_candle',
]





