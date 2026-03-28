from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class VolatilitySnapshot:
    atr_pct: float = 0.0
    opening_volatility_pct: float = 0.0
    vwap_deviation_pct: float = 0.0
    expansion_ratio: float = 0.0
    volatility_score: int = 0
    market_state: str = 'QUIET'
    volatility_decision: str = 'NO_TRADE_LOW_VOL'
    trade_allowed: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == '':
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def evaluate_volatility_snapshot(row: dict[str, object]) -> dict[str, object]:
    atr_pct = _safe_float(row.get('atr_pct'))
    opening_volatility_pct = _safe_float(row.get('opening_volatility_pct'))
    vwap_deviation_pct = _safe_float(row.get('vwap_deviation_pct'))
    expansion_ratio = _safe_float(row.get('expansion_ratio'))

    score = 0
    if atr_pct > 0.5:
        score += 2
    elif atr_pct > 0.3:
        score += 1

    if opening_volatility_pct > 0.4:
        score += 2
    elif opening_volatility_pct > 0.25:
        score += 1

    if vwap_deviation_pct > 0.5:
        score += 2
    elif vwap_deviation_pct > 0.2:
        score += 1

    if expansion_ratio > 1.5:
        score += 2
    elif expansion_ratio > 1.0:
        score += 1

    if atr_pct < 0.3:
        decision = 'NO_TRADE_LOW_VOL'
        market_state = 'QUIET'
    elif opening_volatility_pct < 0.25:
        decision = 'NO_TRADE_WEAK_OPEN'
        market_state = 'QUIET'
    elif vwap_deviation_pct < 0.2:
        decision = 'NO_MOMENTUM'
        market_state = 'RANGE_BOUND'
    elif expansion_ratio < 1.0:
        decision = 'WEAK_CANDLE'
        market_state = 'RANGE_BOUND'
    else:
        decision = 'TRADE_ALLOWED'
        market_state = 'TRENDING' if score >= 6 else 'NORMAL'
        if score >= 8:
            market_state = 'EXPLOSIVE'

    snapshot = VolatilitySnapshot(
        atr_pct=round(atr_pct, 2),
        opening_volatility_pct=round(opening_volatility_pct, 2),
        vwap_deviation_pct=round(vwap_deviation_pct, 2),
        expansion_ratio=round(expansion_ratio, 2),
        volatility_score=int(score),
        market_state=market_state,
        volatility_decision=decision,
        trade_allowed=decision == 'TRADE_ALLOWED' and score >= 6,
    )
    return snapshot.to_dict()


def latest_volatility_snapshot(candles: pd.DataFrame) -> dict[str, object]:
    if candles is None or getattr(candles, 'empty', True):
        return VolatilitySnapshot().to_dict()
    latest = candles.iloc[-1].to_dict()
    return evaluate_volatility_snapshot(latest)


__all__ = ['VolatilitySnapshot', 'evaluate_volatility_snapshot', 'latest_volatility_snapshot']
