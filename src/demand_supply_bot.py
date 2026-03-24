from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from src.breakout_bot import Candle, _coerce_candles, add_intraday_vwap
from src.trading_core import ScoringConfig, StandardTrade, safe_quantity, weighted_score


@dataclass(frozen=True, slots=True)
class Zone:
    kind: str
    low: float
    high: float
    idx: int


@dataclass(slots=True)
class DemandSupplyConfig:
    mode: str = 'Balanced'
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    touch_tolerance_pct: float = 0.005
    max_trades_per_day: int = 1
    scoring: ScoringConfig = field(default_factory=ScoringConfig)


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    by_day: dict[object, list[Candle]] = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _find_zones(day_candles: list[Candle], pivot_window: int) -> list[Zone]:
    n = len(day_candles)
    w = max(1, int(pivot_window or 1))
    zones: list[Zone] = []
    for i in range(w, n - w):
        candle = day_candles[i]
        lows = [day_candles[j].low for j in range(i - w, i + w + 1) if j != i]
        highs = [day_candles[j].high for j in range(i - w, i + w + 1) if j != i]
        if lows and candle.low < min(lows):
            zones.append(Zone(kind='demand', low=float(candle.low), high=float(max(candle.open, candle.close)), idx=i))
        if highs and candle.high > max(highs):
            zones.append(Zone(kind='supply', low=float(min(candle.open, candle.close)), high=float(candle.high), idx=i))
    return zones


def _trend_ok(day_candles: list[Candle], idx: int, side: str) -> bool:
    if idx < 2:
        return False
    fast = sum(c.close for c in day_candles[max(0, idx - 2): idx + 1]) / min(3, idx + 1)
    slow = sum(c.close for c in day_candles[max(0, idx - 5): idx + 1]) / min(6, idx + 1)
    close = float(day_candles[idx].close)
    if side == 'BUY':
        return close >= fast >= slow
    return close <= fast <= slow


def _score_retest(day_candles: list[Candle], idx: int, zone: Zone, side: str, config: DemandSupplyConfig) -> tuple[float, str] | None:
    candle = day_candles[idx]
    prev_close = float(day_candles[idx - 1].close) if idx > 0 else float(candle.close)
    close = float(candle.close)
    vwap_ok = close >= float(candle.vwap) if side == 'BUY' else close <= float(candle.vwap)
    rsi_proxy = close >= prev_close if side == 'BUY' else close <= prev_close
    adx_proxy = abs(close - prev_close) >= max(abs(close) * 0.0015, 0.2)
    score = weighted_score(
        {
            'trend': _trend_ok(day_candles, idx, side),
            'vwap': vwap_ok,
            'rsi': rsi_proxy,
            'adx': adx_proxy,
            'zone': True,
            'fvg': False,
            'sweep': (float(candle.low) < float(zone.low)) if side == 'BUY' else (float(candle.high) > float(zone.high)),
            'retest': True,
        },
        config.scoring,
    )
    if not score.accepted:
        return None
    return score.total, f'{side.lower()} zone retest score={score.total:.2f}'


def generate_trades(
    df: Any,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: DemandSupplyConfig | None = None,
    *,
    trailing_sl_pct: float = 0.0,
    pivot_window: int = 2,
    touch_tolerance_pct: float = 0.005,
    max_trades_per_day: int = 1,
) -> list[dict[str, object]]:
    cfg = config or DemandSupplyConfig()
    if config is None:
        cfg.trailing_sl_pct = float(trailing_sl_pct)
        cfg.pivot_window = int(pivot_window)
        cfg.touch_tolerance_pct = float(touch_tolerance_pct)
        cfg.max_trades_per_day = int(max_trades_per_day)

    candles = _coerce_candles(df)
    if not candles:
        return []
    add_intraday_vwap(candles)
    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        day_candles = sorted(by_day[day], key=lambda c: c.timestamp)
        if len(day_candles) < (cfg.pivot_window * 2 + 3):
            continue
        zones = _find_zones(day_candles, pivot_window=cfg.pivot_window)
        if not zones:
            continue
        trades_taken = 0
        for zone in sorted(zones, key=lambda z: z.idx, reverse=True):
            if trades_taken >= int(cfg.max_trades_per_day or 1):
                break
            side = ''
            entry_idx = -1
            entry = 0.0
            stop = 0.0
            score_value = 0.0
            reason = ''
            tol = float(cfg.touch_tolerance_pct or 0.0)

            for i in range(zone.idx + 1, len(day_candles)):
                candle = day_candles[i]
                if zone.kind == 'demand':
                    touch = float(candle.low) <= float(zone.high) * (1.0 + tol)
                    if touch and float(candle.close) > float(zone.high):
                        score_result = _score_retest(day_candles, i, zone, 'BUY', cfg)
                        if score_result is None:
                            continue
                        score_value, reason = score_result
                        side = 'BUY'
                        entry_idx = i
                        entry = float(zone.high)
                        stop = min(float(candle.low), float(zone.low))
                        if stop >= entry:
                            stop = entry * 0.999
                        break
                else:
                    touch = float(candle.high) >= float(zone.low) * (1.0 - tol)
                    if touch and float(candle.close) < float(zone.low):
                        score_result = _score_retest(day_candles, i, zone, 'SELL', cfg)
                        if score_result is None:
                            continue
                        score_value, reason = score_result
                        side = 'SELL'
                        entry_idx = i
                        entry = float(zone.low)
                        stop = max(float(candle.high), float(zone.high))
                        if stop <= entry:
                            stop = entry * 1.001
                        break

            if entry_idx < 0:
                continue

            qty = safe_quantity(capital=capital, risk_pct=risk_pct, entry=entry, stop_loss=stop)
            if qty <= 0:
                continue

            target = entry + (entry - stop) * float(rr_ratio or 2.0) if side == 'BUY' else entry - (stop - entry) * float(rr_ratio or 2.0)
            trail_stop = stop
            exit_price = float(day_candles[-1].close)
            exit_time = day_candles[-1].timestamp
            exit_reason = 'EOD'

            for i in range(entry_idx + 1, len(day_candles)):
                candle = day_candles[i]
                if cfg.trailing_sl_pct and float(cfg.trailing_sl_pct) > 0:
                    if side == 'BUY':
                        trail_stop = max(trail_stop, float(candle.high) * (1.0 - float(cfg.trailing_sl_pct)))
                    else:
                        trail_stop = min(trail_stop, float(candle.low) * (1.0 + float(cfg.trailing_sl_pct)))
                if side == 'BUY':
                    if candle.low <= trail_stop:
                        exit_price = float(trail_stop)
                        exit_time = candle.timestamp
                        exit_reason = 'TRAILING_STOP' if trail_stop > stop else 'STOP_LOSS'
                        break
                    if candle.high >= target:
                        exit_price = float(target)
                        exit_time = candle.timestamp
                        exit_reason = 'TARGET'
                        break
                else:
                    if candle.high >= trail_stop:
                        exit_price = float(trail_stop)
                        exit_time = candle.timestamp
                        exit_reason = 'TRAILING_STOP' if trail_stop < stop else 'STOP_LOSS'
                        break
                    if candle.low <= target:
                        exit_price = float(target)
                        exit_time = candle.timestamp
                        exit_reason = 'TARGET'
                        break

            pnl = (exit_price - entry) * qty if side == 'BUY' else (entry - exit_price) * qty
            trade = StandardTrade(
                timestamp=day_candles[entry_idx].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                side=side,
                entry=entry,
                stop_loss=stop,
                target=target,
                strategy='DEMAND_SUPPLY',
                reason=reason,
                score=score_value,
                entry_price=entry,
                target_price=target,
                risk_per_unit=abs(entry - stop),
                quantity=int(qty),
                zone_type=zone.kind,
                extra={
                    'day': day.isoformat(),
                    'zone_kind': zone.kind,
                    'zone_low': round(float(zone.low), 4),
                    'zone_high': round(float(zone.high), 4),
                    'zone_price': round(float(entry), 4),
                    'trailing_stop_loss': round(float(trail_stop), 4),
                    'exit_time': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'exit_price': round(float(exit_price), 4),
                    'exit_reason': exit_reason,
                    'pnl': round(float(pnl), 2),
                },
            ).to_dict()
            trades.append(trade)
            trades_taken += 1
    return trades
