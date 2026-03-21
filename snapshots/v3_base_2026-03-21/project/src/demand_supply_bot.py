from __future__ import annotations

from dataclasses import dataclass
from math import floor

from src.breakout_bot import Candle


@dataclass(frozen=True)
class Zone:
    kind: str  # 'demand' or 'supply'
    low: float
    high: float
    idx: int


def _group_by_day(candles: list[Candle]) -> dict:
    by_day: dict = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _find_zones(day_candles: list[Candle], pivot_window: int) -> list[Zone]:
    n = len(day_candles)
    w = max(1, int(pivot_window or 1))
    zones: list[Zone] = []

    for i in range(w, n - w):
        c = day_candles[i]
        lows = [day_candles[j].low for j in range(i - w, i + w + 1) if j != i]
        highs = [day_candles[j].high for j in range(i - w, i + w + 1) if j != i]

        # Strict pivots to avoid flat areas creating many zones.
        if lows and c.low < min(lows):
            zone_low = float(c.low)
            zone_high = float(max(c.open, c.close))
            zones.append(Zone(kind="demand", low=zone_low, high=zone_high, idx=i))

        if highs and c.high > max(highs):
            zone_low = float(min(c.open, c.close))
            zone_high = float(c.high)
            zones.append(Zone(kind="supply", low=zone_low, high=zone_high, idx=i))

    return zones


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    risk_amount = float(capital or 0.0) * float(risk_pct or 0.0)
    if risk_amount <= 0:
        return 0
    return floor(risk_amount / risk_per_unit)


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
    pivot_window: int = 2,
    touch_tolerance_pct: float = 0.005,
    max_trades_per_day: int = 1,
) -> list[dict[str, object]]:
    """Demand/Supply retest strategy.

    1. Finds pivot-based demand/supply zones within each day.
    2. Enters on first retest (touch zone + close back through zone edge).
    3. Exits on stop/target/trailing stop, else EOD.

    Returns trade dicts with `pnl` so UI can compute win rate.
    """

    if not candles:
        return []

    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        day_candles = sorted(by_day[day], key=lambda c: c.timestamp)
        if len(day_candles) < (pivot_window * 2 + 3):
            continue

        zones = _find_zones(day_candles, pivot_window=pivot_window)
        if not zones:
            continue

        trades_taken = 0
        for zone in sorted(zones, key=lambda z: z.idx, reverse=True):
            if trades_taken >= int(max_trades_per_day or 1):
                break

            side = ""
            entry_idx = -1
            entry = 0.0
            stop = 0.0

            tol = float(touch_tolerance_pct or 0.0)

            # Look for a retest after the pivot candle.
            for i in range(zone.idx + 1, len(day_candles)):
                c = day_candles[i]

                if zone.kind == "demand":
                    touch = float(c.low) <= float(zone.high) * (1.0 + tol)
                    if touch and float(c.close) > float(zone.high):
                        side = "BUY"
                        entry_idx = i
                        entry = float(zone.high)
                        stop = min(float(c.low), float(zone.low))
                        if stop >= entry:
                            stop = entry * 0.999
                        break

                else:
                    touch = float(c.high) >= float(zone.low) * (1.0 - tol)
                    if touch and float(c.close) < float(zone.low):
                        side = "SELL"
                        entry_idx = i
                        entry = float(zone.low)
                        stop = max(float(c.high), float(zone.high))
                        if stop <= entry:
                            stop = entry * 1.001
                        break

            if entry_idx < 0:
                continue

            qty = _calc_qty(capital=capital, risk_pct=risk_pct, entry=entry, stop=stop)
            if qty <= 0:
                continue

            if side == "BUY":
                target = entry + (entry - stop) * float(rr_ratio or 2.0)
            else:
                target = entry - (stop - entry) * float(rr_ratio or 2.0)

            trail_stop = stop
            exit_price = float(day_candles[-1].close)
            exit_time = day_candles[-1].timestamp
            exit_reason = "EOD"

            for i in range(entry_idx + 1, len(day_candles)):
                c = day_candles[i]

                if trailing_sl_pct and float(trailing_sl_pct) > 0:
                    t = float(trailing_sl_pct)
                    if side == "BUY":
                        trail_stop = max(trail_stop, float(c.high) * (1.0 - t))
                    else:
                        trail_stop = min(trail_stop, float(c.low) * (1.0 + t))

                if side == "BUY":
                    if c.low <= trail_stop:
                        exit_price = float(trail_stop)
                        exit_time = c.timestamp
                        exit_reason = "TRAILING_STOP" if trail_stop > stop else "STOP_LOSS"
                        break
                    if c.high >= target:
                        exit_price = float(target)
                        exit_time = c.timestamp
                        exit_reason = "TARGET"
                        break
                else:
                    if c.high >= trail_stop:
                        exit_price = float(trail_stop)
                        exit_time = c.timestamp
                        exit_reason = "TRAILING_STOP" if trail_stop < stop else "STOP_LOSS"
                        break
                    if c.low <= target:
                        exit_price = float(target)
                        exit_time = c.timestamp
                        exit_reason = "TARGET"
                        break

            pnl = (exit_price - entry) * qty if side == "BUY" else (entry - exit_price) * qty

            trades.append(
                {
                    "strategy": "DEMAND_SUPPLY",
                    "day": day.isoformat(),
                    "zone_kind": zone.kind,
                    "zone_low": round(float(zone.low), 4),
                    "zone_high": round(float(zone.high), 4),
                    "zone_price": round(float(entry), 4),
                    "entry_time": day_candles[entry_idx].timestamp.isoformat(sep=" "),
                    "side": side,
                    "entry_price": round(float(entry), 4),
                    "stop_loss": round(float(stop), 4),
                    "trailing_stop_loss": round(float(trail_stop), 4),
                    "target_price": round(float(target), 4),
                    "quantity": int(qty),
                    "exit_time": exit_time.isoformat(sep=" "),
                    "exit_price": round(float(exit_price), 4),
                    "exit_reason": exit_reason,
                    "pnl": round(float(pnl), 2),
                }
            )
            trades_taken += 1

    return trades