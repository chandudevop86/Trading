from __future__ import annotations

from datetime import datetime

from src.breakout_bot import Candle


def _is_pivot_high(candles: list[Candle], idx: int, window: int) -> bool:
    current = candles[idx]
    left = candles[idx - window : idx]
    right = candles[idx + 1 : idx + window + 1]
    neighbors = left + right
    return all(current.high > c.high for c in neighbors)


def _is_pivot_low(candles: list[Candle], idx: int, window: int) -> bool:
    current = candles[idx]
    left = candles[idx - window : idx]
    right = candles[idx + 1 : idx + window + 1]
    neighbors = left + right
    return all(current.low < c.low for c in neighbors)


def _classify_price_action(candle: Candle) -> str:
    candle_range = candle.high - candle.low
    if candle_range <= 0:
        return "DOJI"

    body = abs(candle.close - candle.open)
    body_ratio = body / candle_range
    close_near_high = (candle.high - candle.close) / candle_range <= 0.2
    close_near_low = (candle.close - candle.low) / candle_range <= 0.2

    if body_ratio < 0.25:
        return "INDECISION"

    if candle.close > candle.open:
        return "BULLISH_MARUBOZU" if close_near_high else "BULLISH"

    if candle.close < candle.open:
        return "BEARISH_MARUBOZU" if close_near_low else "BEARISH"

    return "DOJI"


def _find_entry_index(candles: list[Candle], entry_time: datetime) -> int:
    for idx, candle in enumerate(candles):
        if candle.timestamp == entry_time:
            return idx

    last_idx = 0
    for idx, candle in enumerate(candles):
        if candle.timestamp <= entry_time:
            last_idx = idx
        else:
            break
    return last_idx


def annotate_trades_with_zones(
    trades: list[dict[str, object]],
    candles: list[Candle],
    pivot_window: int = 2,
) -> list[dict[str, object]]:
    by_day: dict = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)

    for day in by_day:
        by_day[day].sort(key=lambda c: c.timestamp)

    annotated: list[dict[str, object]] = []
    for trade in trades:
        trade_copy = dict(trade)
        entry_time = datetime.fromisoformat(str(trade_copy["entry_time"]))
        day_candles = by_day.get(entry_time.date(), [])

        trade_copy.update(
            {
                "price_action": "NA",
                "demand_zone_low": "NA",
                "demand_zone_high": "NA",
                "supply_zone_low": "NA",
                "supply_zone_high": "NA",
            }
        )

        if len(day_candles) < (pivot_window * 2 + 1):
            annotated.append(trade_copy)
            continue

        entry_idx = _find_entry_index(day_candles, entry_time)
        entry_candle = day_candles[entry_idx]
        trade_copy["price_action"] = _classify_price_action(entry_candle)

        latest_supply = None
        latest_demand = None
        for idx in range(pivot_window, entry_idx):
            if idx + pivot_window >= len(day_candles):
                break
            if _is_pivot_high(day_candles, idx, pivot_window):
                latest_supply = day_candles[idx]
            if _is_pivot_low(day_candles, idx, pivot_window):
                latest_demand = day_candles[idx]

        if latest_demand is not None:
            trade_copy["demand_zone_low"] = round(latest_demand.low, 4)
            trade_copy["demand_zone_high"] = round(min(latest_demand.open, latest_demand.close), 4)

        if latest_supply is not None:
            trade_copy["supply_zone_low"] = round(max(latest_supply.open, latest_supply.close), 4)
            trade_copy["supply_zone_high"] = round(latest_supply.high, 4)

        annotated.append(trade_copy)
    return annotated
