from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import floor

from vinayak.strategies.common.base import StrategySignal


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float = 0.0


def add_intraday_vwap(candles: list[Candle]) -> None:
    current_day = None
    cumulative_pv = 0.0
    cumulative_volume = 0.0

    for candle in candles:
        day = candle.timestamp.date()
        if day != current_day:
            current_day = day
            cumulative_pv = 0.0
            cumulative_volume = 0.0

        cumulative_pv += candle.close * candle.volume
        cumulative_volume += candle.volume
        candle.vwap = candle.close if cumulative_volume == 0 else cumulative_pv / cumulative_volume


def group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    grouped: dict[object, list[Candle]] = {}
    for candle in candles:
        grouped.setdefault(candle.timestamp.date(), []).append(candle)
    return grouped


def calculate_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    return floor((capital * risk_pct) / risk_per_unit)


def run_breakout_strategy(
    candles: list[Candle],
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
) -> list[StrategySignal]:
    if not candles:
        return []

    candles = sorted(candles, key=lambda candle: candle.timestamp)
    add_intraday_vwap(candles)
    grouped = group_by_day(candles)
    signals: list[StrategySignal] = []

    for day in sorted(grouped.keys()):
        day_candles = grouped[day]
        if len(day_candles) < 5:
            continue

        first_15m = day_candles[0]
        first_hour = day_candles[:4]
        hour_open = first_hour[0].open
        hour_close = first_hour[-1].close

        if hour_close > hour_open:
            bias = 'BUY'
        elif hour_close < hour_open:
            bias = 'SELL'
        else:
            continue

        for candle in day_candles[1:]:
            if bias == 'BUY':
                if candle.high <= first_15m.high or candle.close <= candle.vwap:
                    continue
                entry = first_15m.high
                stop = candle.low
                target = entry + (entry - stop) * rr_ratio
                side = 'BUY'
                if stop >= entry:
                    continue
            else:
                if candle.low >= first_15m.low or candle.close >= candle.vwap:
                    continue
                entry = first_15m.low
                stop = candle.high
                target = entry - (stop - entry) * rr_ratio
                side = 'SELL'
                if stop <= entry:
                    continue

            quantity = calculate_qty(capital, risk_pct, entry, stop)
            if quantity <= 0:
                break

            signals.append(
                StrategySignal(
                    strategy_name='Breakout',
                    symbol=symbol,
                    side=side,
                    entry_price=round(entry, 4),
                    stop_loss=round(stop, 4),
                    target_price=round(target, 4),
                    signal_time=candle.timestamp,
                    metadata={
                        'day': str(day),
                        'quantity': quantity,
                        'bias': bias,
                        'vwap': round(candle.vwap, 4),
                    },
                )
            )
            break

    return signals
