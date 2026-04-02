from __future__ import annotations

from dataclasses import dataclass
from math import floor

from vinayak.strategies.breakout.service import Candle, build_indicator_snapshot, ensure_required_indicator_candles
from vinayak.strategies.common.base import StrategySignal


@dataclass(slots=True)
class BtstConfig:
    allow_stbt: bool = True


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    grouped: dict[object, list[Candle]] = {}
    for candle in candles:
        grouped.setdefault(candle.timestamp.date(), []).append(candle)
    return grouped


def _calculate_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    return floor((capital * risk_pct) / risk_per_unit)


def run_btst_strategy(
    candles: list[Candle],
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: BtstConfig | None = None,
) -> list[StrategySignal]:
    del rr_ratio
    if not candles:
        return []

    cfg = config or BtstConfig()
    candles = ensure_required_indicator_candles(candles)
    grouped = _group_by_day(candles)
    days = sorted(grouped.keys())
    signals: list[StrategySignal] = []

    for idx in range(len(days) - 1):
        today = grouped[days[idx]]
        tomorrow = grouped[days[idx + 1]]
        if not today or not tomorrow:
            continue

        last = today[-1]
        next_open = tomorrow[0]
        side = ''
        bullish = float(last.close) > float(last.vwap) and float(last.close) > float(last.open) and last.ema_9 >= last.ema_21 >= last.ema_50 and last.ema_50 >= last.ema_200 and last.macd >= last.macd_signal and last.rsi >= 55.0
        bearish = float(last.close) < float(last.vwap) and float(last.close) < float(last.open) and last.ema_9 <= last.ema_21 <= last.ema_50 and last.ema_50 <= last.ema_200 and last.macd <= last.macd_signal and last.rsi <= 45.0
        if bullish:
            side = 'BUY'
        elif cfg.allow_stbt and bearish:
            side = 'SELL'
        if not side:
            continue

        entry = float(last.close)
        stop = float(last.low) if side == 'BUY' else float(last.high)
        quantity = _calculate_qty(capital, risk_pct, entry, stop)
        if quantity <= 0:
            continue

        signals.append(
            StrategySignal(
                strategy_name='BTST',
                symbol=symbol,
                side=side,
                entry_price=round(entry, 4),
                stop_loss=round(stop, 4),
                target_price=round(float(next_open.open), 4),
                signal_time=last.timestamp,
                metadata={
                    'quantity': quantity,
                    'day': days[idx].isoformat(),
                    'exit_time': next_open.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'exit_reason': 'NEXT_DAY_OPEN',
                    'next_open_price': round(float(next_open.open), 4),
                    **build_indicator_snapshot(last),
                },
            )
        )

    return signals
