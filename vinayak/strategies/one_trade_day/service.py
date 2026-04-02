from __future__ import annotations

from datetime import time
from math import floor

from vinayak.strategies.breakout.service import Candle, build_indicator_snapshot, ensure_required_indicator_candles
from vinayak.strategies.common.base import StrategySignal
from vinayak.strategies.indicator.service import IndicatorConfig, run_indicator_strategy


def _group_day_indices(candles: list[Candle]) -> dict[object, list[int]]:
    grouped: dict[object, list[int]] = {}
    for idx, candle in enumerate(candles):
        grouped.setdefault(candle.timestamp.date(), []).append(idx)
    return grouped


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk = abs(entry - stop)
    if risk <= 0:
        return 0
    return floor((capital * risk_pct) / risk)


def _parse_cutoff(hhmm: str) -> time | None:
    text = (hhmm or '').strip()
    if not text:
        return None
    hour, minute = text.split(':', 1)
    return time(hour=int(hour), minute=int(minute))


def _side_from_signal(signal: StrategySignal) -> str:
    return signal.side if signal.side in {'BUY', 'SELL'} else ''


def run_one_trade_day_strategy(
    candles: list[Candle],
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    entry_cutoff_hhmm: str = '',
    config: IndicatorConfig | None = None,
) -> list[StrategySignal]:
    if not candles:
        return []

    candles = ensure_required_indicator_candles(candles)
    indicator_signals = run_indicator_strategy(candles=candles, symbol=symbol, config=config)
    indicator_by_time = {signal.signal_time: signal for signal in indicator_signals}
    grouped = _group_day_indices(candles)
    cutoff = _parse_cutoff(entry_cutoff_hhmm)
    results: list[StrategySignal] = []

    for day in sorted(grouped.keys()):
        indices = grouped[day]
        chosen: StrategySignal | None = None
        for idx in indices:
            candle = candles[idx]
            if cutoff is not None and candle.timestamp.time() > cutoff:
                continue
            signal = indicator_by_time.get(candle.timestamp)
            if signal is None:
                continue
            side = _side_from_signal(signal)
            if not side:
                continue

            entry = float(candle.close)
            stop = float(candle.low) if side == 'BUY' else float(candle.high)
            risk = abs(entry - stop)
            if risk <= 0:
                continue
            target = entry + (risk * rr_ratio) if side == 'BUY' else entry - (risk * rr_ratio)
            quantity = _calc_qty(capital, risk_pct, entry, stop)
            if quantity <= 0:
                continue

            chosen = StrategySignal(
                strategy_name='One Trade/Day',
                symbol=symbol,
                side=side,
                entry_price=round(entry, 4),
                stop_loss=round(stop, 4),
                target_price=round(target, 4),
                signal_time=candle.timestamp,
                metadata={
                    'quantity': quantity,
                    'entry_cutoff': entry_cutoff_hhmm,
                    'market_signal': signal.metadata.get('market_signal', ''),
                    **build_indicator_snapshot(candle),
                },
            )
            break

        if chosen is not None:
            results.append(chosen)

    return results
