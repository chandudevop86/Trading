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
    ema_9: float = 0.0
    ema_21: float = 0.0
    ema_50: float = 0.0
    ema_200: float = 0.0
    rsi: float = 0.0
    macd: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0


def _ema_series(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (float(period) + 1.0)
    output = [float(values[0])]
    for value in values[1:]:
        output.append((float(value) * alpha) + (output[-1] * (1.0 - alpha)))
    return output


def _rsi_series(closes: list[float], period: int = 14) -> list[float]:
    if not closes:
        return []
    effective_period = max(1, min(int(period), max(len(closes) - 1, 1)))
    output: list[float] = [50.0] * len(closes)
    if len(closes) == 1:
        return output

    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(closes)):
        delta = float(closes[idx]) - float(closes[idx - 1])
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains[:effective_period]) / effective_period
    avg_loss = sum(losses[:effective_period]) / effective_period

    def _to_rsi(gain: float, loss: float) -> float:
        if loss <= 0:
            return 100.0 if gain > 0 else 50.0
        rs = gain / loss
        return 100.0 - (100.0 / (1.0 + rs))

    first_index = min(effective_period, len(output) - 1)
    output[first_index] = _to_rsi(avg_gain, avg_loss)
    for idx in range(first_index + 1, len(closes)):
        gain = gains[idx - 1]
        loss = losses[idx - 1]
        avg_gain = ((avg_gain * (effective_period - 1)) + gain) / effective_period
        avg_loss = ((avg_loss * (effective_period - 1)) + loss) / effective_period
        output[idx] = _to_rsi(avg_gain, avg_loss)

    for idx in range(first_index):
        output[idx] = output[first_index]
    return output


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


def add_required_indicators(candles: list[Candle]) -> None:
    if not candles:
        return
    candles.sort(key=lambda candle: candle.timestamp)
    add_intraday_vwap(candles)
    closes = [float(c.close) for c in candles]
    ema_9 = _ema_series(closes, 9)
    ema_21 = _ema_series(closes, 21)
    ema_50 = _ema_series(closes, 50)
    ema_200 = _ema_series(closes, 200)
    macd_line = [fast - slow for fast, slow in zip(ema_9, ema_21)]
    macd_signal = _ema_series(macd_line, 9)
    rsi_values = _rsi_series(closes, 14)

    for idx, candle in enumerate(candles):
        candle.ema_9 = round(float(ema_9[idx]), 4)
        candle.ema_21 = round(float(ema_21[idx]), 4)
        candle.ema_50 = round(float(ema_50[idx]), 4)
        candle.ema_200 = round(float(ema_200[idx]), 4)
        candle.rsi = round(float(rsi_values[idx]), 2)
        candle.macd = round(float(macd_line[idx]), 4)
        candle.macd_signal = round(float(macd_signal[idx]), 4)
        candle.macd_hist = round(float(macd_line[idx] - macd_signal[idx]), 4)


def ensure_required_indicator_candles(candles: list[Candle]) -> list[Candle]:
    add_required_indicators(candles)
    for candle in candles:
        required_values = [
            candle.vwap,
            candle.ema_9,
            candle.ema_21,
            candle.ema_50,
            candle.ema_200,
            candle.rsi,
            candle.macd,
            candle.macd_signal,
        ]
        if any(value is None for value in required_values):
            raise ValueError('required indicators missing for strategy candle set')
    return candles


def build_indicator_snapshot(candle: Candle) -> dict[str, float]:
    return {
        'vwap': round(float(candle.vwap), 4),
        'ema_9': round(float(candle.ema_9), 4),
        'ema_21': round(float(candle.ema_21), 4),
        'ema_50': round(float(candle.ema_50), 4),
        'ema_200': round(float(candle.ema_200), 4),
        'rsi': round(float(candle.rsi), 2),
        'macd': round(float(candle.macd), 4),
        'macd_signal': round(float(candle.macd_signal), 4),
        'macd_hist': round(float(candle.macd_hist), 4),
    }


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

    candles = ensure_required_indicator_candles(candles)
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
            bullish_indicators = candle.close > candle.vwap and candle.ema_9 >= candle.ema_21 >= candle.ema_50 and candle.ema_50 >= candle.ema_200 and candle.macd >= candle.macd_signal and candle.rsi >= 55.0
            bearish_indicators = candle.close < candle.vwap and candle.ema_9 <= candle.ema_21 <= candle.ema_50 and candle.ema_50 <= candle.ema_200 and candle.macd <= candle.macd_signal and candle.rsi <= 45.0
            if bias == 'BUY':
                if candle.high <= first_15m.high or not bullish_indicators:
                    continue
                entry = first_15m.high
                stop = candle.low
                target = entry + (entry - stop) * rr_ratio
                side = 'BUY'
                if stop >= entry:
                    continue
            else:
                if candle.low >= first_15m.low or not bearish_indicators:
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
                        **build_indicator_snapshot(candle),
                    },
                )
            )
            break

    return signals
