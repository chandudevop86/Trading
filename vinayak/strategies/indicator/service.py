from __future__ import annotations

from dataclasses import dataclass

from vinayak.strategies.breakout.service import Candle, add_intraday_vwap
from vinayak.strategies.common.base import StrategySignal


@dataclass
class IndicatorConfig:
    rsi_period: int = 14
    adx_trend_min: float = 20.0
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0


def _rsi(closes: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= period:
        return out
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(closes)):
        delta = closes[idx] - closes[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    def to_rsi(g: float, l: float) -> float:
        if l == 0:
            return 100.0
        rs = g / l
        return 100.0 - (100.0 / (1.0 + rs))

    out[period] = to_rsi(avg_gain, avg_loss)
    for idx in range(period + 1, len(closes)):
        gain = gains[idx - 1]
        loss = losses[idx - 1]
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        out[idx] = to_rsi(avg_gain, avg_loss)
    return out


def _market_signal(close: float, vwap: float, rsi_val: float | None, config: IndicatorConfig) -> str:
    if rsi_val is None:
        return 'INSUFFICIENT_DATA'
    if close > vwap and rsi_val >= 55:
        return 'BULLISH_TREND'
    if close < vwap and rsi_val <= 45:
        return 'BEARISH_TREND'
    if rsi_val > config.rsi_overbought:
        return 'OVERBOUGHT'
    if rsi_val < config.rsi_oversold:
        return 'OVERSOLD'
    if 45 <= rsi_val <= 55:
        return 'RANGE'
    return 'NEUTRAL'


def run_indicator_strategy(candles: list[Candle], symbol: str, config: IndicatorConfig | None = None) -> list[StrategySignal]:
    if not candles:
        return []
    cfg = config or IndicatorConfig()
    candles = sorted(candles, key=lambda c: c.timestamp)
    add_intraday_vwap(candles)
    closes = [c.close for c in candles]
    rsi_vals = _rsi(closes, cfg.rsi_period)

    signals: list[StrategySignal] = []
    for idx, candle in enumerate(candles):
        market_signal = _market_signal(candle.close, candle.vwap, rsi_vals[idx], cfg)
        side = ''
        if market_signal in {'BULLISH_TREND', 'OVERSOLD'}:
            side = 'BUY'
        elif market_signal in {'BEARISH_TREND', 'OVERBOUGHT'}:
            side = 'SELL'
        else:
            continue

        stop_buffer = max(candle.close * 0.002, 0.1)
        stop_loss = candle.low - stop_buffer if side == 'BUY' else candle.high + stop_buffer
        risk = abs(candle.close - stop_loss)
        if risk <= 0:
            continue
        target = candle.close + (risk * 2.0) if side == 'BUY' else candle.close - (risk * 2.0)
        signals.append(
            StrategySignal(
                strategy_name='Indicator',
                symbol=symbol,
                side=side,
                entry_price=round(candle.close, 4),
                stop_loss=round(stop_loss, 4),
                target_price=round(target, 4),
                signal_time=candle.timestamp,
                metadata={
                    'market_signal': market_signal,
                    'rsi': '' if rsi_vals[idx] is None else round(rsi_vals[idx] or 0.0, 2),
                    'vwap': round(candle.vwap, 4),
                },
            )
        )
    return signals
