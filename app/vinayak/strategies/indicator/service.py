from __future__ import annotations

from dataclasses import dataclass

from vinayak.strategies.breakout.service import Candle, build_indicator_snapshot, ensure_required_indicator_candles
from vinayak.strategies.common.base import StrategySignal


@dataclass
class IndicatorConfig:
    rsi_period: int = 14
    adx_trend_min: float = 20.0
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0


def _market_signal(candle: Candle, config: IndicatorConfig) -> str:
    if candle.close > candle.vwap and candle.ema_9 >= candle.ema_21 >= candle.ema_50 and candle.macd >= candle.macd_signal and candle.rsi >= 58:
        return 'BULLISH_TREND'
    if candle.close < candle.vwap and candle.ema_9 <= candle.ema_21 <= candle.ema_50 and candle.macd <= candle.macd_signal and candle.rsi <= 42:
        return 'BEARISH_TREND'
    if candle.rsi > config.rsi_overbought and candle.macd_hist <= 0:
        return 'OVERBOUGHT'
    if candle.rsi < config.rsi_oversold and candle.macd_hist >= 0:
        return 'OVERSOLD'
    if 45 <= candle.rsi <= 55:
        return 'RANGE'
    return 'NEUTRAL'


def run_indicator_strategy(candles: list[Candle], symbol: str, config: IndicatorConfig | None = None) -> list[StrategySignal]:
    if not candles:
        return []
    cfg = config or IndicatorConfig()
    candles = ensure_required_indicator_candles(candles)

    signals: list[StrategySignal] = []
    for candle in candles:
        market_signal = _market_signal(candle, cfg)
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
                    **build_indicator_snapshot(candle),
                },
            )
        )
    return signals
