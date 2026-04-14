from __future__ import annotations

from decimal import Decimal

from vinayak.domain.models import CandleBatch, ExecutionSide, StrategyConfig, StrategySignalBatch
from vinayak.strategies.factories.signal_factory import build_entry_signal, build_no_trade_signal


def _ema(values: list[Decimal], period: int) -> Decimal:
    if not values:
        return Decimal('0')
    multiplier = Decimal('2') / Decimal(str(period + 1))
    result = values[0]
    for value in values[1:]:
        result = (value - result) * multiplier + result
    return result


class ConfirmationStrategy:
    name = 'CONFIRMATION'

    def run(self, candles: CandleBatch, config: StrategyConfig) -> StrategySignalBatch:
        bars = candles.candles
        latest = bars[-1]
        closes = [bar.close for bar in bars[-min(len(bars), 30):]]
        highs = [bar.high for bar in bars[-min(len(bars), 14):]]
        lows = [bar.low for bar in bars[-min(len(bars), 14):]]
        ema_fast = _ema(closes[-9:], 9)
        ema_slow = _ema(closes[-21:], 21)
        adx_proxy = (max(highs) - min(lows)) / max(latest.close, Decimal('1'))
        vwap_bias = latest.vwap is not None and latest.close > latest.vwap
        rr_multiplier = Decimal(str(config.parameters.get('rr_multiplier', '2.0')))

        if ema_fast > ema_slow and adx_proxy >= Decimal('0.01') and vwap_bias:
            risk = max(latest.close - min(lows[-5:]), Decimal('0.05'))
            signal = build_entry_signal(
                strategy_name=self.name,
                symbol=config.symbol,
                timeframe=config.timeframe,
                candle=latest,
                side=ExecutionSide.BUY,
                entry_price=latest.close,
                stop_loss=latest.close - risk,
                target_price=latest.close + (risk * rr_multiplier),
                quantity=Decimal('1'),
                confidence=Decimal('0.76'),
                rationale='EMA, ADX proxy, and VWAP alignment confirm a bullish setup.',
                metadata={'ema_fast': str(ema_fast), 'ema_slow': str(ema_slow), 'adx_proxy': str(adx_proxy)},
            )
            return StrategySignalBatch(signals=(signal,), generated_at=latest.timestamp)

        if ema_fast < ema_slow and adx_proxy >= Decimal('0.01') and latest.vwap is not None and latest.close < latest.vwap:
            risk = max(max(highs[-5:]) - latest.close, Decimal('0.05'))
            signal = build_entry_signal(
                strategy_name=self.name,
                symbol=config.symbol,
                timeframe=config.timeframe,
                candle=latest,
                side=ExecutionSide.SELL,
                entry_price=latest.close,
                stop_loss=latest.close + risk,
                target_price=latest.close - (risk * rr_multiplier),
                quantity=Decimal('1'),
                confidence=Decimal('0.76'),
                rationale='EMA, ADX proxy, and VWAP alignment confirm a bearish setup.',
                metadata={'ema_fast': str(ema_fast), 'ema_slow': str(ema_slow), 'adx_proxy': str(adx_proxy)},
            )
            return StrategySignalBatch(signals=(signal,), generated_at=latest.timestamp)

        return StrategySignalBatch(
            signals=(
                build_no_trade_signal(
                    strategy_name=self.name,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    candle=latest,
                    rationale='RSI/ADX/MACD/VWAP confirmation conditions were not aligned.',
                ),
            ),
            generated_at=latest.timestamp,
        )
