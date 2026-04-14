from __future__ import annotations

from decimal import Decimal

from vinayak.domain.models import CandleBatch, ExecutionSide, StrategyConfig, StrategySignalBatch
from vinayak.strategies.factories.signal_factory import build_entry_signal, build_no_trade_signal


class BreakoutStrategy:
    name = 'BREAKOUT'

    def run(self, candles: CandleBatch, config: StrategyConfig) -> StrategySignalBatch:
        bars = candles.candles
        latest = bars[-1]
        previous = bars[-2] if len(bars) >= 2 else latest
        opening_window = bars[: min(len(bars), 5)]
        breakout_high = max(bar.high for bar in opening_window)
        breakout_low = min(bar.low for bar in opening_window)
        avg_volume = sum(bar.volume for bar in bars[-min(len(bars), 10):]) / Decimal(str(min(len(bars), 10)))
        volume_multiplier = Decimal(str(config.parameters.get('volume_multiplier', '1.20')))
        rr_multiplier = Decimal(str(config.parameters.get('rr_multiplier', '2.0')))
        risk_buffer = Decimal(str(config.parameters.get('risk_buffer_pct', '0.001')))

        signals = []
        if latest.close > breakout_high and latest.volume >= avg_volume * volume_multiplier:
            risk = max(latest.close * risk_buffer, latest.close - previous.low)
            signals.append(
                build_entry_signal(
                    strategy_name=self.name,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    candle=latest,
                    side=ExecutionSide.BUY,
                    entry_price=latest.close,
                    stop_loss=latest.close - risk,
                    target_price=latest.close + (risk * rr_multiplier),
                    quantity=Decimal('1'),
                    confidence=Decimal('0.82'),
                    rationale='Price closed above the breakout range with confirming volume.',
                    metadata={'breakout_high': str(breakout_high), 'avg_volume': str(avg_volume)},
                )
            )
        elif latest.close < breakout_low and latest.volume >= avg_volume * volume_multiplier:
            risk = max(latest.close * risk_buffer, previous.high - latest.close)
            signals.append(
                build_entry_signal(
                    strategy_name=self.name,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    candle=latest,
                    side=ExecutionSide.SELL,
                    entry_price=latest.close,
                    stop_loss=latest.close + risk,
                    target_price=latest.close - (risk * rr_multiplier),
                    quantity=Decimal('1'),
                    confidence=Decimal('0.82'),
                    rationale='Price broke below support with confirming sell volume.',
                    metadata={'breakout_low': str(breakout_low), 'avg_volume': str(avg_volume)},
                )
            )
        else:
            signals.append(
                build_no_trade_signal(
                    strategy_name=self.name,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    candle=latest,
                    rationale='Breakout conditions were not satisfied.',
                )
            )

        return StrategySignalBatch(signals=tuple(signals), generated_at=latest.timestamp)
