from __future__ import annotations

from decimal import Decimal

from vinayak.domain.models import CandleBatch, ExecutionSide, StrategyConfig, StrategySignalBatch
from vinayak.strategies.factories.signal_factory import build_entry_signal, build_no_trade_signal


class DemandSupplyStrategy:
    name = 'DEMAND_SUPPLY'

    def run(self, candles: CandleBatch, config: StrategyConfig) -> StrategySignalBatch:
        bars = candles.candles
        latest = bars[-1]
        recent = bars[-min(len(bars), 20):]
        zone_low = min(bar.low for bar in recent)
        zone_high = max(bar.high for bar in recent)
        zone_mid = (zone_low + zone_high) / Decimal('2')
        rr_multiplier = Decimal(str(config.parameters.get('rr_multiplier', '2.5')))

        if latest.low <= zone_low * Decimal('1.002') and latest.close > zone_mid:
            risk = max(latest.close - zone_low, Decimal('0.05'))
            signal = build_entry_signal(
                strategy_name=self.name,
                symbol=config.symbol,
                timeframe=config.timeframe,
                candle=latest,
                side=ExecutionSide.BUY,
                entry_price=latest.close,
                stop_loss=zone_low,
                target_price=latest.close + (risk * rr_multiplier),
                quantity=Decimal('1'),
                confidence=Decimal('0.78'),
                rationale='Retest of demand zone followed by bullish rejection close.',
                metadata={'zone_low': str(zone_low), 'zone_high': str(zone_high)},
            )
            return StrategySignalBatch(signals=(signal,), generated_at=latest.timestamp)

        if latest.high >= zone_high * Decimal('0.998') and latest.close < zone_mid:
            risk = max(zone_high - latest.close, Decimal('0.05'))
            signal = build_entry_signal(
                strategy_name=self.name,
                symbol=config.symbol,
                timeframe=config.timeframe,
                candle=latest,
                side=ExecutionSide.SELL,
                entry_price=latest.close,
                stop_loss=zone_high,
                target_price=latest.close - (risk * rr_multiplier),
                quantity=Decimal('1'),
                confidence=Decimal('0.78'),
                rationale='Supply zone rejection confirmed by a weak closing candle.',
                metadata={'zone_low': str(zone_low), 'zone_high': str(zone_high)},
            )
            return StrategySignalBatch(signals=(signal,), generated_at=latest.timestamp)

        return StrategySignalBatch(
            signals=(
                build_no_trade_signal(
                    strategy_name=self.name,
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    candle=latest,
                    rationale='No valid demand/supply rejection was detected.',
                ),
            ),
            generated_at=latest.timestamp,
        )
