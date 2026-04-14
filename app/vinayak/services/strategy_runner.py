from __future__ import annotations

from collections.abc import Mapping

from vinayak.domain.models import CandleBatch, StrategyConfig, StrategySignalBatch
from vinayak.observability.prometheus import record_signal_generated
from vinayak.strategies.implementations.breakout import BreakoutStrategy
from vinayak.strategies.implementations.confirmation import ConfirmationStrategy
from vinayak.strategies.implementations.demand_supply import DemandSupplyStrategy
from vinayak.strategies.interfaces.base import TradingStrategy


class StrategyRunnerService:
    def __init__(self, strategy_registry: Mapping[str, TradingStrategy] | None = None) -> None:
        self.strategy_registry = dict(
            strategy_registry
            or {
                'BREAKOUT': BreakoutStrategy(),
                'DEMAND_SUPPLY': DemandSupplyStrategy(),
                'CONFIRMATION': ConfirmationStrategy(),
            }
        )

    def run(self, candles: CandleBatch, config: StrategyConfig) -> StrategySignalBatch:
        strategy = self.strategy_registry[config.strategy_name]
        result = strategy.run(candles, config)
        for signal in result.signals:
            record_signal_generated(signal.strategy_name)
        return result
