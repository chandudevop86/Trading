from __future__ import annotations

from typing import Protocol

from vinayak.domain.models import CandleBatch, StrategyConfig, StrategySignalBatch


class TradingStrategy(Protocol):
    def run(self, candles: CandleBatch, config: StrategyConfig) -> StrategySignalBatch:
        """Generate only validated TradeSignal objects."""
