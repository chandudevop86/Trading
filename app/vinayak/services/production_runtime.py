from __future__ import annotations

"""Application service for production signal orchestration."""

from decimal import Decimal

from vinayak.domain.models import Candle, CandleBatch, RiskConfig, StrategyConfig, StrategySignalBatch, Timeframe
from vinayak.market_data.providers.base import MarketDataRequest
from vinayak.market_data.service import MarketDataService
from vinayak.services.strategy_runner import StrategyRunnerService


class ProductionSignalService:
    def __init__(
        self,
        *,
        market_data_service: MarketDataService,
        strategy_runner: StrategyRunnerService,
    ) -> None:
        self.market_data_service = market_data_service
        self.strategy_runner = strategy_runner

    def run_signals(
        self,
        *,
        symbol: str,
        timeframe: str,
        lookback: int,
        strategy: str,
        risk_per_trade_pct: Decimal,
        max_daily_loss_pct: Decimal,
        max_trades_per_day: int,
        cooldown_minutes: int,
    ) -> tuple[CandleBatch, StrategySignalBatch]:
        provider_result = self.market_data_service.fetch_candles(
            MarketDataRequest(symbol=symbol, timeframe=timeframe, lookback=lookback)
        )
        timeframe_value = self._timeframe(timeframe)
        candles = tuple(
            Candle(
                symbol=symbol,
                timeframe=timeframe_value,
                timestamp=row["timestamp"].to_pydatetime(),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=Decimal(str(row["volume"])),
            )
            for _, row in provider_result.frame.tail(lookback).iterrows()
        )
        candle_batch = CandleBatch(symbol=symbol, timeframe=timeframe_value, candles=candles)
        strategy_config = StrategyConfig(
            strategy_name=strategy,
            symbol=symbol,
            timeframe=timeframe_value,
            risk=RiskConfig(
                risk_per_trade_pct=risk_per_trade_pct,
                max_daily_loss_pct=max_daily_loss_pct,
                max_trades_per_day=max_trades_per_day,
                cooldown_minutes=cooldown_minutes,
            ),
        )
        signal_batch = self.strategy_runner.run(candle_batch, strategy_config)
        return candle_batch, signal_batch

    def _timeframe(self, value: str) -> Timeframe:
        mapping = {
            "1m": Timeframe.M1,
            "5m": Timeframe.M5,
            "15m": Timeframe.M15,
            "30m": Timeframe.M30,
            "1h": Timeframe.H1,
            "1d": Timeframe.D1,
        }
        return mapping[value]


__all__ = ["ProductionSignalService"]
