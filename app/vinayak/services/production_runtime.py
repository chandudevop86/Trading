from __future__ import annotations

"""Application service for production signal orchestration."""

from datetime import UTC, datetime, timedelta
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
        result_ttl_seconds: int = 15,
    ) -> None:
        self.market_data_service = market_data_service
        self.strategy_runner = strategy_runner
        self.result_ttl_seconds = max(int(result_ttl_seconds), 0)
        self._result_cache: dict[tuple[object, ...], tuple[datetime, CandleBatch, StrategySignalBatch]] = {}

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
        cache_key = (
            str(symbol).upper(),
            str(timeframe),
            int(lookback),
            str(strategy).upper(),
            str(risk_per_trade_pct),
            str(max_daily_loss_pct),
            int(max_trades_per_day),
            int(cooldown_minutes),
        )
        cached = self._get_cached_result(cache_key)
        if cached is not None:
            return cached

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
        self._store_cached_result(cache_key, candle_batch, signal_batch)
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

    def _get_cached_result(self, cache_key: tuple[object, ...]) -> tuple[CandleBatch, StrategySignalBatch] | None:
        cached = self._result_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, candle_batch, signal_batch = cached
        if expires_at <= datetime.now(UTC):
            self._result_cache.pop(cache_key, None)
            return None
        return candle_batch, signal_batch

    def _store_cached_result(
        self,
        cache_key: tuple[object, ...],
        candle_batch: CandleBatch,
        signal_batch: StrategySignalBatch,
    ) -> None:
        if self.result_ttl_seconds <= 0:
            return
        self._result_cache[cache_key] = (
            datetime.now(UTC) + timedelta(seconds=self.result_ttl_seconds),
            candle_batch,
            signal_batch,
        )


__all__ = ["ProductionSignalService"]
