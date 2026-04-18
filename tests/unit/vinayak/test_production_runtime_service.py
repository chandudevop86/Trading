from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from decimal import Decimal

import pandas as pd

from vinayak.domain.models import StrategySignalBatch, Timeframe
from vinayak.services.production_runtime import ProductionSignalService


@dataclass
class _StubMarketDataService:
    frame: pd.DataFrame
    calls: int = 0

    def fetch_candles(self, request):
        self.calls += 1
        return type("ProviderResult", (), {"frame": self.frame})()


@dataclass
class _StubStrategyRunner:
    def run(self, candles, config):
        assert candles.symbol == "NIFTY"
        assert candles.timeframe == Timeframe.M5
        assert config.strategy_name == "BREAKOUT"
        return StrategySignalBatch(generated_at=pd.Timestamp("2026-01-01T09:20:00Z").to_pydatetime(), signals=())


def test_production_signal_service_builds_domain_batches() -> None:
    frame = pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-01-01T09:15:00Z"), "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10},
            {"timestamp": pd.Timestamp("2026-01-01T09:20:00Z"), "open": 100.5, "high": 102, "low": 100, "close": 101.5, "volume": 12},
        ]
    )
    service = ProductionSignalService(
        market_data_service=_StubMarketDataService(frame=frame),  # type: ignore[arg-type]
        strategy_runner=_StubStrategyRunner(),  # type: ignore[arg-type]
    )

    candle_batch, signal_batch = service.run_signals(
        symbol="NIFTY",
        timeframe="5m",
        lookback=2,
        strategy="BREAKOUT",
        risk_per_trade_pct=Decimal("1"),
        max_daily_loss_pct=Decimal("3"),
        max_trades_per_day=5,
        cooldown_minutes=15,
    )

    assert candle_batch.symbol == "NIFTY"
    assert candle_batch.timeframe == Timeframe.M5
    assert len(candle_batch.candles) == 2
    assert candle_batch.candles[0].timestamp.tzinfo == UTC
    assert signal_batch.signals == ()


def test_production_signal_service_caches_identical_requests() -> None:
    frame = pd.DataFrame(
        [
            {"timestamp": pd.Timestamp("2026-01-01T09:15:00Z"), "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10},
            {"timestamp": pd.Timestamp("2026-01-01T09:20:00Z"), "open": 100.5, "high": 102, "low": 100, "close": 101.5, "volume": 12},
        ]
    )
    market_data = _StubMarketDataService(frame=frame)
    service = ProductionSignalService(
        market_data_service=market_data,  # type: ignore[arg-type]
        strategy_runner=_StubStrategyRunner(),  # type: ignore[arg-type]
        result_ttl_seconds=30,
    )

    first = service.run_signals(
        symbol="NIFTY",
        timeframe="5m",
        lookback=2,
        strategy="BREAKOUT",
        risk_per_trade_pct=Decimal("1"),
        max_daily_loss_pct=Decimal("3"),
        max_trades_per_day=5,
        cooldown_minutes=15,
    )
    second = service.run_signals(
        symbol="NIFTY",
        timeframe="5m",
        lookback=2,
        strategy="BREAKOUT",
        risk_per_trade_pct=Decimal("1"),
        max_daily_loss_pct=Decimal("3"),
        max_trades_per_day=5,
        cooldown_minutes=15,
    )

    assert market_data.calls == 1
    assert first == second
