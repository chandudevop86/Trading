from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from vinayak.domain.models import Candle, CandleBatch, RiskConfig, StrategyConfig, Timeframe, TradeSignal
from vinayak.strategies.implementations.breakout import BreakoutStrategy
from vinayak.strategies.implementations.confirmation import ConfirmationStrategy
from vinayak.strategies.implementations.demand_supply import DemandSupplyStrategy


def _batch() -> CandleBatch:
    start = datetime(2026, 4, 14, 9, 15, tzinfo=UTC)
    candles = []
    for index in range(12):
        candles.append(
            Candle(
                symbol='NIFTY',
                timeframe=Timeframe.M5,
                timestamp=start + timedelta(minutes=index * 5),
                open=Decimal('100') + Decimal(index),
                high=Decimal('102') + Decimal(index),
                low=Decimal('99') + Decimal(index),
                close=Decimal('101') + Decimal(index),
                volume=Decimal('1000') + Decimal(index * 100),
                vwap=Decimal('100.5') + Decimal(index),
            )
        )
    return CandleBatch(symbol='NIFTY', timeframe=Timeframe.M5, candles=tuple(candles))


def _config(name: str) -> StrategyConfig:
    return StrategyConfig(
        strategy_name=name,
        symbol='NIFTY',
        timeframe=Timeframe.M5,
        risk=RiskConfig(
            risk_per_trade_pct=Decimal('1'),
            max_daily_loss_pct=Decimal('3'),
            max_trades_per_day=5,
            cooldown_minutes=15,
        ),
    )


def test_strategies_only_emit_trade_signal_models() -> None:
    batch = _batch()
    for strategy, name in (
        (BreakoutStrategy(), 'BREAKOUT'),
        (DemandSupplyStrategy(), 'DEMAND_SUPPLY'),
        (ConfirmationStrategy(), 'CONFIRMATION'),
    ):
        result = strategy.run(batch, _config(name))
        assert all(isinstance(signal, TradeSignal) for signal in result.signals)
