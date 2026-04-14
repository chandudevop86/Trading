from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from vinayak.backtest.engine import BacktestEngine, BacktestEngineConfig
from vinayak.domain.models import Candle, CandleBatch, ExecutionSide, Timeframe, TradeSignal, TradeSignalType


def test_backtest_engine_reuses_trade_signal_contract() -> None:
    candles = CandleBatch(
        symbol='NIFTY',
        timeframe=Timeframe.M5,
        candles=(
            Candle(
                symbol='NIFTY',
                timeframe=Timeframe.M5,
                timestamp=datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
                open=Decimal('100'),
                high=Decimal('101'),
                low=Decimal('99'),
                close=Decimal('100.5'),
                volume=Decimal('100'),
            ),
        ),
    )
    signal = TradeSignal(
        idempotency_key='signal-key-1234567890',
        strategy_name='BREAKOUT',
        symbol='NIFTY',
        timeframe=Timeframe.M5,
        signal_type=TradeSignalType.ENTRY,
        generated_at=datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
        candle_timestamp=datetime(2026, 1, 1, 9, 15, tzinfo=UTC),
        side=ExecutionSide.BUY,
        entry_price=Decimal('100'),
        stop_loss=Decimal('99'),
        target_price=Decimal('102'),
        quantity=Decimal('1'),
        rationale='backtest',
    )

    report = BacktestEngine(BacktestEngineConfig(slippage_bps=Decimal('0'), brokerage_per_trade=Decimal('0'))).run(
        candles,
        (signal,),
    )

    assert report.trade_count == 1
    assert report.profit_factor >= 1
    assert report.trades[0].signal_id == signal.signal_id
