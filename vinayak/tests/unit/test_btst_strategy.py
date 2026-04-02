from datetime import datetime, timedelta

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.btst.service import run_btst_strategy


def test_btst_returns_standardized_signal() -> None:
    day_one = datetime(2026, 3, 20, 15, 20)
    day_two = datetime(2026, 3, 21, 9, 15)
    candles = [
        Candle(day_one - timedelta(minutes=10), 100, 101, 99, 100.5, 1000),
        Candle(day_one - timedelta(minutes=5), 100.5, 102, 100, 101.5, 1000),
        Candle(day_one, 101.5, 104, 101, 103.5, 1000),
        Candle(day_two, 104.0, 105, 103.5, 104.5, 1000),
    ]

    signals = run_btst_strategy(
        candles=candles,
        symbol='^NSEI',
        capital=100000,
        risk_pct=0.01,
        rr_ratio=2.0,
    )

    assert signals
    signal = signals[0]
    assert signal.strategy_name == 'BTST'
    assert signal.side in {'BUY', 'SELL'}
    assert signal.entry_price > 0
    assert signal.stop_loss > 0
    assert signal.target_price > 0
