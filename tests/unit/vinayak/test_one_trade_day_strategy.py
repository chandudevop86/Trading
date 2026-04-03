from datetime import datetime

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.one_trade_day.service import run_one_trade_day_strategy


def test_one_trade_day_returns_standardized_signal() -> None:
    candles = [
        Candle(datetime(2026, 3, 20, 9, 15), 100, 102, 99, 101, 1000),
        Candle(datetime(2026, 3, 20, 9, 20), 101, 103, 100, 102, 1000),
        Candle(datetime(2026, 3, 20, 9, 25), 102, 104, 101, 103, 1000),
        Candle(datetime(2026, 3, 20, 9, 30), 103, 105, 102, 104, 1000),
        Candle(datetime(2026, 3, 20, 9, 35), 104, 106, 103, 105, 1000),
        Candle(datetime(2026, 3, 20, 9, 40), 105, 107, 104, 106, 1000),
        Candle(datetime(2026, 3, 20, 9, 45), 106, 108, 105, 107, 1000),
        Candle(datetime(2026, 3, 20, 9, 50), 107, 109, 106, 108, 1000),
        Candle(datetime(2026, 3, 20, 9, 55), 108, 110, 107, 109, 1000),
        Candle(datetime(2026, 3, 20, 10, 0), 109, 111, 108, 110, 1000),
        Candle(datetime(2026, 3, 20, 10, 5), 110, 112, 109, 111, 1000),
        Candle(datetime(2026, 3, 20, 10, 10), 111, 113, 110, 112, 1000),
        Candle(datetime(2026, 3, 20, 10, 15), 112, 114, 111, 113, 1000),
        Candle(datetime(2026, 3, 20, 10, 20), 113, 115, 112, 114, 1000),
        Candle(datetime(2026, 3, 20, 10, 25), 114, 116, 113, 115, 1000),
    ]

    signals = run_one_trade_day_strategy(
        candles=candles,
        symbol='^NSEI',
        capital=100000,
        risk_pct=0.01,
        rr_ratio=2.0,
    )

    assert signals
    signal = signals[0]
    assert signal.strategy_name == 'One Trade/Day'
    assert signal.side in {'BUY', 'SELL'}
    assert signal.entry_price > 0
    assert signal.stop_loss > 0
    assert signal.target_price > 0
