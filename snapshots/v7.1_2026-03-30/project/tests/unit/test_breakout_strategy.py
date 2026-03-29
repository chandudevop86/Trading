from datetime import datetime

from vinayak.strategies.breakout.service import Candle, run_breakout_strategy


def test_breakout_strategy_returns_standardized_signal() -> None:
    candles = [
        Candle(datetime(2026, 3, 20, 9, 15), 100, 105, 99, 104, 1000),
        Candle(datetime(2026, 3, 20, 9, 20), 104, 106, 103, 105, 1100),
        Candle(datetime(2026, 3, 20, 9, 25), 105, 107, 104, 106, 1200),
        Candle(datetime(2026, 3, 20, 9, 30), 106, 108, 105, 107, 1300),
        Candle(datetime(2026, 3, 20, 9, 35), 107, 110, 106, 109, 1400),
    ]

    signals = run_breakout_strategy(candles, symbol='^NSEI', capital=100000, risk_pct=0.01, rr_ratio=2.0)

    assert len(signals) == 1
    signal = signals[0]
    assert signal.strategy_name == 'Breakout'
    assert signal.symbol == '^NSEI'
    assert signal.side == 'BUY'
    assert signal.entry_price > 0
    assert signal.stop_loss > 0
    assert signal.target_price > signal.entry_price
