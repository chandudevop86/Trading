from datetime import datetime, timedelta

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.mtf.service import run_mtf_strategy


def test_mtf_returns_standardized_signal() -> None:
    start = datetime(2026, 3, 20, 9, 15)
    candles = []
    price = 100.0
    for idx in range(24):
        candles.append(
            Candle(
                start + timedelta(minutes=5 * idx),
                price,
                price + 3,
                price - 1,
                price + 2,
                1000,
            )
        )
        price += 1.2

    signals = run_mtf_strategy(
        candles=candles,
        symbol='^NSEI',
        capital=100000,
        risk_pct=0.01,
        rr_ratio=2.0,
        ema_period=3,
        setup_mode='either',
        require_retest_strength=False,
    )

    if signals:
        signal = signals[0]
        assert signal.strategy_name == 'MTF 5m'
        assert signal.side in {'BUY', 'SELL'}
        assert signal.entry_price > 0
        assert signal.stop_loss > 0
        assert signal.target_price > 0
