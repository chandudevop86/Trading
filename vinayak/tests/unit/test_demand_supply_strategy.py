from datetime import datetime

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.demand_supply.service import run_demand_supply_strategy


def test_demand_supply_returns_standardized_signal() -> None:
    candles = [
        Candle(datetime(2026, 3, 20, 9, 15), 100, 103, 98, 101, 1000),
        Candle(datetime(2026, 3, 20, 9, 20), 101, 104, 99, 103, 1000),
        Candle(datetime(2026, 3, 20, 9, 25), 103, 107, 102, 106, 1000),
        Candle(datetime(2026, 3, 20, 9, 30), 106, 108, 104, 105, 1000),
        Candle(datetime(2026, 3, 20, 9, 35), 105, 106, 100, 101, 1000),
        Candle(datetime(2026, 3, 20, 9, 40), 101, 105, 99, 104, 1000),
        Candle(datetime(2026, 3, 20, 9, 45), 104, 109, 103, 108, 1000),
    ]

    signals = run_demand_supply_strategy(
        candles=candles,
        symbol='^NSEI',
        capital=100000,
        risk_pct=0.01,
        rr_ratio=2.0,
    )

    assert signals, 'Expected at least one demand-supply signal'
    signal = signals[0]
    assert signal.strategy_name == 'Demand Supply'
    assert signal.symbol == '^NSEI'
    assert signal.side in {'BUY', 'SELL'}
    assert signal.entry_price > 0
    assert signal.stop_loss > 0
    assert signal.target_price > 0
