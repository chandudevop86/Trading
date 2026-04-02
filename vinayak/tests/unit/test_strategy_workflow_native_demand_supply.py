from datetime import datetime

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def test_strategy_workflow_uses_native_demand_supply_service() -> None:
    candles = [
        Candle(datetime(2026, 3, 20, 9, 15), 100, 103, 98, 101, 1000),
        Candle(datetime(2026, 3, 20, 9, 20), 101, 104, 99, 103, 1000),
        Candle(datetime(2026, 3, 20, 9, 25), 103, 107, 102, 106, 1000),
        Candle(datetime(2026, 3, 20, 9, 30), 106, 108, 104, 105, 1000),
        Candle(datetime(2026, 3, 20, 9, 35), 105, 106, 100, 101, 1000),
        Candle(datetime(2026, 3, 20, 9, 40), 101, 105, 99, 104, 1000),
        Candle(datetime(2026, 3, 20, 9, 45), 104, 109, 103, 108, 1000),
    ]
    rows = [
        {
            'timestamp': candle.timestamp,
            'open': candle.open,
            'high': candle.high,
            'low': candle.low,
            'close': candle.close,
            'volume': candle.volume,
        }
        for candle in candles
    ]
    context = StrategyContext(
        strategy='Demand Supply',
        candles=rows,
        candle_rows=candles,
        capital=100000,
        risk_pct=1.0,
        rr_ratio=2.0,
        trailing_sl_pct=0.5,
        symbol='^NSEI',
    )

    signals = run_strategy_workflow(context)

    assert signals
    assert signals[0]['strategy_name'] == 'DEMAND_SUPPLY'
    assert signals[0]['symbol'] == '^NSEI'
    assert signals[0]['side'] in {'BUY', 'SELL'}
    assert float(signals[0]['entry_price']) > 0
