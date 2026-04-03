from datetime import datetime, timedelta

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def test_strategy_workflow_uses_native_btst_service() -> None:
    day_one = datetime(2026, 3, 20, 15, 20)
    day_two = datetime(2026, 3, 21, 9, 15)
    candles = [
        Candle(day_one - timedelta(minutes=10), 100, 101, 99, 100.5, 1000),
        Candle(day_one - timedelta(minutes=5), 100.5, 102, 100, 101.5, 1000),
        Candle(day_one, 101.5, 104, 101, 103.5, 1000),
        Candle(day_two, 104.0, 105, 103.5, 104.5, 1000),
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
        strategy='BTST',
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
    assert signals[0]['strategy_name'] == 'BTST'
    assert signals[0]['symbol'] == '^NSEI'
    assert signals[0]['side'] in {'BUY', 'SELL'}
    assert float(signals[0]['entry_price']) > 0
