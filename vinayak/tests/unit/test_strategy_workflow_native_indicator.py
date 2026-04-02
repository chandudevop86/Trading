from datetime import datetime

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def test_strategy_workflow_uses_native_indicator_service() -> None:
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
        strategy='Indicator',
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
    assert signals[0]['strategy_name'] == 'INDICATOR'
    assert signals[0]['symbol'] == '^NSEI'
    assert signals[0]['side'] in {'BUY', 'SELL'}
    assert float(signals[0]['entry_price']) > 0
