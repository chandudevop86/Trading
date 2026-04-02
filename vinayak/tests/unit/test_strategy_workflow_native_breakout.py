from datetime import datetime

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def test_strategy_workflow_uses_native_breakout_service() -> None:
    candles = [
        Candle(datetime(2026, 3, 20, 9, 15), 100, 105, 99, 104, 1000),
        Candle(datetime(2026, 3, 20, 9, 20), 104, 106, 103, 105, 1100),
        Candle(datetime(2026, 3, 20, 9, 25), 105, 107, 104, 106, 1200),
        Candle(datetime(2026, 3, 20, 9, 30), 106, 108, 105, 107, 1300),
        Candle(datetime(2026, 3, 20, 9, 35), 107, 110, 106, 109, 1400),
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
        strategy='Breakout',
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
    assert signals[0]['strategy_name'] == 'BREAKOUT'
    assert signals[0]['symbol'] == '^NSEI'
    assert signals[0]['side'] == 'BUY'
    assert float(signals[0]['entry_price']) > 0
