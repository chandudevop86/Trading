from datetime import datetime

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def _sample_candles() -> list[Candle]:
    return [
        Candle(datetime(2026, 4, 1, 9, 15), 110.0, 110.5, 108.8, 109.0, 1000),
        Candle(datetime(2026, 4, 1, 9, 20), 109.0, 109.2, 108.5, 108.6, 1200),
        Candle(datetime(2026, 4, 1, 9, 25), 108.6, 108.9, 108.3, 108.5, 900),
        Candle(datetime(2026, 4, 1, 9, 30), 108.5, 108.8, 108.2, 108.45, 850),
        Candle(datetime(2026, 4, 1, 9, 35), 108.45, 111.2, 108.4, 110.9, 2200),
        Candle(datetime(2026, 4, 1, 9, 40), 110.9, 111.4, 109.7, 110.1, 1500),
        Candle(datetime(2026, 4, 1, 9, 45), 110.1, 111.8, 109.9, 111.5, 1900),
    ]


def test_strategy_workflow_uses_native_demand_supply_service() -> None:
    candles = _sample_candles()
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
    assert signals[0]['side'] == 'BUY'
    assert float(signals[0]['entry_price']) > 0
    assert float(signals[0]['validation_score']) > 0

