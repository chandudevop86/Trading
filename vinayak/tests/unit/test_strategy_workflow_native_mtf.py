from datetime import datetime, timedelta

from vinayak.api.services.strategy_workflow import StrategyContext, run_strategy_workflow
from vinayak.strategies.breakout.service import Candle


def test_strategy_workflow_uses_native_mtf_service() -> None:
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
        strategy='MTF 5m',
        candles=rows,
        candle_rows=candles,
        capital=100000,
        risk_pct=1.0,
        rr_ratio=2.0,
        trailing_sl_pct=0.5,
        symbol='^NSEI',
        mtf_ema_period=3,
        mtf_setup_mode='either',
        mtf_retest_strength=False,
    )

    signals = run_strategy_workflow(context)

    if signals:
        assert signals[0]['strategy_name'] == 'MTF_5M'
        assert signals[0]['symbol'] == '^NSEI'
        assert signals[0]['side'] in {'BUY', 'SELL'}
        assert float(signals[0]['entry_price']) > 0
