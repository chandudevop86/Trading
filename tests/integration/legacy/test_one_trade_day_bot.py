import unittest
from datetime import datetime, timedelta

from src.breakout_bot import load_candles
from src.indicator_bot import IndicatorConfig
from src.one_trade_day_bot import generate_trades


class TestOneTradeDayBot(unittest.TestCase):
    def test_at_most_one_trade_per_day(self):
        rows = []
        price = 100.0
        t0 = datetime(2026, 3, 10, 9, 15, 0)
        for i in range(70):
            ts = t0 + timedelta(minutes=15 * i)
            rows.append(
                {
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": str(round(price, 2)),
                    "high": str(round(price + 1.2, 2)),
                    "low": str(round(price - 0.8, 2)),
                    "close": str(round(price + 0.6, 2)),
                    "volume": "1200",
                }
            )
            price += 0.4

        candles = load_candles(rows)
        cfg = IndicatorConfig(rsi_overbought=80.0, rsi_oversold=20.0, adx_trend_min=15.0)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=2.0, config=cfg)

        by_day = {}
        for t in trades:
            by_day[t["day"]] = by_day.get(t["day"], 0) + 1
        self.assertTrue(all(v <= 1 for v in by_day.values()))

        if trades:
            self.assertIn(trades[0]["side"], {"BUY", "SELL"})
            self.assertEqual(trades[0]["strategy"], "ONE_TRADE_DAY")


if __name__ == "__main__":
    unittest.main()
