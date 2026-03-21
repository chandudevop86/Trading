import unittest

from src.breakout_bot import load_candles
from src.demand_supply_bot import generate_trades


class TestDemandSupplyBot(unittest.TestCase):
    def test_generates_buy_trade_from_demand_retest(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100", "high": "102", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99", "high": "103", "low": "100", "close": "102", "volume": "1000"},
            {"timestamp": "2026-03-05 10:00:00", "open": "102", "high": "104", "low": "101", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 10:15:00", "open": "103", "high": "104", "low": "100", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-05 10:30:00", "open": "101", "high": "104", "low": "100.5", "close": "103.5", "volume": "1200"},
            {"timestamp": "2026-03-05 10:45:00", "open": "103.5", "high": "106", "low": "103", "close": "105.8", "volume": "1200"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=1.5, pivot_window=1)

        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t["strategy"], "DEMAND_SUPPLY")
        self.assertEqual(t["side"], "BUY")
        self.assertIn(t["exit_reason"], {"TARGET", "EOD", "STOP_LOSS"})


if __name__ == "__main__":
    unittest.main()
