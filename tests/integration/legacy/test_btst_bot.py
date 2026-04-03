import unittest

from src.breakout_bot import load_candles
from src.btst_bot import generate_trades


class TestBtstBot(unittest.TestCase):
    def test_btst_generates_next_day_open_exit(self):
        rows = [
            {"timestamp": "2026-03-05 15:25:00", "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "1000"},
            {"timestamp": "2026-03-05 15:30:00", "open": "100.5", "high": "102", "low": "100", "close": "101.8", "volume": "1500"},
            {"timestamp": "2026-03-06 09:15:00", "open": "102.5", "high": "103", "low": "101.8", "close": "102.7", "volume": "1100"},
            {"timestamp": "2026-03-06 09:20:00", "open": "102.7", "high": "103", "low": "102", "close": "102.2", "volume": "1200"},
        ]
        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000.0, risk_pct=0.01, allow_stbt=True)

        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t["strategy"], "BTST")
        self.assertEqual(t["exit_reason"], "NEXT_DAY_OPEN")
        self.assertEqual(t["side"], "BUY")
        self.assertGreater(float(t["quantity"]), 0)


if __name__ == "__main__":
    unittest.main()