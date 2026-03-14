import unittest

from src.breakout_bot import generate_trades, load_candles


class TestBreakoutBot(unittest.TestCase):
    def test_long_trade_hits_target(self):
        rows = [
            {"timestamp": "2026-03-01 09:15:00", "open": "100", "high": "102", "low": "99", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-01 09:30:00", "open": "101", "high": "102", "low": "100", "close": "101.5", "volume": "1000"},
            {"timestamp": "2026-03-01 09:45:00", "open": "101.5", "high": "103", "low": "101", "close": "102.5", "volume": "1000"},
            {"timestamp": "2026-03-01 10:00:00", "open": "102.5", "high": "104", "low": "102", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-01 10:15:00", "open": "103", "high": "105", "low": "101", "close": "104", "volume": "1200"},
            {"timestamp": "2026-03-01 10:30:00", "open": "104", "high": "106", "low": "103", "close": "105", "volume": "1200"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade["side"], "BUY")
        self.assertEqual(trade["entry_price"], 102.0)
        self.assertEqual(trade["stop_loss"], 101.0)
        self.assertEqual(trade["target_price"], 104.0)
        self.assertEqual(trade["quantity"], 1000)
        self.assertEqual(trade["exit_reason"], "TARGET")
        self.assertEqual(trade["pnl"], 2000.0)

    def test_short_trade_hits_stop_loss(self):
        rows = [
            {"timestamp": "2026-03-02 09:15:00", "open": "200", "high": "202", "low": "198", "close": "201", "volume": "1000"},
            {"timestamp": "2026-03-02 09:30:00", "open": "201", "high": "201", "low": "199", "close": "200", "volume": "1000"},
            {"timestamp": "2026-03-02 09:45:00", "open": "200", "high": "200", "low": "198", "close": "198", "volume": "1000"},
            {"timestamp": "2026-03-02 10:00:00", "open": "198", "high": "199", "low": "198", "close": "198", "volume": "1000"},
            {"timestamp": "2026-03-02 10:15:00", "open": "198", "high": "199", "low": "194", "close": "195", "volume": "1200"},
            {"timestamp": "2026-03-02 10:30:00", "open": "195", "high": "201", "low": "195", "close": "200", "volume": "1200"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade["side"], "SELL")
        self.assertEqual(trade["entry_price"], 198.0)
        self.assertEqual(trade["stop_loss"], 199.0)
        self.assertEqual(trade["target_price"], 196.0)
        self.assertEqual(trade["quantity"], 1000)
        self.assertEqual(trade["exit_reason"], "STOP_LOSS")
        self.assertEqual(trade["pnl"], -1000.0)


if __name__ == "__main__":
    unittest.main()
