import unittest

from src.telegram_notifier import build_trade_summary


class TestTelegramNotifier(unittest.TestCase):
    def test_build_trade_summary_with_trades(self):
        trades = [
            {
                "pnl": 150.0,
                "exit_time": "2026-03-06 10:30:00",
                "exit_reason": "TARGET",
            },
            {
                "pnl": -50.0,
                "exit_time": "2026-03-06 11:00:00",
                "exit_reason": "STOP_LOSS",
            },
        ]

        message = build_trade_summary(trades)

        self.assertIn("Intratrade alert", message)
        self.assertIn("Trades: 2", message)
        self.assertIn("Win rate: 50.00%", message)
        self.assertIn("Total PnL: 100.00", message)
        self.assertIn("Last reason: STOP_LOSS", message)

    def test_build_trade_summary_without_trades(self):
        message = build_trade_summary([])
        self.assertEqual(message, "Intratrade: no trades generated for this run.")


if __name__ == "__main__":
    unittest.main()
