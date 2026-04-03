import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from src.execution_engine import close_paper_trades


class TestPaperTradeCloser(unittest.TestCase):
    def test_close_paper_trade_hits_target(self):
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "executed.csv"
            log_path.write_text(
                "strategy,symbol,signal_time,side,price,quantity,execution_type,execution_status,stop_loss,target_price\n"
                "BREAKOUT,NIFTY,2026-03-06 10:00:00,BUY,100,65,PAPER,EXECUTED,95,110\n",
                encoding="utf-8",
            )

            candles = [
                {"timestamp": "2026-03-06 09:59:00", "open": 99, "high": 100, "low": 98, "close": 99.5},
                {"timestamp": "2026-03-06 10:00:00", "open": 100, "high": 102, "low": 99, "close": 101},
                {"timestamp": "2026-03-06 10:05:00", "open": 101, "high": 111, "low": 100, "close": 110},
            ]

            closed = close_paper_trades(log_path, candles, max_hold_minutes=60)
            self.assertEqual(len(closed), 1)
            self.assertEqual(closed[0]["execution_status"], "CLOSED")
            self.assertEqual(closed[0]["exit_reason"], "TARGET")
            self.assertEqual(float(closed[0]["exit_price"]), 110.0)

            # file updated
            text = log_path.read_text(encoding="utf-8")
            self.assertIn("exit_reason", text)
            self.assertIn("TARGET", text)


if __name__ == "__main__":
    unittest.main()