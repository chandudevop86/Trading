import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.execution_engine import (
    build_analysis_queue,
    build_execution_candidates,
    default_quantity_for_symbol,
    execute_paper_trades,
    live_trading_unlock_status,
    normalize_order_quantity,
)


class TestExecutionEngine(unittest.TestCase):
    def test_build_indicator_candidate(self):
        rows = [{"timestamp": "2026-03-06 10:00:00", "market_signal": "BULLISH_TREND", "close": 22350.0}]
        c = build_execution_candidates("Indicator (RSI/ADX/MACD+VWAP)", rows, "NIFTY")
        self.assertEqual(len(c), 1)
        self.assertEqual(c[0]["side"], "BUY")
        self.assertEqual(c[0]["quantity"], 65)
        self.assertIn("share_price", c[0])
        self.assertIn("strike_price", c[0])

    def test_default_quantity_for_symbol(self):
        self.assertEqual(default_quantity_for_symbol("NIFTY"), 65)
        self.assertEqual(default_quantity_for_symbol("BANKNIFTY"), 1)

    def test_build_analysis_queue_filters_actionable_candidates(self):
        analyzed = build_analysis_queue(
            [
                {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100},
                {"strategy": "INDICATOR", "symbol": "NIFTY", "signal_time": "2026-03-06 10:05:00", "side": "HOLD", "price": 101},
            ],
            analyzed_at_utc="2026-03-06 10:10:00",
        )
        self.assertEqual(len(analyzed), 1)
        self.assertEqual(analyzed[0]["analysis_status"], "ANALYZED")
        self.assertEqual(analyzed[0]["execution_ready"], "YES")
        self.assertEqual(analyzed[0]["analyzed_at_utc"], "2026-03-06 10:10:00")


    def test_build_execution_candidates_preserves_trade_labels(self):
        rows = [
            {
                "strategy": "MTF_5M",
                "entry_time": "2026-03-05 10:50:00",
                "side": "BUY",
                "entry_price": 107.3,
                "stop_loss": 105.1,
                "trailing_stop_loss": 105.1,
                "target_price": 109.5,
                "quantity": 65,
                "trade_no": 2,
                "trade_label": "Trade 2",
                "option_type": "CE",
                "strike_price": 23450,
            }
        ]

        candidates = build_execution_candidates("MTF 5m", rows, "NIFTY")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["trade_no"], 2)
        self.assertEqual(candidates[0]["trade_label"], "Trade 2")
    def test_normalize_order_quantity_nifty(self):
        self.assertEqual(normalize_order_quantity("NIFTY", 10), 65)
        self.assertEqual(normalize_order_quantity("NIFTY", 129), 65)
        self.assertEqual(normalize_order_quantity("NIFTY", 130), 130)

    def test_execute_paper_trades(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100, "quantity": 10, "reason": "x"},
            {"strategy": "INDICATOR", "symbol": "NIFTY", "signal_time": "2026-03-06 10:05:00", "side": "HOLD", "price": 101, "quantity": 1, "reason": "neutral"},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            done = execute_paper_trades(candidates, out)
            self.assertEqual(len(done), 1)
            self.assertTrue(out.exists())
            self.assertEqual(done[0]["quantity"], 65)
            self.assertIn("share_price", done[0])
            self.assertIn("strike_price", done[0])

    def test_execute_paper_trades_deduplicates(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100, "quantity": 10, "reason": "x"}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            first = execute_paper_trades(candidates, out, deduplicate=True)
            second = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(len(first), 1)
            self.assertEqual(len(second), 0)

    def test_live_trading_unlock_after_30_days(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-01-01 10:00:00", "side": "BUY", "price": 100, "quantity": 65, "reason": "x"}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "dhan_paper.csv"
            execute_paper_trades(candidates, out)

            lines = out.read_text(encoding="utf-8").splitlines()
            header = lines[0]
            row = lines[1].split(",")
            cols = header.split(",")
            idx = cols.index("executed_at_utc")
            row[idx] = "2026-01-01 00:00:00"
            out.write_text("\n".join([header, ",".join(row)]) + "\n", encoding="utf-8")

            unlocked, days, unlock_date = live_trading_unlock_status(
                out,
                min_days=30,
                now_utc=datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC),
            )
            self.assertTrue(unlocked)
            self.assertGreaterEqual(days, 30)
            self.assertIn("2026-01-31", unlock_date)


if __name__ == "__main__":
    unittest.main()



