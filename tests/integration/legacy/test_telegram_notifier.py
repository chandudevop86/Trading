import unittest

from src.telegram_notifier import build_trade_summary, _encode_multipart_formdata


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



    def test_encode_multipart_formdata_includes_fields_and_file(self):
        body, content_type = _encode_multipart_formdata(
            fields={"chat_id": "123"},
            files={"document": ("report.pdf", b"%PDF-1.4\\n", "application/pdf")},
        )
        self.assertIn("multipart/form-data; boundary=", content_type)
        self.assertIn(b"name=\"chat_id\"", body)
        self.assertIn(b"123", body)
        self.assertIn(b"filename=\"report.pdf\"", body)
        self.assertIn(b"Content-Type: application/pdf", body)
        self.assertIn(b"%PDF-1.4", body)

if __name__ == "__main__":
    unittest.main()
