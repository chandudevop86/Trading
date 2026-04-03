import unittest
from datetime import datetime, UTC

from src.aws_storage import build_s3_key


class TestAwsStorage(unittest.TestCase):
    def test_build_s3_key_with_prefix(self):
        key = build_s3_key("intratrade/exports", "trades.csv", now=datetime(2026, 3, 6, 10, 30, 0, tzinfo=UTC))
        self.assertEqual(key, "intratrade/exports/20260306_103000_trades.csv")

    def test_build_s3_key_without_prefix(self):
        key = build_s3_key("", "trades.csv", now=datetime(2026, 3, 6, 10, 30, 0, tzinfo=UTC))
        self.assertEqual(key, "20260306_103000_trades.csv")


if __name__ == "__main__":
    unittest.main()
