import unittest

import pandas as pd

from src.preprocessing import prepare_trading_data


class TestPrepareTradingData(unittest.TestCase):
    def test_normalizes_mixed_case_ohlcv_columns(self):
        frame = pd.DataFrame(
            [
                {
                    "Timestamp": "2026-03-20 09:15:00",
                    "Open": "100",
                    "HIGH": "102",
                    "Low": "99",
                    "Close": "101",
                    "Volume": "1200",
                }
            ]
        )

        prepared = prepare_trading_data(frame)

        self.assertEqual(list(prepared.columns), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertEqual(prepared.iloc[0]["open"], 100.0)
        self.assertEqual(prepared.iloc[0]["volume"], 1200.0)

    def test_uses_datetime_alias_as_timestamp(self):
        prepared = prepare_trading_data(
            [
                {
                    "datetime": "2026-03-20 09:15:00",
                    "open": 100,
                    "high": 102,
                    "low": 99,
                    "close": 101,
                    "volume": 1000,
                }
            ]
        )

        self.assertEqual(str(prepared.iloc[0]["timestamp"]), "2026-03-20 09:15:00")

    def test_combines_date_and_time_columns_into_timestamp(self):
        frame = pd.DataFrame(
            [
                {
                    "Date": "2026-03-20",
                    "Time": "09:15:00",
                    "Open": 100,
                    "High": 102,
                    "Low": 99,
                    "Close": 101,
                    "Volume": 1000,
                }
            ]
        )

        prepared = prepare_trading_data(frame)

        self.assertEqual(str(prepared.iloc[0]["timestamp"]), "2026-03-20 09:15:00")

    def test_keeps_latest_duplicate_timestamp_after_normalization(self):
        prepared = prepare_trading_data(
            [
                {
                    "date": "2026-03-20",
                    "time": "09:15:00",
                    "open": 100,
                    "high": 102,
                    "low": 99,
                    "close": 101,
                    "volume": 1000,
                },
                {
                    "timestamp": "2026-03-20 09:15:00",
                    "open": 101,
                    "high": 103,
                    "low": 100,
                    "close": 102,
                    "volume": 1100,
                },
            ]
        )

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared.iloc[0]["open"], 101.0)


if __name__ == "__main__":
    unittest.main()
