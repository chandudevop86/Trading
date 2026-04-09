import unittest

import pandas as pd

from src.data_processing import load_and_process_ohlcv, normalize_ohlcv_schema


class TestDataProcessing(unittest.TestCase):
    def test_normalizes_aliases_and_removes_duplicates(self):
        rows = [
            {
                "Date": "2026-03-20",
                "Time": "09:15:00",
                "Open": "100",
                "High": "102",
                "Low": "99",
                "adj_close": "101",
                "Vol": "1200",
            },
            {
                "timestamp": "2026-03-20 09:15:00",
                "open": 101,
                "high": 103,
                "low": 100,
                "close": 102,
                "volume": 1300,
            },
        ]

        prepared, report = load_and_process_ohlcv(rows, include_derived=False)

        self.assertEqual(list(prepared.columns), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertEqual(len(prepared), 1)
        self.assertEqual(float(prepared.iloc[0]["open"]), 101.0)
        self.assertEqual(report["duplicates_removed"], 1)

    def test_rejects_invalid_rows_and_tracks_report_counts(self):
        rows = [
            {
                "timestamp": "2026-03-20 09:15:00",
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "volume": 1000,
            },
            {
                "timestamp": "2026-03-20 09:20:00",
                "open": 105,
                "high": 104,
                "low": 100,
                "close": 101,
                "volume": 1000,
            },
            {
                "timestamp": "",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 100,
            },
        ]

        prepared, report = load_and_process_ohlcv(rows, include_derived=False)

        self.assertEqual(len(prepared), 1)
        self.assertEqual(report["invalid_rows_removed"], 1)
        self.assertEqual(report["missing_rows_removed"], 1)
        self.assertEqual(report["rejection_counts"]["invalid_price"], 1)
        self.assertEqual(report["rejection_counts"]["missing_required"], 1)

    def test_adds_intraday_metrics_vwap_and_opening_range(self):
        frame = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1200},
                {"timestamp": "2026-03-20 09:25:00", "open": 102, "high": 104, "low": 101, "close": 103, "volume": 1500},
                {"timestamp": "2026-03-20 09:30:00", "open": 103, "high": 106, "low": 102, "close": 105, "volume": 1800},
            ]
        )

        prepared, report = load_and_process_ohlcv(frame, include_derived=True)

        for column in [
            "range",
            "body_ratio",
            "avg_range_20",
            "avg_volume_20",
            "volume_ratio",
            "vwap",
            "above_vwap",
            "session_date",
            "session_time",
            "time_block",
            "opening_range_high",
            "opening_range_low",
            "opening_range_breakout_up",
            "intraday_high_so_far",
            "intraday_low_so_far",
        ]:
            self.assertIn(column, prepared.columns)

        self.assertAlmostEqual(float(prepared.iloc[0]["opening_range_high"]), 104.0)
        self.assertAlmostEqual(float(prepared.iloc[0]["opening_range_low"]), 99.0)
        self.assertTrue(bool(prepared.iloc[-1]["opening_range_breakout_up"]))
        self.assertGreater(float(prepared.iloc[-1]["vwap"]), 0.0)
        self.assertEqual(report["rows_out"], 4)

    def test_flags_large_interval_gaps(self):
        rows = [
            {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
            {"timestamp": "2026-03-20 09:20:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1100},
            {"timestamp": "2026-03-20 09:40:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1200},
        ]

        prepared, report = load_and_process_ohlcv(rows, include_derived=True)

        self.assertTrue(bool(prepared.iloc[-1]["gap_flag"]))
        self.assertTrue(report["interval_warnings"])

    def test_parses_epoch_timestamps_and_zero_volume_vwap_safely(self):
        rows = [
            {"timestamp": 1710906300, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 0},
            {"timestamp": 1710906600, "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
        ]

        prepared, _ = load_and_process_ohlcv(rows, include_derived=True)

        self.assertEqual(str(prepared.iloc[0]["timestamp"]), "2024-03-20 09:15:00")
        self.assertEqual(float(prepared.iloc[0]["vwap"]), float(prepared.iloc[0]["close"]))
        self.assertFalse(pd.isna(prepared.iloc[1]["avg_range_20"]))
        self.assertFalse(pd.isna(prepared.iloc[1]["avg_volume_20"]))

    def test_missing_csv_path_raises_clean_error(self):
        with self.assertRaises(FileNotFoundError):
            normalize_ohlcv_schema(r"F:\Trading\data\does_not_exist.csv")


if __name__ == "__main__":
    unittest.main()
