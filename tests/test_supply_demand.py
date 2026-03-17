import unittest

import pandas as pd

from src.breakout_bot import load_candles
from src.supply_demand import generate_trades


class TestSupplyDemand(unittest.TestCase):
    def test_detects_bullish_and_bearish_fvg_zones(self):
        df = pd.DataFrame(
            [
                {"timestamp": "2026-03-05 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-05 09:20:00", "open": 101, "high": 104, "low": 100, "close": 103, "volume": 1000},
                {"timestamp": "2026-03-05 09:25:00", "open": 106, "high": 108, "low": 106, "close": 107, "volume": 1000},
                {"timestamp": "2026-03-05 09:30:00", "open": 104, "high": 105, "low": 103, "close": 104, "volume": 1000},
                {"timestamp": "2026-03-05 09:35:00", "open": 103, "high": 104, "low": 102, "close": 103, "volume": 1000},
            ]
        )

        zones = generate_trades(df)

        bullish_fvg = [z for z in zones if z["type"] == "demand" and z.get("source") == "fvg"]
        bearish_fvg = [z for z in zones if z["type"] == "supply" and z.get("source") == "fvg"]

        self.assertTrue(any(z["zone_low"] == 101.0 and z["zone_high"] == 106.0 for z in bullish_fvg))
        self.assertTrue(any(z["zone_low"] == 104.0 and z["zone_high"] == 106.0 for z in bearish_fvg))

    def test_detects_bullish_and_bearish_bos_zones(self):
        df = pd.DataFrame(
            [
                {"timestamp": "2026-03-05 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-05 09:20:00", "open": 100, "high": 102, "low": 99.5, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-05 09:25:00", "open": 101, "high": 104, "low": 100.5, "close": 103.5, "volume": 1000},
                {"timestamp": "2026-03-05 09:30:00", "open": 103.5, "high": 103.8, "low": 98, "close": 99, "volume": 1000},
                {"timestamp": "2026-03-05 09:35:00", "open": 99, "high": 100, "low": 97.5, "close": 98.5, "volume": 1000},
                {"timestamp": "2026-03-05 09:40:00", "open": 98.5, "high": 105.5, "low": 98.2, "close": 105, "volume": 1000},
                {"timestamp": "2026-03-05 09:45:00", "open": 105, "high": 105.2, "low": 96.2, "close": 96.5, "volume": 1000},
            ]
        )

        zones = generate_trades(df)

        bullish_bos = [z for z in zones if z["type"] == "demand" and z.get("source") == "bos"]
        bearish_bos = [z for z in zones if z["type"] == "supply" and z.get("source") == "bos"]

        self.assertTrue(any(z["index"] == 5 and z["zone_low"] == 98.2 and z["zone_high"] == 105.0 and z["structure_level"] == 104.0 for z in bullish_bos))
        self.assertTrue(any(z["index"] == 6 and z["zone_low"] == 96.5 and z["zone_high"] == 105.2 and z["structure_level"] == 97.5 for z in bearish_bos))

    def test_accepts_candle_objects_from_loader(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:20:00", "open": "101", "high": "104", "low": "100", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 09:25:00", "open": "106", "high": "108", "low": "106", "close": "107", "volume": "1000"},
        ]

        candles = load_candles(rows)
        zones = generate_trades(candles)

        self.assertGreaterEqual(len(zones), 1)
        self.assertEqual(zones[0]["source"], "fvg")


if __name__ == "__main__":
    unittest.main()
