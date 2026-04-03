import unittest

from src.breakout_bot import load_candles
from src.price_action import annotate_trades_with_zones


class TestPriceAction(unittest.TestCase):
    def test_adds_supply_demand_and_price_action(self):
        rows = [
            {"timestamp": "2026-03-03 09:15:00", "open": "100", "high": "102", "low": "99", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-03 09:30:00", "open": "101", "high": "104", "low": "100", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-03 09:45:00", "open": "103", "high": "103.5", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-03 10:00:00", "open": "99", "high": "101", "low": "97", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-03 10:15:00", "open": "100", "high": "106", "low": "99", "close": "105.8", "volume": "1000"},
            {"timestamp": "2026-03-03 10:30:00", "open": "105.8", "high": "106.2", "low": "104", "close": "104.3", "volume": "1000"},
            {"timestamp": "2026-03-03 10:45:00", "open": "104.3", "high": "105", "low": "103", "close": "103.5", "volume": "1000"},
        ]
        candles = load_candles(rows)
        trades = [
            {
                "entry_time": "2026-03-03 10:15:00",
                "side": "BUY",
                "entry_price": 102.0,
                "pnl": 500.0,
            }
        ]

        out = annotate_trades_with_zones(trades, candles, pivot_window=1)

        self.assertEqual(len(out), 1)
        self.assertIn(out[0]["price_action"], {"BULLISH", "BULLISH_MARUBOZU", "BEARISH", "BEARISH_MARUBOZU", "INDECISION", "DOJI"})
        self.assertNotEqual(out[0]["demand_zone_low"], "NA")
        self.assertNotEqual(out[0]["supply_zone_high"], "NA")


if __name__ == "__main__":
    unittest.main()
