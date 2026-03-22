import unittest

import pandas as pd

from src.supply_demand import generate_trades


class TestSupplyDemandSignals(unittest.TestCase):
    def test_generate_buy_signal_from_demand_retest(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 99, "high": 100, "low": 95, "close": 96, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1000},
                {"timestamp": "2026-03-20 09:35:00", "open": 100, "high": 101, "low": 98, "close": 99, "volume": 1000},
                {"timestamp": "2026-03-20 09:40:00", "open": 97, "high": 100, "low": 95.2, "close": 99, "volume": 1000},
                {"timestamp": "2026-03-20 09:45:00", "open": 99, "high": 102, "low": 98.5, "close": 101.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:50:00", "open": 101.5, "high": 103, "low": 100.8, "close": 102.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:55:00", "open": 102.5, "high": 104, "low": 101.8, "close": 103.5, "volume": 1000},
                {"timestamp": "2026-03-20 10:00:00", "open": 103.2, "high": 104.2, "low": 99.5, "close": 103.8, "volume": 1000},
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertTrue(rows)
        self.assertEqual(rows[-1]["side"], "BUY")
        self.assertEqual(rows[-1]["signal"], "DEMAND_RETEST")
        self.assertEqual(rows[-1]["zone_fresh"], "YES")
        self.assertEqual(rows[-1]["zone_boundary_mode"], "BASE_BODY")
        self.assertGreater(rows[-1]["zone_low"], 95.0)
        self.assertGreaterEqual(rows[-1]["zone_score"], 3)
        self.assertIn(rows[-1]["higher_tf_bias"], {"BULLISH", "NEUTRAL"})
        self.assertGreater(rows[-1]["target_price"], rows[-1]["entry_price"])
        self.assertLess(rows[-1]["stop_loss"], rows[-1]["entry_price"])

    def test_generate_sell_signal_from_supply_retest(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 102, "high": 104, "low": 101, "close": 103, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 105, "high": 110, "low": 104, "close": 109, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 103, "high": 104, "low": 102, "close": 103, "volume": 1000},
                {"timestamp": "2026-03-20 09:35:00", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 09:40:00", "open": 109, "high": 110, "low": 105, "close": 106, "volume": 1000},
                {"timestamp": "2026-03-20 09:45:00", "open": 106, "high": 106.2, "low": 103, "close": 103.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:50:00", "open": 103.5, "high": 104, "low": 101, "close": 101.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:55:00", "open": 101.5, "high": 102, "low": 99.5, "close": 100.2, "volume": 1000},
                {"timestamp": "2026-03-20 10:00:00", "open": 108.8, "high": 110.2, "low": 104.8, "close": 105.2, "volume": 1000},
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertTrue(rows)
        self.assertEqual(rows[-1]["side"], "SELL")
        self.assertEqual(rows[-1]["signal"], "SUPPLY_RETEST")
        self.assertEqual(rows[-1]["zone_fresh"], "YES")
        self.assertEqual(rows[-1]["zone_boundary_mode"], "BASE_BODY")
        self.assertLess(rows[-1]["zone_high"], 110.0)
        self.assertGreaterEqual(rows[-1]["zone_score"], 3)
        self.assertIn(rows[-1]["higher_tf_bias"], {"BEARISH", "NEUTRAL"})
        self.assertLess(rows[-1]["target_price"], rows[-1]["entry_price"])
        self.assertGreater(rows[-1]["stop_loss"], rows[-1]["entry_price"])

    def test_skips_repeated_touches_of_same_demand_zone(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 99, "high": 100, "low": 95, "close": 96, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1000},
                {"timestamp": "2026-03-20 09:35:00", "open": 97.5, "high": 100.5, "low": 95.4, "close": 99.2, "volume": 1000},
                {"timestamp": "2026-03-20 09:40:00", "open": 98.0, "high": 99.5, "low": 95.3, "close": 98.7, "volume": 1000},
                {"timestamp": "2026-03-20 09:45:00", "open": 97.2, "high": 100.8, "low": 95.1, "close": 99.8, "volume": 1000},
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["side"], "BUY")

    def test_blocks_demand_buy_when_derived_15m_bias_is_bearish(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 112, "high": 113, "low": 111, "close": 112, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 111, "high": 111.5, "low": 109.5, "close": 110, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 110, "high": 110.2, "low": 108, "close": 108.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 108.5, "high": 109, "low": 106.5, "close": 107, "volume": 1000},
                {"timestamp": "2026-03-20 09:35:00", "open": 107, "high": 107.4, "low": 105.2, "close": 105.8, "volume": 1000},
                {"timestamp": "2026-03-20 09:40:00", "open": 105.8, "high": 106, "low": 103.8, "close": 104.3, "volume": 1000},
                {"timestamp": "2026-03-20 09:45:00", "open": 104.3, "high": 104.8, "low": 102.5, "close": 103, "volume": 1000},
                {"timestamp": "2026-03-20 09:50:00", "open": 103, "high": 103.2, "low": 101.4, "close": 101.8, "volume": 1000},
                {"timestamp": "2026-03-20 09:55:00", "open": 101.8, "high": 102.1, "low": 99.8, "close": 100.5, "volume": 1000},
                {"timestamp": "2026-03-20 10:00:00", "open": 101.2, "high": 102, "low": 96.5, "close": 97.5, "volume": 1000},
                {"timestamp": "2026-03-20 10:05:00", "open": 98.2, "high": 99.2, "low": 95.2, "close": 97.8, "volume": 1000},
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertEqual(rows, [])

    def test_blocks_buy_when_next_supply_zone_is_too_close_for_target(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 99, "high": 100, "low": 95, "close": 96, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 1000},
                {"timestamp": "2026-03-20 09:35:00", "open": 104, "high": 105, "low": 103, "close": 104, "volume": 1000},
                {"timestamp": "2026-03-20 09:40:00", "open": 103, "high": 106, "low": 102, "close": 103, "volume": 1000},
                {"timestamp": "2026-03-20 09:45:00", "open": 102, "high": 104, "low": 101, "close": 102.5, "volume": 1000},
                {"timestamp": "2026-03-20 09:50:00", "open": 97, "high": 100, "low": 95.2, "close": 99, "volume": 1000},
                {"timestamp": "2026-03-20 09:55:00", "open": 99, "high": 101.5, "low": 98.7, "close": 101, "volume": 1000},
                {"timestamp": "2026-03-20 10:00:00", "open": 101.2, "high": 103.8, "low": 99.6, "close": 103.2, "volume": 1000},
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
