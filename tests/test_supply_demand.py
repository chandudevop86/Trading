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
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertTrue(rows)
        self.assertEqual(rows[-1]["side"], "BUY")
        self.assertEqual(rows[-1]["signal"], "DEMAND_RETEST")
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
            ]
        )

        rows = generate_trades(candles, include_fvg=False, include_bos=False, capital=100000, risk_pct=0.01, rr_ratio=2.0)

        self.assertTrue(rows)
        self.assertEqual(rows[-1]["side"], "SELL")
        self.assertEqual(rows[-1]["signal"], "SUPPLY_RETEST")
        self.assertLess(rows[-1]["target_price"], rows[-1]["entry_price"])
        self.assertGreater(rows[-1]["stop_loss"], rows[-1]["entry_price"])


if __name__ == "__main__":
    unittest.main()
