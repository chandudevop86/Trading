import unittest

import pandas as pd

from src.market_data_service import dataframe_to_candles, period_for_interval


class TestMarketDataService(unittest.TestCase):
    def test_period_for_interval_uses_legacy_mapping(self):
        self.assertEqual(period_for_interval('5m', '5d'), '60d')
        self.assertEqual(period_for_interval('unknown', '5d'), '5d')

    def test_dataframe_to_candles_converts_rows(self):
        frame = pd.DataFrame([
            {'timestamp': '2026-03-01 09:15:00', 'open': 100, 'high': 102, 'low': 99, 'close': 101, 'volume': 1000}
        ])
        candles = dataframe_to_candles(frame)
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].close, 101.0)


if __name__ == '__main__':
    unittest.main()
