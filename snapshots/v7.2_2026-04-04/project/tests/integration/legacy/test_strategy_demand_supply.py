import unittest

import pandas as pd

from src.strategy_demand_supply import DemandSupplyConfig, generate_trades, is_retest, is_session_valid, is_vwap_valid, rejection_candle


class TestStrategyDemandSupply(unittest.TestCase):
    def test_session_filter_allows_only_morning_window(self):
        self.assertTrue(is_session_valid('2026-03-27 09:25:00'))
        self.assertFalse(is_session_valid('2026-03-27 12:15:00'))
        self.assertFalse(is_session_valid('2026-03-27 13:50:00'))

    def test_vwap_filter_is_directional(self):
        self.assertTrue(is_vwap_valid(101.0, 100.0, 'BUY'))
        self.assertTrue(is_vwap_valid(99.0, 100.0, 'SELL'))
        self.assertFalse(is_vwap_valid(99.0, 100.0, 'BUY'))
        self.assertFalse(is_vwap_valid(101.0, 100.0, 'SELL'))

    def test_retest_requires_leave_and_return(self):
        frame = pd.DataFrame(
            [
                {'timestamp': '2026-03-27 09:20:00', 'open': 102.0, 'high': 102.2, 'low': 101.8, 'close': 102.1},
                {'timestamp': '2026-03-27 09:25:00', 'open': 103.0, 'high': 103.4, 'low': 103.0, 'close': 103.2},
                {'timestamp': '2026-03-27 09:30:00', 'open': 102.4, 'high': 102.8, 'low': 101.9, 'close': 102.2},
            ]
        )
        self.assertTrue(is_retest(frame, 2, 101.9, 102.3, lookback=2))
        self.assertFalse(is_retest(frame, 1, 101.9, 102.3, lookback=2))

    def test_rejection_candle_checks_direction(self):
        buy_row = {'open': 100.4, 'high': 101.0, 'low': 99.6, 'close': 100.9}
        sell_row = {'open': 100.8, 'high': 101.4, 'low': 100.2, 'close': 100.3}
        self.assertTrue(rejection_candle(buy_row, 'BUY', DemandSupplyConfig()))
        self.assertTrue(rejection_candle(sell_row, 'SELL', DemandSupplyConfig()))

    def test_generate_trades_accepts_dict_config_threshold_override(self):
        rows = [
            {'timestamp': '2026-03-05 09:15:00', 'open': 100.8, 'high': 101.0, 'low': 100.4, 'close': 100.6, 'volume': 1000},
            {'timestamp': '2026-03-05 09:20:00', 'open': 100.6, 'high': 100.8, 'low': 98.9, 'close': 99.3, 'volume': 1500},
            {'timestamp': '2026-03-05 09:25:00', 'open': 99.3, 'high': 100.4, 'low': 99.1, 'close': 100.2, 'volume': 1400},
            {'timestamp': '2026-03-05 09:30:00', 'open': 100.2, 'high': 101.2, 'low': 100.0, 'close': 101.0, 'volume': 1400},
            {'timestamp': '2026-03-05 09:35:00', 'open': 101.0, 'high': 101.8, 'low': 100.8, 'close': 101.5, 'volume': 1450},
            {'timestamp': '2026-03-05 09:40:00', 'open': 101.5, 'high': 101.6, 'low': 99.2, 'close': 100.7, 'volume': 1700},
            {'timestamp': '2026-03-05 09:45:00', 'open': 100.7, 'high': 102.3, 'low': 100.5, 'close': 102.2, 'volume': 1800},
            {'timestamp': '2026-03-05 09:50:00', 'open': 101.7, 'high': 103.4, 'low': 101.6, 'close': 103.1, 'volume': 1900},
            {'timestamp': '2026-03-05 09:55:00', 'open': 103.1, 'high': 103.8, 'low': 102.8, 'close': 103.6, 'volume': 1200},
        ]
        trades = generate_trades(rows, capital=100000, risk_pct=0.01, rr_ratio=2.0, config={'score_threshold': 6.8, 'pivot_window': 1})
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]['strategy'], 'DEMAND_SUPPLY')


if __name__ == '__main__':
    unittest.main()

