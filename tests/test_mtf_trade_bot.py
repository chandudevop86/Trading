import unittest

from src.breakout_bot import load_candles
from src.mtf_trade_bot import generate_trades


class TestMtfTradeBot(unittest.TestCase):
    def _build_rows(self, weak_retest: bool = False) -> list[dict[str, str]]:
        open_1045 = '105.45' if weak_retest else '105.7'
        close_1045 = '105.55' if weak_retest else '106.2'
        return [
            {'timestamp': '2026-03-05 09:15:00', 'open': '100.0', 'high': '100.8', 'low': '99.8', 'close': '100.5', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:20:00', 'open': '100.5', 'high': '101.0', 'low': '100.3', 'close': '100.8', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:25:00', 'open': '100.8', 'high': '101.2', 'low': '100.6', 'close': '101.0', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:30:00', 'open': '101.0', 'high': '101.6', 'low': '100.9', 'close': '101.4', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:35:00', 'open': '101.4', 'high': '101.9', 'low': '101.2', 'close': '101.7', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:40:00', 'open': '101.7', 'high': '102.3', 'low': '101.6', 'close': '102.0', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:45:00', 'open': '102.0', 'high': '102.6', 'low': '101.9', 'close': '102.4', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:50:00', 'open': '102.4', 'high': '103.0', 'low': '102.3', 'close': '102.8', 'volume': '1000'},
            {'timestamp': '2026-03-05 09:55:00', 'open': '102.8', 'high': '103.4', 'low': '102.7', 'close': '103.1', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:00:00', 'open': '103.1', 'high': '103.8', 'low': '103.0', 'close': '103.5', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:05:00', 'open': '103.5', 'high': '104.3', 'low': '103.4', 'close': '104.0', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:10:00', 'open': '104.0', 'high': '104.6', 'low': '103.9', 'close': '104.4', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:15:00', 'open': '104.4', 'high': '104.9', 'low': '104.1', 'close': '104.6', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:20:00', 'open': '104.6', 'high': '105.0', 'low': '104.3', 'close': '104.8', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:25:00', 'open': '104.8', 'high': '105.2', 'low': '104.6', 'close': '105.0', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:30:00', 'open': '105.6', 'high': '106.2', 'low': '105.5', 'close': '106.0', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:35:00', 'open': '106.0', 'high': '106.6', 'low': '105.8', 'close': '106.4', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:40:00', 'open': '106.4', 'high': '106.9', 'low': '106.0', 'close': '106.8', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:45:00', 'open': open_1045, 'high': '106.3', 'low': '105.1', 'close': close_1045, 'volume': '1000'},
            {'timestamp': '2026-03-05 10:50:00', 'open': '105.8', 'high': '107.5', 'low': '105.7', 'close': '107.3', 'volume': '1000'},
            {'timestamp': '2026-03-05 10:55:00', 'open': '107.3', 'high': '108.0', 'low': '107.0', 'close': '107.8', 'volume': '1000'},
        ]

    def _build_multi_trade_rows(self) -> list[dict[str, str]]:
        rows = self._build_rows()
        rows.extend(
            [
                {'timestamp': '2026-03-05 11:00:00', 'open': '107.8', 'high': '108.0', 'low': '107.2', 'close': '107.4', 'volume': '1000'},
                {'timestamp': '2026-03-05 11:05:00', 'open': '107.4', 'high': '108.6', 'low': '107.3', 'close': '108.4', 'volume': '1000'},
                {'timestamp': '2026-03-05 11:10:00', 'open': '108.4', 'high': '109.2', 'low': '108.3', 'close': '109.0', 'volume': '1000'},
                {'timestamp': '2026-03-05 11:15:00', 'open': '108.0', 'high': '108.8', 'low': '107.9', 'close': '108.6', 'volume': '1000'},
                {'timestamp': '2026-03-05 11:20:00', 'open': '108.6', 'high': '109.2', 'low': '108.4', 'close': '109.0', 'volume': '1000'},
                {'timestamp': '2026-03-05 11:25:00', 'open': '109.0', 'high': '109.4', 'low': '108.8', 'close': '109.2', 'volume': '1000'},
            ]
        )
        return rows

    def test_generates_buy_trade_with_configurable_mtf_filters(self):
        candles = load_candles(self._build_rows())
        trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=2.0,
            ema_period=4,
            setup_mode='bos',
            require_retest_strength=True,
        )

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade['side'], 'BUY')
        self.assertEqual(trade['setup_source'], 'BOS')
        self.assertEqual(trade['ema_period'], 4)
        self.assertEqual(trade['setup_mode'], 'bos')
        self.assertTrue(trade['require_retest_strength'])
        self.assertAlmostEqual(trade['target_1'], 105.3)
        self.assertAlmostEqual(trade['target_2'], 105.4)
        self.assertAlmostEqual(trade['target_3'], 105.5)
        self.assertEqual(trade['target_price'], trade['target_2'])
        self.assertIn('gross_pnl', trade)
        self.assertIn('trading_cost', trade)

    def test_weak_retest_is_filtered_unless_disabled(self):
        candles = load_candles(self._build_rows(weak_retest=True))
        strict_trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=2.0,
            require_retest_strength=True,
        )
        loose_trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=2.0,
            require_retest_strength=False,
        )

        self.assertEqual(strict_trades, [])
        self.assertEqual(len(loose_trades), 1)

    def test_generates_multiple_intraday_trades_up_to_limit(self):
        candles = load_candles(self._build_multi_trade_rows())
        one_trade = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=1.0,
            setup_mode='bos',
            max_trades_per_day=1,
        )
        two_trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=1.0,
            setup_mode='bos',
            max_trades_per_day=2,
        )

        self.assertEqual(len(one_trade), 1)
        self.assertEqual(len(two_trades), 2)
        self.assertLess(two_trades[0]['entry_time'], two_trades[1]['entry_time'])
        self.assertEqual(two_trades[0]['max_trades_per_day'], 2)
        self.assertEqual(two_trades[1]['max_trades_per_day'], 2)
        self.assertEqual(two_trades[0]['trade_no'], 1)
        self.assertEqual(two_trades[1]['trade_no'], 2)
        self.assertEqual(two_trades[0]['trade_label'], 'Trade 1')
        self.assertEqual(two_trades[1]['trade_label'], 'Trade 2')

    def test_daily_loss_limit_blocks_follow_up_trades(self):
        candles = load_candles(self._build_multi_trade_rows())
        trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=1.0,
            setup_mode='bos',
            max_trades_per_day=3,
            fixed_cost_per_trade=5000.0,
            max_daily_loss=1000.0,
        )

        self.assertEqual(len(trades), 1)
        self.assertLess(trades[0]['pnl'], 0)


if __name__ == '__main__':
    unittest.main()
