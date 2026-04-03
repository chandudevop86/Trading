import unittest

from src.breakout_bot import generate_trades, load_candles


class TestBreakoutBot(unittest.TestCase):
    def _strong_long_rows(self):
        return [
            {'timestamp': '2026-03-01 09:15:00', 'open': '100', 'high': '102', 'low': '99', 'close': '101', 'volume': '1000'},
            {'timestamp': '2026-03-01 09:30:00', 'open': '101', 'high': '103', 'low': '100.5', 'close': '102.5', 'volume': '1100'},
            {'timestamp': '2026-03-01 09:45:00', 'open': '102.5', 'high': '104', 'low': '102', 'close': '103.5', 'volume': '1200'},
            {'timestamp': '2026-03-01 10:00:00', 'open': '103.5', 'high': '105', 'low': '103', 'close': '104.5', 'volume': '1300'},
            {'timestamp': '2026-03-01 10:15:00', 'open': '104.5', 'high': '107', 'low': '104', 'close': '106.8', 'volume': '2200'},
            {'timestamp': '2026-03-01 10:30:00', 'open': '106.8', 'high': '107.2', 'low': '105.2', 'close': '106.2', 'volume': '1800'},
            {'timestamp': '2026-03-01 10:45:00', 'open': '106.2', 'high': '108', 'low': '105.6', 'close': '107.5', 'volume': '2000'},
        ]

    def _counter_trend_short_rows(self):
        return [
            {'timestamp': '2026-03-02 09:15:00', 'open': '200', 'high': '202', 'low': '199', 'close': '201', 'volume': '1000'},
            {'timestamp': '2026-03-02 09:30:00', 'open': '201', 'high': '203', 'low': '200.5', 'close': '202', 'volume': '1100'},
            {'timestamp': '2026-03-02 09:45:00', 'open': '202', 'high': '203.5', 'low': '201.5', 'close': '203', 'volume': '1200'},
            {'timestamp': '2026-03-02 10:00:00', 'open': '203', 'high': '204', 'low': '202.5', 'close': '203.5', 'volume': '1300'},
            {'timestamp': '2026-03-02 10:15:00', 'open': '199.5', 'high': '200', 'low': '196', 'close': '197', 'volume': '2500'},
            {'timestamp': '2026-03-02 10:30:00', 'open': '197', 'high': '198', 'low': '195.8', 'close': '196.2', 'volume': '2200'},
            {'timestamp': '2026-03-02 10:45:00', 'open': '196.2', 'high': '197', 'low': '194.5', 'close': '194.8', 'volume': '2100'},
        ]

    def _choppy_breakout_rows(self):
        return [
            {'timestamp': '2026-03-03 09:15:00', 'open': '100', 'high': '101', 'low': '99', 'close': '100.2', 'volume': '1000'},
            {'timestamp': '2026-03-03 09:30:00', 'open': '100.2', 'high': '100.8', 'low': '99.7', 'close': '100.0', 'volume': '1000'},
            {'timestamp': '2026-03-03 09:45:00', 'open': '100.0', 'high': '100.9', 'low': '99.6', 'close': '100.3', 'volume': '1000'},
            {'timestamp': '2026-03-03 10:00:00', 'open': '100.3', 'high': '100.7', 'low': '99.8', 'close': '100.1', 'volume': '1000'},
            {'timestamp': '2026-03-03 10:15:00', 'open': '100.1', 'high': '103.5', 'low': '100', 'close': '103', 'volume': '2000'},
            {'timestamp': '2026-03-03 10:30:00', 'open': '103', 'high': '104', 'low': '101.2', 'close': '103.4', 'volume': '1800'},
            {'timestamp': '2026-03-03 10:45:00', 'open': '103.4', 'high': '104.5', 'low': '102.8', 'close': '104.2', 'volume': '1800'},
        ]

    def test_long_trade_uses_confirmation_slippage_and_bounded_stop(self):
        candles = load_candles(self._strong_long_rows())
        trades = generate_trades(candles, capital=100000, risk_pct=0.01)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade['side'], 'BUY')
        self.assertEqual(trade['fill_model'], 'TRIGGER_PLUS_SLIPPAGE')
        self.assertEqual(trade['market_regime'], 'TREND')
        self.assertEqual(trade['first_hour_bias'], 'BUY')
        self.assertEqual(trade['bias_aligned'], 'YES')
        self.assertAlmostEqual(trade['entry_trigger_price'], 102.0, places=4)
        self.assertGreater(trade['entry_price'], trade['entry_trigger_price'])
        self.assertAlmostEqual(trade['entry_price'], 102.1188, places=4)
        self.assertAlmostEqual(trade['stop_loss'], 100.2375, places=4)
        self.assertAlmostEqual(trade['risk_per_unit'], 1.8813, places=4)
        self.assertAlmostEqual(trade['target_price'], 105.8813, places=4)
        self.assertEqual(trade['quantity'], 531)
        self.assertEqual(trade['exit_reason'], 'TARGET')
        self.assertAlmostEqual(trade['gross_pnl'], 1997.89, places=2)
        self.assertAlmostEqual(trade['pnl'], 1997.89, places=2)

    def test_applies_trading_costs_after_slippage_adjusted_fill(self):
        candles = load_candles(self._strong_long_rows())
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, cost_bps=10.0, fixed_cost_per_trade=5.0)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertAlmostEqual(trade['gross_pnl'], 1997.89, places=2)
        self.assertAlmostEqual(trade['trading_cost'], 115.45, places=2)
        self.assertAlmostEqual(trade['pnl'], 1882.44, places=2)

    def test_first_hour_bias_blocks_counter_trend_breakout_when_required(self):
        candles = load_candles(self._counter_trend_short_rows())

        trades_with_bias = generate_trades(candles, capital=100000, risk_pct=0.01, use_first_hour_bias=True)
        trades_without_bias = generate_trades(candles, capital=100000, risk_pct=0.01, use_first_hour_bias=False)

        self.assertEqual(trades_with_bias, [])
        self.assertEqual(len(trades_without_bias), 1)
        trade = trades_without_bias[0]
        self.assertEqual(trade['side'], 'SELL')
        self.assertEqual(trade['first_hour_bias'], 'BUY')
        self.assertEqual(trade['bias_mode'], 'OBSERVE_ONLY')
        self.assertEqual(trade['bias_aligned'], 'NO')
        self.assertAlmostEqual(trade['entry_trigger_price'], 199.0, places=4)
        self.assertLess(trade['entry_price'], trade['entry_trigger_price'])
        self.assertEqual(trade['exit_reason'], 'TARGET')
        self.assertAlmostEqual(trade['gross_pnl'], 1998.0, places=2)

    def test_regime_filter_skips_choppy_breakouts_unless_disabled(self):
        candles = load_candles(self._choppy_breakout_rows())

        filtered_trades = generate_trades(candles, capital=100000, risk_pct=0.01, use_first_hour_bias=False, filter_choppy_days=True)
        unfiltered_trades = generate_trades(candles, capital=100000, risk_pct=0.01, use_first_hour_bias=False, filter_choppy_days=False)

        self.assertEqual(filtered_trades, [])
        self.assertEqual(len(unfiltered_trades), 1)
        trade = unfiltered_trades[0]
        self.assertEqual(trade['market_regime'], 'CHOPPY')
        self.assertEqual(trade['regime_filter'], 'OFF')
        self.assertEqual(trade['side'], 'BUY')
        self.assertEqual(trade['exit_reason'], 'TARGET')


if __name__ == '__main__':
    unittest.main()
