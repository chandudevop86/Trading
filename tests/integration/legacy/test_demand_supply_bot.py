import unittest

from src.breakout_bot import load_candles
from src.demand_supply_bot import DemandSupplyConfig, Zone, generate_trades, score_zone


class TestDemandSupplyBot(unittest.TestCase):
    def _buy_rows(self) -> list[dict[str, str]]:
        return [
            {"timestamp": "2026-03-05 09:15:00", "open": "100.8", "high": "101.0", "low": "100.4", "close": "100.6", "volume": "1000"},
            {"timestamp": "2026-03-05 09:20:00", "open": "100.6", "high": "100.8", "low": "98.9", "close": "99.3", "volume": "1500"},
            {"timestamp": "2026-03-05 09:25:00", "open": "99.3", "high": "100.4", "low": "99.1", "close": "100.2", "volume": "1400"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100.2", "high": "101.2", "low": "100.0", "close": "101.0", "volume": "1400"},
            {"timestamp": "2026-03-05 09:35:00", "open": "101.0", "high": "101.8", "low": "100.8", "close": "101.5", "volume": "1450"},
            {"timestamp": "2026-03-05 09:40:00", "open": "101.5", "high": "101.6", "low": "99.2", "close": "100.7", "volume": "1700"},
            {"timestamp": "2026-03-05 09:45:00", "open": "100.7", "high": "102.3", "low": "100.5", "close": "102.2", "volume": "1800"},
            {"timestamp": "2026-03-05 09:50:00", "open": "101.7", "high": "103.4", "low": "101.6", "close": "103.1", "volume": "1900"},
            {"timestamp": "2026-03-05 09:55:00", "open": "103.1", "high": "103.8", "low": "102.8", "close": "103.6", "volume": "1200"},
        ]

    def _sell_rows(self) -> list[dict[str, str]]:
        return [
            {"timestamp": "2026-03-05 09:15:00", "open": "102.0", "high": "102.3", "low": "101.7", "close": "102.1", "volume": "1200"},
            {"timestamp": "2026-03-05 09:20:00", "open": "102.1", "high": "104.2", "low": "101.9", "close": "103.8", "volume": "1800"},
            {"timestamp": "2026-03-05 09:25:00", "open": "103.8", "high": "103.9", "low": "102.7", "close": "102.9", "volume": "1600"},
            {"timestamp": "2026-03-05 09:30:00", "open": "102.9", "high": "102.7", "low": "101.7", "close": "102.0", "volume": "1500"},
            {"timestamp": "2026-03-05 09:35:00", "open": "102.0", "high": "101.9", "low": "101.0", "close": "101.3", "volume": "1400"},
            {"timestamp": "2026-03-05 09:40:00", "open": "101.3", "high": "103.9", "low": "101.2", "close": "102.4", "volume": "1750"},
            {"timestamp": "2026-03-05 09:45:00", "open": "102.4", "high": "102.5", "low": "100.8", "close": "100.9", "volume": "1900"},
            {"timestamp": "2026-03-05 09:50:00", "open": "101.2", "high": "101.4", "low": "99.1", "close": "99.4", "volume": "2000"},
            {"timestamp": "2026-03-05 09:55:00", "open": "99.4", "high": "99.6", "low": "98.9", "close": "99.1", "volume": "1500"},
        ]

    def test_generates_buy_trade_from_confirmed_demand_retest(self):
        candles = load_candles(self._buy_rows())
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=2.0, pivot_window=1)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade['strategy'], 'DEMAND_SUPPLY')
        self.assertEqual(trade['side'], 'BUY')
        self.assertEqual(trade['setup_type'], 'retest')
        self.assertEqual(trade['first_touch_entry_allowed'], 'NO')
        self.assertEqual(trade['vwap_aligned'], 'YES')
        self.assertEqual(trade['bias_aligned'], 'YES')
        self.assertGreaterEqual(float(trade['zone_strength_score']), float(trade['score_threshold']))
        self.assertGreater(float(trade['reaction_component']), 0.0)
        self.assertGreater(float(trade['retest_component']), 0.0)

    def test_generates_sell_trade_from_confirmed_supply_retest(self):
        candles = load_candles(self._sell_rows())
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=2.0, pivot_window=1)

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade['side'], 'SELL')
        self.assertEqual(trade['vwap_aligned'], 'YES')
        self.assertEqual(trade['setup_type'], 'retest')
        self.assertGreaterEqual(float(trade['zone_strength_score']), float(trade['score_threshold']))

    def test_blocks_first_touch_entry_without_retest_confirmation(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100.8", "high": "101.0", "low": "100.4", "close": "100.6", "volume": "1000"},
            {"timestamp": "2026-03-05 09:20:00", "open": "100.6", "high": "100.8", "low": "98.9", "close": "99.3", "volume": "1500"},
            {"timestamp": "2026-03-05 09:25:00", "open": "99.3", "high": "100.4", "low": "99.1", "close": "100.2", "volume": "1400"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100.2", "high": "101.2", "low": "100.0", "close": "101.0", "volume": "1400"},
            {"timestamp": "2026-03-05 09:35:00", "open": "101.0", "high": "101.8", "low": "100.8", "close": "101.5", "volume": "1450"},
            {"timestamp": "2026-03-05 09:40:00", "open": "101.5", "high": "101.7", "low": "99.6", "close": "101.6", "volume": "1700"},
            {"timestamp": "2026-03-05 09:45:00", "open": "101.6", "high": "102.0", "low": "101.4", "close": "101.9", "volume": "1800"},
            {"timestamp": "2026-03-05 09:50:00", "open": "101.9", "high": "103.0", "low": "101.7", "close": "102.8", "volume": "1900"},
        ]
        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=2.0, pivot_window=1)
        self.assertEqual(trades, [])

    def test_skips_midday_and_optional_afternoon_session_by_default(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:20:00", "open": "100", "high": "98.5", "low": "97.8", "close": "98.1", "volume": "1000"},
            {"timestamp": "2026-03-05 12:10:00", "open": "98.1", "high": "98.4", "low": "97.6", "close": "97.9", "volume": "1200"},
            {"timestamp": "2026-03-05 12:15:00", "open": "97.9", "high": "99.2", "low": "97.8", "close": "99.1", "volume": "1300"},
            {"timestamp": "2026-03-05 13:50:00", "open": "99.1", "high": "99.4", "low": "97.6", "close": "97.8", "volume": "1500"},
            {"timestamp": "2026-03-05 13:55:00", "open": "97.8", "high": "99.3", "low": "97.6", "close": "99.2", "volume": "1600"},
            {"timestamp": "2026-03-05 14:00:00", "open": "99.2", "high": "101.0", "low": "99.0", "close": "100.8", "volume": "1700"},
        ]
        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=2.0, pivot_window=1)
        self.assertEqual(trades, [])

        afternoon_trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=2.0,
            config=DemandSupplyConfig(allow_afternoon_session=True),
        )
        if afternoon_trades:
            self.assertEqual(afternoon_trades[0]['session_window'], 'AFTERNOON')

    def test_prequalification_floor_filters_out_weaker_zones_before_entry(self):
        candles = load_candles(self._buy_rows())
        trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=2.0,
            config=DemandSupplyConfig(min_zone_selection_score=4.6),
        )

        self.assertEqual(trades, [])
    def test_zone_scoring_exposes_weighted_components(self):
        candles = load_candles(self._buy_rows())
        zone = Zone(kind='demand', low=98.9, high=100.6, idx=1, reaction_strength=0.95)
        scored = score_zone(candles, 6, zone, 'BUY', DemandSupplyConfig(), touch=True, retest_confirmed=True)

        self.assertIsNotNone(scored)
        score_value, components, _, _, diagnostics = scored
        self.assertGreaterEqual(score_value, diagnostics['score_threshold'])
        self.assertIn('freshness', components)
        self.assertIn('retest_confirmation', components)
        self.assertGreater(float(diagnostics['reaction_score']), 0.0)
        self.assertGreater(float(diagnostics['zone_selection_score']), 0.0)
        self.assertEqual(diagnostics['zone_status'], 'PASS')
        self.assertEqual(diagnostics['zone_fail_reasons'], [])



    def test_rejected_zone_reports_fail_reasons_in_zone_records(self):
        from src.strategies.supply_demand import detect_scored_zones

        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100.0", "high": "100.5", "low": "99.8", "close": "100.1", "volume": "1000"},
            {"timestamp": "2026-03-05 09:20:00", "open": "100.1", "high": "100.2", "low": "98.9", "close": "99.2", "volume": "1100"},
            {"timestamp": "2026-03-05 09:25:00", "open": "99.2", "high": "99.5", "low": "99.0", "close": "99.3", "volume": "900"},
            {"timestamp": "2026-03-05 09:30:00", "open": "99.3", "high": "99.7", "low": "99.1", "close": "99.4", "volume": "850"},
            {"timestamp": "2026-03-05 09:35:00", "open": "99.4", "high": "99.8", "low": "99.2", "close": "99.5", "volume": "820"},
            {"timestamp": "2026-03-05 09:40:00", "open": "99.5", "high": "99.7", "low": "99.0", "close": "99.1", "volume": "840"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99.1", "high": "99.6", "low": "99.0", "close": "99.4", "volume": "830"},
            {"timestamp": "2026-03-05 09:50:00", "open": "99.4", "high": "99.9", "low": "99.3", "close": "99.5", "volume": "810"},
            {"timestamp": "2026-03-05 09:55:00", "open": "99.5", "high": "100.0", "low": "99.4", "close": "99.6", "volume": "805"},
        ]

        zones = detect_scored_zones(rows, symbol='NIFTY')
        rejected = [zone for zone in zones if zone.zone_status == 'FAIL']

        self.assertTrue(rejected)
        self.assertTrue(any(reason for reason in rejected[0].zone_fail_reasons.split(',') if reason))
if __name__ == '__main__':
    unittest.main()



