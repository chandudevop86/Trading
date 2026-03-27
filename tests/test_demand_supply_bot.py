import unittest

from src.breakout_bot import load_candles
from src.demand_supply_bot import DemandSupplyConfig, Zone, generate_trades, score_zone


class TestDemandSupplyBot(unittest.TestCase):
    def test_generates_buy_trade_from_demand_retest(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100", "high": "102", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99", "high": "103", "low": "100", "close": "102", "volume": "1000"},
            {"timestamp": "2026-03-05 10:00:00", "open": "102", "high": "104", "low": "101", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 10:15:00", "open": "103", "high": "104", "low": "100", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-05 10:30:00", "open": "101", "high": "103.2", "low": "100.5", "close": "102.4", "volume": "1200"},
            {"timestamp": "2026-03-05 10:45:00", "open": "102.4", "high": "105.8", "low": "102.2", "close": "105.4", "volume": "1400"},
            {"timestamp": "2026-03-05 11:00:00", "open": "105.4", "high": "106", "low": "104.5", "close": "105.8", "volume": "1200"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=1.5, pivot_window=1)

        self.assertEqual(len(trades), 1)
        t = trades[0]
        self.assertEqual(t["strategy"], "DEMAND_SUPPLY")
        self.assertEqual(t["side"], "BUY")
        self.assertEqual(t["vwap_aligned"], "YES")
        self.assertEqual(t["bias_aligned"], "YES")
        self.assertGreaterEqual(float(t["zone_strength_score"]), 4.0)
        self.assertEqual(t["setup_type"], "retest")
        self.assertEqual(t["session_allowed"], "YES")
        self.assertIn(t["trend_bias"], {"BULLISH", "BEARISH", "NEUTRAL"})
        self.assertIn(t["exit_reason"], {"TARGET", "EOD", "STOP_LOSS"})

    def test_blocks_trade_when_zone_strength_threshold_is_raised(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100", "high": "102", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99", "high": "103", "low": "100", "close": "102", "volume": "1000"},
            {"timestamp": "2026-03-05 10:00:00", "open": "102", "high": "104", "low": "101", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 10:15:00", "open": "103", "high": "104", "low": "100", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-05 10:30:00", "open": "101", "high": "103.2", "low": "100.5", "close": "102.4", "volume": "1200"},
            {"timestamp": "2026-03-05 10:45:00", "open": "102.4", "high": "105.8", "low": "102.2", "close": "105.4", "volume": "1400"},
            {"timestamp": "2026-03-05 11:00:00", "open": "105.4", "high": "106", "low": "104.5", "close": "105.8", "volume": "1200"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=1.5,
            config=DemandSupplyConfig(min_zone_strength_score=4.6),
        )

        self.assertEqual(trades, [])

    def test_skips_midday_and_optional_afternoon_session_by_default(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100", "high": "102", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99", "high": "103", "low": "100", "close": "102", "volume": "1000"},
            {"timestamp": "2026-03-05 10:00:00", "open": "102", "high": "104", "low": "101", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 12:10:00", "open": "103", "high": "104", "low": "100", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-05 12:15:00", "open": "101", "high": "103.2", "low": "100.5", "close": "102.4", "volume": "1200"},
            {"timestamp": "2026-03-05 12:20:00", "open": "102.4", "high": "105.8", "low": "102.2", "close": "105.4", "volume": "1400"},
            {"timestamp": "2026-03-05 13:50:00", "open": "105.4", "high": "106", "low": "102", "close": "103", "volume": "1400"},
            {"timestamp": "2026-03-05 13:55:00", "open": "103", "high": "103.4", "low": "101.2", "close": "102.6", "volume": "1400"},
            {"timestamp": "2026-03-05 14:00:00", "open": "102.6", "high": "106.2", "low": "102.5", "close": "105.9", "volume": "1500"},
            {"timestamp": "2026-03-05 14:15:00", "open": "105.9", "high": "106.1", "low": "105.2", "close": "105.7", "volume": "1000"},
        ]

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000, risk_pct=0.01, rr_ratio=1.5, pivot_window=1)
        self.assertEqual(trades, [])

        afternoon_trades = generate_trades(
            candles,
            capital=100000,
            risk_pct=0.01,
            rr_ratio=1.5,
            config=DemandSupplyConfig(allow_afternoon_session=True),
        )
        if afternoon_trades:
            self.assertEqual(afternoon_trades[0]["session_window"], "AFTERNOON")


    def test_zone_scoring_exposes_structure_component(self):
        rows = [
            {"timestamp": "2026-03-05 09:15:00", "open": "100", "high": "101", "low": "99", "close": "100", "volume": "1000"},
            {"timestamp": "2026-03-05 09:30:00", "open": "100", "high": "102", "low": "98", "close": "99", "volume": "1000"},
            {"timestamp": "2026-03-05 09:45:00", "open": "99", "high": "103", "low": "100", "close": "102", "volume": "1000"},
            {"timestamp": "2026-03-05 10:00:00", "open": "102", "high": "104", "low": "101", "close": "103", "volume": "1000"},
            {"timestamp": "2026-03-05 10:15:00", "open": "103", "high": "104", "low": "100", "close": "101", "volume": "1000"},
            {"timestamp": "2026-03-05 10:30:00", "open": "101", "high": "103.2", "low": "100.5", "close": "102.4", "volume": "1200"},
            {"timestamp": "2026-03-05 10:45:00", "open": "102.4", "high": "105.8", "low": "102.2", "close": "105.4", "volume": "1400"},
        ]

        candles = load_candles(rows)
        zone = Zone(kind="demand", low=98.0, high=100.0, idx=1, reaction_strength=0.9)
        scored = score_zone(candles, 6, zone, "BUY", DemandSupplyConfig(), touch=True, retest_confirmed=True)

        self.assertIsNotNone(scored)
        _, _, _, _, diagnostics = scored
        self.assertGreaterEqual(float(diagnostics["structure_score"]), 1.2)
        self.assertGreater(float(diagnostics["zone_selection_score"]), 0.0)
        self.assertGreaterEqual(float(diagnostics["zone_strength_score"]), 4.0)

if __name__ == "__main__":

    unittest.main()




