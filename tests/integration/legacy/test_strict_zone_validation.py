import unittest

import pandas as pd

from src.strict_zone_validation import (
    StrictValidationConfig,
    clear_rejected_trades_log,
    execution_allowed,
    get_rejected_trades_frame,
    standardize_market_data,
    validate_zone_candidate,
)


class TestStrictZoneValidation(unittest.TestCase):
    def setUp(self) -> None:
        clear_rejected_trades_log()
        self.config = StrictValidationConfig()
        self.df_15m = pd.DataFrame(
            [
                ["2026-03-29 09:15:00", 100.8, 101.0, 100.2, 100.9, 1000],
                ["2026-03-29 09:30:00", 100.9, 103.2, 100.8, 103.0, 1500],
                ["2026-03-29 09:45:00", 103.0, 103.4, 101.1, 101.7, 1300],
                ["2026-03-29 10:00:00", 101.7, 104.2, 101.6, 103.9, 1600],
            ],
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        self.df_5m = pd.DataFrame(
            [
                ["2026-03-29 09:15:00", 100.8, 101.0, 100.4, 100.9, 1000],
                ["2026-03-29 09:20:00", 100.9, 102.8, 100.8, 102.6, 1400],
                ["2026-03-29 09:25:00", 102.6, 103.0, 102.2, 102.8, 1200],
                ["2026-03-29 09:30:00", 102.8, 103.1, 101.2, 101.4, 1100],
                ["2026-03-29 09:35:00", 101.4, 102.2, 100.2, 101.8, 1600],
                ["2026-03-29 09:40:00", 101.8, 103.2, 101.7, 103.0, 1500],
                ["2026-03-29 09:45:00", 103.0, 103.6, 102.8, 103.4, 1450],
                ["2026-03-29 09:50:00", 103.4, 103.8, 103.2, 103.6, 1300],
            ],
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )

    def _zone(self, **overrides):
        zone = {
            "symbol": "NIFTY",
            "zone_id": "NIFTY_2026-03-29_15m_demand_01",
            "zone_type": "demand",
            "timeframe_zone": "15m",
            "timeframe_entry": "5m",
            "creation_timestamp": pd.Timestamp("2026-03-29 09:15:00"),
            "zone_low": 100.0,
            "zone_high": 101.0,
            "departure_atr": 1.8,
            "impulsive_candles": 2,
            "avg_body_pct": 0.65,
            "close_location_strength": 0.8,
            "displacement_speed": 2,
            "bos_confirmed": True,
            "base_candles": 3,
            "base_range_atr": 0.55,
            "wick_ratio": 0.8,
            "overlap_ratio": 0.35,
            "imbalance_score": 7.5,
            "touch_count": 0,
            "candles_since_zone_created": 8,
            "entry_idx": 4,
            "entry_price": 101.8,
            "stop_price": 99.8,
            "target_price": 106.2,
            "penetration_pct": 0.30,
            "rejection_score": 7.5,
            "atr_pct": 0.0035,
            "session_range": 3.6,
            "volatility_percentile": 0.65,
            "volatility_score": 7.2,
            "chop_score": 3.0,
            "structure_score": 7.2,
            "zone_width_atr": 0.8,
            "setup_already_used": False,
            "reversal_score": 8.0,
            "breakout_chase_entry": False,
            "rr_ratio": 2.2,
        }
        zone.update(overrides)
        return zone


    def test_standardizes_alias_columns(self):
        aliased = self.df_5m.rename(columns={"timestamp": "time", "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
        standardized = standardize_market_data(aliased, expected_interval_minutes=5, require_vwap=True, config=self.config)
        self.assertEqual(list(standardized.columns[:6]), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertIn("vwap", standardized.columns)

    def test_duplicate_timestamps_fail_as_invalid_input(self):
        duplicate = pd.concat([self.df_5m.iloc[[0]], self.df_5m], ignore_index=True)
        result = validate_zone_candidate(self._zone(), duplicate, self.df_15m, self.config)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("invalid_input", result["fail_reasons"])
        self.assertFalse(result["execution_allowed"])

    def test_missing_candles_fail_as_invalid_input(self):
        missing = self.df_5m.drop(index=2).reset_index(drop=True)
        result = validate_zone_candidate(self._zone(), missing, self.df_15m, self.config)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("invalid_input", result["fail_reasons"])
        self.assertFalse(result["execution_allowed"])

    def test_pass_result_exposes_execution_gate(self):
        result = validate_zone_candidate(self._zone(), self.df_5m, self.df_15m, self.config)
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(result["execution_allowed"])
        self.assertEqual(result["execution_blockers"], [])
    def test_stale_zone_fails(self):
        result = validate_zone_candidate(self._zone(touch_count=2), self.df_5m, self.df_15m, self.config)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("stale_zone", result["fail_reasons"])

    def test_no_bos_fails(self):
        result = validate_zone_candidate(self._zone(bos_confirmed=False), self.df_5m, self.df_15m, self.config)
        self.assertIn("no_structure_break", result["fail_reasons"])

    def test_deep_penetration_fails(self):
        result = validate_zone_candidate(self._zone(penetration_pct=0.82), self.df_5m, self.df_15m, self.config)
        self.assertIn("deep_zone_penetration", result["fail_reasons"])

    def test_weak_rejection_fails(self):
        result = validate_zone_candidate(self._zone(rejection_score=4.9), self.df_5m, self.df_15m, self.config)
        self.assertIn("weak_retest_reaction", result["fail_reasons"])

    def test_bad_rr_fails(self):
        result = validate_zone_candidate(self._zone(rr_ratio=1.5), self.df_5m, self.df_15m, self.config)
        self.assertIn("bad_rr", result["fail_reasons"])

    def test_vwap_misalignment_fails(self):
        bearish_df = self.df_5m.copy()
        bearish_df.loc[bearish_df.index[-1], ["open", "high", "low", "close"]] = [100.8, 101.0, 100.1, 100.2]
        result = validate_zone_candidate(self._zone(), bearish_df, self.df_15m, self.config)
        self.assertIn("no_vwap_alignment", result["fail_reasons"])

    def test_duplicate_setup_fails(self):
        result = validate_zone_candidate(self._zone(setup_already_used=True), self.df_5m, self.df_15m, self.config)
        self.assertIn("duplicate_setup_context", result["fail_reasons"])

    def test_late_retest_fails(self):
        result = validate_zone_candidate(self._zone(candles_since_zone_created=40), self.df_5m, self.df_15m, self.config)
        self.assertIn("late_retest", result["fail_reasons"])

    def test_chop_market_fails(self):
        result = validate_zone_candidate(self._zone(chop_score=7.4), self.df_5m, self.df_15m, self.config)
        self.assertIn("chop_market_fail", result["fail_reasons"])

    def test_oversized_zone_fails(self):
        result = validate_zone_candidate(self._zone(zone_width_atr=1.5), self.df_5m, self.df_15m, self.config)
        self.assertIn("oversized_zone", result["fail_reasons"])

    def test_breakout_chase_is_rejected(self):
        result = validate_zone_candidate(self._zone(breakout_chase_entry=True), self.df_5m, self.df_15m, self.config)
        self.assertIn("breakout_chase_entry", result["fail_reasons"])
        self.assertFalse(execution_allowed(result))

    def test_rejections_are_logged(self):
        validate_zone_candidate(self._zone(touch_count=2), self.df_5m, self.df_15m, self.config)
        rejected = get_rejected_trades_frame()
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected.iloc[0]["zone_id"], "NIFTY_2026-03-29_15m_demand_01")


if __name__ == "__main__":
    unittest.main()


