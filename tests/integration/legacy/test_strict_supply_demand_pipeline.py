import unittest

import pandas as pd

from src.strategies.strict_supply_demand import (
    SupplyDemandStrategyConfig,
    build_supply_demand_report,
    detect_supply_demand_structures,
    evaluate_supply_demand_readiness,
    generate_supply_demand_trade_candidates,
    normalize_ohlcv_for_supply_demand,
)


class TestStrictSupplyDemandPipeline(unittest.TestCase):
    def _sample_frame(self) -> pd.DataFrame:
        rows = [
            {"timestamp": "2026-04-01 09:15:00", "open": 110.0, "high": 110.5, "low": 108.8, "close": 109.0, "volume": 1000},
            {"timestamp": "2026-04-01 09:20:00", "open": 109.0, "high": 109.2, "low": 108.5, "close": 108.6, "volume": 1200},
            {"timestamp": "2026-04-01 09:25:00", "open": 108.6, "high": 108.9, "low": 108.3, "close": 108.5, "volume": 900},
            {"timestamp": "2026-04-01 09:30:00", "open": 108.5, "high": 108.8, "low": 108.2, "close": 108.45, "volume": 850},
            {"timestamp": "2026-04-01 09:35:00", "open": 108.45, "high": 111.2, "low": 108.4, "close": 110.9, "volume": 2200},
            {"timestamp": "2026-04-01 09:40:00", "open": 110.9, "high": 111.4, "low": 109.7, "close": 110.1, "volume": 1500},
            {"timestamp": "2026-04-01 09:45:00", "open": 110.1, "high": 111.8, "low": 109.9, "close": 111.5, "volume": 1900},
        ]
        return pd.DataFrame(rows)

    def test_normalization_maps_aliases_and_drops_duplicate_timestamps(self):
        raw = pd.DataFrame([
            {"Date": "2026-04-01", "Time": "09:15:00", "O": 100, "H": 101, "L": 99, "C": 100.5, "Vol": 1000},
            {"Date": "2026-04-01", "Time": "09:15:00", "O": 100.1, "H": 101.2, "L": 99.1, "C": 100.6, "Vol": 1100},
            {"Date": "2026-04-01", "Time": "09:20:00", "O": 100.6, "H": 101.4, "L": 100.2, "C": 101.1, "Vol": 1200},
        ])
        cleaned = normalize_ohlcv_for_supply_demand(raw)
        self.assertEqual(list(cleaned.columns[:6]), ["timestamp", "open", "high", "low", "close", "volume"])
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(str(cleaned.iloc[0]["timestamp"]), "2026-04-01 09:15:00")

    def test_invalid_timestamp_raises(self):
        raw = pd.DataFrame([{"timestamp": "bad-ts", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000}])
        with self.assertRaises(Exception):
            normalize_ohlcv_for_supply_demand(raw)

    def test_detects_structure_and_generates_trade_candidate(self):
        cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
        structures = detect_supply_demand_structures(self._sample_frame(), cfg)
        self.assertTrue(structures)
        trades, rejects, zones = generate_supply_demand_trade_candidates(self._sample_frame(), cfg)
        self.assertTrue(zones)
        self.assertTrue(trades or rejects)
        if trades:
            self.assertEqual(trades[0]["validation_status"], "PASS")
            self.assertGreater(trades[0]["quantity"], 0)

    def test_duplicate_zone_is_blocked(self):
        cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
        first_trades, _, _ = generate_supply_demand_trade_candidates(self._sample_frame(), cfg)
        existing = [{"zone_id": row["zone_id"], "trade_time": row["trade_time"]} for row in first_trades]
        second_trades, second_rejects, _ = generate_supply_demand_trade_candidates(self._sample_frame(), cfg, existing_trade_rows=existing)
        self.assertEqual(len(second_trades), 0)
        self.assertTrue(any("duplicate_zone" in row["reasons"] for row in second_rejects))

    def test_readiness_and_report_expose_reason_counts(self):
        executed = [
            {"pnl": 200.0, "risk_per_unit": 2.0, "quantity": 50, "validation_status": "PASS"},
            {"pnl": -80.0, "risk_per_unit": 2.0, "quantity": 40, "validation_status": "PASS"},
        ]
        rejects = [{"reasons": ["weak_zone_score"]}, {"reasons": ["weak_zone_score", "invalid_rr"]}]
        readiness = evaluate_supply_demand_readiness(executed, rejects, min_trades=2)
        self.assertIn("validation_fail_counts", readiness)
        self.assertEqual(readiness["validation_fail_counts"]["weak_zone_score"], 2)
        report = build_supply_demand_report(self._sample_frame(), SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0))
        self.assertIn("zone_rows", report)
        self.assertIn("trade_rows", report)
        self.assertIn("readiness_summary", report)


if __name__ == "__main__":
    unittest.main()
