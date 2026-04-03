import unittest

from src.execution_engine import (
    SKIP_REASON_DUPLICATE_ZONE_SETUP,
    SKIP_REASON_MISSING_ZONE_ID,
    SKIP_REASON_EXECUTION_GATE_BLOCKED,
    SKIP_REASON_VALIDATION_STATUS_FAIL,
    make_trade_id,
    make_trade_key,
    validate_candidate,
)


class TestExecutionEngineZoneSafety(unittest.TestCase):

    def test_validate_candidate_requires_zone_id_when_zone_gate_is_present(self):
        ok, reason, _ = validate_candidate(
            {
                "strategy": "DEMAND_SUPPLY",
                "symbol": "NIFTY",
                "timeframe": "5m",
                "signal_time": "2026-03-29 09:45:00",
                "side": "BUY",
                "price": 101.8,
                "quantity": 65,
                "stop_loss": 100.8,
                "target_price": 104.2,
                "validation_status": "PASS",
            }
        )
        self.assertFalse(ok)
        self.assertEqual(reason, SKIP_REASON_MISSING_ZONE_ID)

    def test_validate_candidate_blocks_execution_gate_false(self):
        ok, reason, _ = validate_candidate(
            {
                "strategy": "DEMAND_SUPPLY",
                "symbol": "NIFTY",
                "timeframe": "5m",
                "signal_time": "2026-03-29 09:45:00",
                "side": "BUY",
                "price": 101.8,
                "quantity": 65,
                "stop_loss": 100.8,
                "target_price": 104.2,
                "validation_status": "PASS",
                "zone_id": "NIFTY_2026-03-29_15m_demand_01",
                "execution_allowed": False,
            }
        )
        self.assertFalse(ok)
        self.assertEqual(reason, SKIP_REASON_EXECUTION_GATE_BLOCKED)
    def test_validate_candidate_blocks_non_pass_validation_status(self):
        ok, reason, _ = validate_candidate(
            {
                "strategy": "DEMAND_SUPPLY",
                "symbol": "NIFTY",
                "timeframe": "5m",
                "signal_time": "2026-03-29 09:45:00",
                "side": "BUY",
                "price": 101.8,
                "quantity": 65,
                "stop_loss": 100.8,
                "target_price": 104.2,
                "validation_status": "FAIL",
            }
        )
        self.assertFalse(ok)
        self.assertEqual(reason, SKIP_REASON_VALIDATION_STATUS_FAIL)

    def test_validate_candidate_blocks_used_zone_setup(self):
        ok, reason, _ = validate_candidate(
            {
                "strategy": "DEMAND_SUPPLY",
                "symbol": "NIFTY",
                "timeframe": "5m",
                "signal_time": "2026-03-29 09:45:00",
                "side": "BUY",
                "price": 101.8,
                "quantity": 65,
                "stop_loss": 100.8,
                "target_price": 104.2,
                "validation_status": "PASS",
                "setup_already_used": True,
            }
        )
        self.assertFalse(ok)
        self.assertEqual(reason, SKIP_REASON_DUPLICATE_ZONE_SETUP)

    def test_zone_id_stabilizes_trade_identity(self):
        candidate_a = {
            "strategy": "DEMAND_SUPPLY",
            "symbol": "NIFTY",
            "timeframe": "5m",
            "signal_time": "2026-03-29 09:45:00",
            "side": "BUY",
            "price": 101.8,
            "zone_id": "NIFTY_2026-03-29_15m_demand_01",
        }
        candidate_b = {
            "strategy": "DEMAND_SUPPLY",
            "symbol": "NIFTY",
            "timeframe": "5m",
            "signal_time": "2026-03-29 10:00:00",
            "side": "BUY",
            "price": 102.1,
            "zone_id": "NIFTY_2026-03-29_15m_demand_01",
        }
        self.assertEqual(make_trade_key(candidate_a), make_trade_key(candidate_b))
        self.assertEqual(make_trade_id(candidate_a), make_trade_id(candidate_b))


if __name__ == "__main__":
    unittest.main()

