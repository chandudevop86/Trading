import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.operational_daemon as operational_daemon
import src.runtime_workflow_service as runtime_workflow_service
import src.trading_workflows as trading_workflows
from src.analytics.readiness_api import evaluate_readiness, summarize_validation_failures
from src.data.cleaner import coerce_ohlcv
from src.execution.contracts import CONTRACT_VERSION, normalize_candidate_contract, validate_candidate_contract
from src.execution.guardrails import GuardConfig, check_all_guards
from src.execution.paper_execution_service import CanonicalExecutionConfig, ExecutionAuditLogger, execute_candidate
from src.execution.state import TradingState
from src.telegram_notifier import build_trade_summary
import src.execution_engine as execution_engine
from src.execution.guards import execute_candidates, execute_paper_trades
from src.strategy_service import standardize_strategy_rows
from src.trading_workflows import run_paper_workflow


class TestProductionExecutionApi(unittest.TestCase):
    def _current_ts(self, minute: int = 20) -> str:
        now = pd.Timestamp.now().floor("min")
        return now.replace(hour=10, minute=minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    def test_strategy_standardization_emits_canonical_contract_shell(self):
        rows = standardize_strategy_rows(
            [{
                "timestamp": self._current_ts(),
                "side": "BUY",
                "entry_price": 100.0,
                "stop_loss": 99.0,
                "target_price": 102.0,
                "timeframe": "5m",
            }],
            strategy_name="Breakout",
            symbol="NIFTY",
        )
        self.assertEqual(rows[0]["strategy_name"], "BREAKOUT")
        self.assertTrue(str(rows[0]["zone_id"]).startswith("NIFTY_BREAKOUT_"))
        self.assertTrue(str(rows[0]["trade_id"]).strip())
        self.assertEqual(rows[0]["validation_status"], "PENDING")
        self.assertFalse(rows[0]["execution_allowed"])
        self.assertEqual(rows[0]["contract_version"], CONTRACT_VERSION)
    def test_telegram_summary_includes_trade_and_zone_ids(self):
        summary = build_trade_summary([
            {
                "trade_id": "TRADE-123",
                "zone_id": "ZONE-123",
                "timestamp": self._current_ts(),
                "side": "BUY",
                "entry": 100.0,
                "stop_loss": 99.0,
                "target": 102.0,
            }
        ])
        self.assertIn("Trade ID: TRADE-123", summary)
        self.assertIn("Zone ID: ZONE-123", summary)

    def test_strategy_output_missing_zone_id_is_blocked_before_standardization(self):
        raw = {
            "symbol": "NIFTY",
            "timestamp": self._current_ts(),
            "strategy_name": "BREAKOUT",
            "setup_type": "BREAKOUT",
            "side": "BUY",
            "entry": 100.0,
            "stop_loss": 99.0,
            "target": 102.0,
            "timeframe": "5m",
            "validation_status": "PASS",
            "validation_score": 8.0,
            "validation_reasons": [],
            "execution_allowed": True,
        }
        ok, reasons, _normalized = validate_candidate_contract(raw)
        self.assertFalse(ok)
        self.assertIn("MISSING_ZONE_ID", reasons)

    def test_invalid_trade_contract_is_blocked(self):
        state = TradingState()
        candidate = {
            "symbol": "NIFTY",
            "timestamp": self._current_ts(),
            "strategy_name": "BREAKOUT",
            "setup_type": "BREAKOUT",
            "zone_id": "NIFTY_BREAKOUT_01",
            "side": "BUY",
            "entry": 100.0,
            "stop_loss": 0.0,
            "target": 102.0,
            "timeframe": "5m",
            "validation_status": "PASS",
            "validation_score": 8.0,
            "validation_reasons": [],
            "execution_allowed": True,
            "contract_version": CONTRACT_VERSION,
        }
        result = check_all_guards(candidate, state, GuardConfig(stale_after_minutes=0))
        self.assertFalse(result.allowed)
        self.assertIn("INVALID_STOP_LOSS", result.reasons)

    def test_execute_candidate_blocks_validation_fail(self):
        with tempfile.TemporaryDirectory() as td:
            logger = ExecutionAuditLogger(Path(td))
            state = TradingState()
            candidate = normalize_candidate_contract(
                {
                    "symbol": "NIFTY",
                    "timestamp": self._current_ts(),
                    "strategy_name": "BREAKOUT",
                    "setup_type": "BREAKOUT",
                    "zone_id": "NIFTY_BREAKOUT_01",
                    "side": "BUY",
                    "entry": 100.0,
                    "stop_loss": 99.0,
                    "target": 102.0,
                    "timeframe": "5m",
                    "validation_status": "FAIL",
                    "validation_score": 5.0,
                    "validation_reasons": ["bad_rr"],
                    "execution_allowed": False,
                    "contract_version": CONTRACT_VERSION,
                }
            )
            decision = execute_candidate(candidate, state, CanonicalExecutionConfig(output_path=Path(td) / "executed.csv"), logger)
            self.assertFalse(decision.allowed)
            self.assertIn("VALIDATION_NOT_PASS", decision.reasons)

    def test_execute_paper_trades_blocks_execution_allowed_false(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            candidate = normalize_candidate_contract(
                {
                    "symbol": "NIFTY",
                    "timestamp": self._current_ts(),
                    "strategy_name": "DEMAND_SUPPLY",
                    "setup_type": "DEMAND_SUPPLY",
                    "zone_id": "NIFTY_DS_01",
                    "side": "BUY",
                    "entry": 100.0,
                    "stop_loss": 99.0,
                    "target": 102.5,
                    "timeframe": "5m",
                    "validation_status": "PASS",
                    "validation_score": 8.0,
                    "validation_reasons": [],
                    "execution_allowed": False,
                    "contract_version": CONTRACT_VERSION,
                }
            )
            result = execute_paper_trades([candidate], out)
            self.assertEqual(result.executed_count, 0)
            self.assertEqual(result.blocked_count, 1)
            self.assertEqual(result.blocked_rows[0]["blocked_reason"], "EXECUTION_NOT_ALLOWED")

    def test_execute_paper_trades_blocks_legacy_candidate_without_required_schema(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            legacy_candidate = {
                "strategy": "BREAKOUT",
                "symbol": "NIFTY",
                "signal_time": self._current_ts(),
                "side": "BUY",
                "price": 100.0,
                "quantity": 65,
            }
            result = execute_paper_trades([legacy_candidate], out)
            self.assertEqual(result.executed_count, 0)
            self.assertEqual(result.blocked_count, 1)
            self.assertIn("MISSING_ZONE_ID", result.blocked_rows[0]["reason_codes"])
            self.assertIn("MISSING_VALIDATION_STATUS", result.blocked_rows[0]["reason_codes"])

    def test_duplicate_zone_and_cooldown_are_blocked(self):
        state = TradingState.from_rows(
            [
                {
                    "zone_id": "NIFTY_DS_01",
                    "trade_id": "trade-1",
                    "execution_status": "EXECUTED",
                    "timestamp": self._current_ts(15),
                    "symbol": "NIFTY",
                    "strategy_name": "DEMAND_SUPPLY",
                    "setup_type": "DEMAND_SUPPLY",
                    "side": "BUY",
                    "entry": 100.0,
                    "stop_loss": 99.0,
                    "target": 102.0,
                    "timeframe": "5m",
                    "validation_status": "PASS",
                    "validation_score": 8.0,
                    "validation_reasons": [],
                    "execution_allowed": True,
                }
            ]
        )
        candidate = normalize_candidate_contract(
            {
                "symbol": "NIFTY",
                "timestamp": self._current_ts(20),
                "strategy_name": "DEMAND_SUPPLY",
                "setup_type": "DEMAND_SUPPLY",
                "zone_id": "NIFTY_DS_01",
                "side": "BUY",
                "entry": 100.0,
                "stop_loss": 99.0,
                "target": 102.0,
                "timeframe": "5m",
                "validation_status": "PASS",
                "validation_score": 8.0,
                "validation_reasons": [],
                "execution_allowed": True,
                "contract_version": CONTRACT_VERSION,
            }
        )
        result = check_all_guards(candidate, state, GuardConfig(cooldown_minutes=15, stale_after_minutes=0))
        self.assertFalse(result.allowed)
        self.assertIn("DUPLICATE_ZONE", result.reasons)
        self.assertIn("COOLDOWN_ACTIVE", result.reasons)

    def test_daily_loss_and_invalid_session_are_blocked(self):
        state = TradingState()
        state.daily_pnl_by_day[pd.Timestamp(self._current_ts()).strftime("%Y-%m-%d")] = -1000.0
        candidate = normalize_candidate_contract(
            {
                "symbol": "NIFTY",
                "timestamp": pd.Timestamp(self._current_ts()).replace(hour=8, minute=30).strftime("%Y-%m-%d %H:%M:%S"),
                "strategy_name": "BREAKOUT",
                "setup_type": "BREAKOUT",
                "zone_id": "NIFTY_BO_01",
                "side": "BUY",
                "entry": 100.0,
                "stop_loss": 99.0,
                "target": 102.0,
                "timeframe": "5m",
                "validation_status": "PASS",
                "validation_score": 8.0,
                "validation_reasons": [],
                "execution_allowed": True,
                "contract_version": CONTRACT_VERSION,
            }
        )
        result = check_all_guards(candidate, state, GuardConfig(max_daily_loss=500.0, stale_after_minutes=0))
        self.assertFalse(result.allowed)
        self.assertIn("MAX_DAILY_LOSS", result.reasons)
        self.assertIn("INVALID_SESSION", result.reasons)

    def test_execute_paper_trades_uses_canonical_entrypoint(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            strict_candidate = normalize_candidate_contract(
                {
                    "symbol": "NIFTY",
                    "timestamp": self._current_ts(),
                    "strategy_name": "BREAKOUT",
                    "setup_type": "BREAKOUT",
                    "zone_id": "NIFTY_BREAKOUT_07",
                    "side": "BUY",
                    "entry": 100.0,
                    "stop_loss": 99.0,
                    "target": 102.0,
                    "timeframe": "5m",
                    "validation_status": "PASS",
                    "validation_score": 8.0,
                    "validation_reasons": [],
                    "execution_allowed": True,
                    "contract_version": CONTRACT_VERSION,
                }
            )
            with patch("src.execution.paper_execution_service.run_canonical_paper_execution") as mock_run:
                class _Result:
                    rows = []
                    blocked_rows = []
                    executed_rows = []
                    blocked_count = 0
                    executed_count = 0
                    skipped_count = 0
                    duplicate_count = 0
                    error_count = 0
                mock_run.return_value = (_Result(), [], TradingState())
                execute_paper_trades([strict_candidate], out)
            mock_run.assert_called_once()

    def test_runtime_modules_import_guard_gateway_for_paper_execution(self):
        self.assertEqual(runtime_workflow_service.execute_candidates.__module__, "src.execution.guards")
        self.assertEqual(trading_workflows.execute_candidates.__module__, "src.execution.guards")
        self.assertEqual(operational_daemon.execute_candidates.__module__, "src.execution.guards")

    def test_execution_engine_wrapper_delegates_to_guard_gateway(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            with patch("src.execution.guards.execute_paper_trades") as mock_gateway:
                class _Result:
                    rows = []
                    blocked_rows = []
                    executed_rows = []
                    skipped_rows = []
                    executed_count = 0
                    blocked_count = 0
                    skipped_count = 0
                    duplicate_count = 0
                    error_count = 0
                mock_gateway.return_value = _Result()
                execution_engine.execute_paper_trades([], out)
            mock_gateway.assert_called_once()

    def test_canonical_execute_candidates_gateway_is_used_for_live_and_paper(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            with patch("src.execution.paper_execution_service.run_canonical_paper_execution") as mock_run:
                class _Result:
                    rows = []
                    blocked_rows = []
                    executed_rows = []
                    skipped_rows = []
                    executed_count = 0
                    blocked_count = 0
                    skipped_count = 0
                    duplicate_count = 0
                    error_count = 0
                mock_run.return_value = (_Result(), [], TradingState())
                execute_candidates([], out, execution_mode="PAPER")
                execute_candidates([], out, execution_mode="LIVE")
            self.assertEqual(mock_run.call_count, 2)

    def test_run_paper_workflow_normalizes_and_executes_candidates(self):
        with tempfile.TemporaryDirectory() as td:
            candles = pd.DataFrame(
                [
                    {"timestamp": self._current_ts(15), "open": 100.0, "high": 100.8, "low": 99.9, "close": 100.6, "volume": 1000},
                    {"timestamp": self._current_ts(20), "open": 100.6, "high": 101.1, "low": 100.5, "close": 101.0, "volume": 1100},
                ]
            )
            rows = [
                {
                    "symbol": "NIFTY",
                    "timestamp": self._current_ts(20),
                    "strategy_name": "BREAKOUT",
                    "setup_type": "BREAKOUT",
                    "zone_id": "NIFTY_BREAKOUT_01",
                    "side": "BUY",
                    "entry": 101.0,
                    "stop_loss": 100.5,
                    "target": 102.0,
                    "timeframe": "5m",
                    "contract_version": CONTRACT_VERSION,
                }
            ]
            with patch("src.execution.pipeline.validate_trade", return_value={"decision": "PASS", "score": 8.4, "reasons": [], "metrics": {}}):
                result = run_paper_workflow(rows, "Breakout", "NIFTY", candles=candles, output_path=Path(td) / "executed.csv")
            self.assertEqual(len(result.execution_candidates), 1)
            self.assertEqual(result.execution_candidates[0]["validation_status"], "PASS")

    def test_readiness_api_and_rejection_summary(self):
        trades = pd.DataFrame(
            [
                {"pnl": 100.0, "risk_per_unit": 1.0, "quantity": 50, "validation_status": "PASS", "execution_status": "EXECUTED", "duplicate_reason": ""},
                {"pnl": -50.0, "risk_per_unit": 1.0, "quantity": 50, "validation_status": "PASS", "execution_status": "EXECUTED", "duplicate_reason": ""},
            ]
        )
        rejects = pd.DataFrame(
            [
                {"reasons": "bad_rr,no_vwap_alignment"},
                {"reasons": "bad_rr"},
            ]
        )
        summary = evaluate_readiness(trades, rejects)
        self.assertEqual(summary["verdict"], "NOT_READY")
        self.assertEqual(summary["readiness_decision"], "NOT_READY")
        self.assertIn("INSUFFICIENT_TRADE_SAMPLE", summary["reasons"])
        self.assertIn("thresholds", summary)
        self.assertIn("threshold_status", summary)
        self.assertIn("failure_counts", summary)
        self.assertEqual(summary["failure_counts"]["insufficient_trade_sample"], 1)
        top = summarize_validation_failures(rejects)
        self.assertEqual(top["bad_rr"], 2)
        self.assertEqual(summary["validation_failure_summary"]["bad_rr"], 2)

    def test_clean_data_to_execution_logging_integration(self):
        with tempfile.TemporaryDirectory() as td:
            raw = pd.DataFrame(
                [
                    {"Date": pd.Timestamp.now().strftime("%Y-%m-%d"), "Time": "10:15:00", "Open": 100, "High": 101, "Low": 99.8, "Close": 100.8, "Volume": 1000},
                    {"Date": pd.Timestamp.now().strftime("%Y-%m-%d"), "Time": "10:20:00", "Open": 100.8, "High": 101.3, "Low": 100.7, "Close": 101.1, "Volume": 1200},
                ]
            )
            cleaned = coerce_ohlcv(raw)
            rows = [{
                "symbol": "NIFTY",
                "timestamp": str(cleaned.iloc[-1]["timestamp"]),
                "strategy_name": "BREAKOUT",
                "setup_type": "BREAKOUT",
                "zone_id": "NIFTY_BREAKOUT_99",
                "side": "BUY",
                "entry": float(cleaned.iloc[-1]["close"]),
                "stop_loss": float(cleaned.iloc[-1]["close"]) - 0.4,
                "target": float(cleaned.iloc[-1]["close"]) + 0.8,
                "timeframe": "5m",
                "contract_version": CONTRACT_VERSION,
            }]
            with patch("src.execution.pipeline.validate_trade", return_value={"decision": "PASS", "score": 8.8, "reasons": [], "metrics": {}}):
                workflow = run_paper_workflow(rows, "Breakout", "NIFTY", candles=cleaned, output_path=Path(td) / "executed.csv")
            self.assertTrue((Path(td) / "candidates_log.csv").exists())
            self.assertTrue((Path(td) / "executed_trades_log.csv").exists())
            self.assertEqual(len(workflow.execution_candidates), 1)


if __name__ == "__main__":
    unittest.main()








