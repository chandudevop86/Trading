import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))

from src.analytics.metrics import compute_trade_metrics, evaluate_production_readiness
from src.analytics.readiness_api import evaluate_readiness
from src.data.cleaner import CleanerConfig, OHLCVValidationError, coerce_ohlcv
from src.execution.guards import ExecutionGuardConfig, evaluate_trade_guards
from src.runtime_models import TradingActionRequest
from src.runtime_workflow_service import run_execution
from src.validation.engine import ValidationConfig, validate_trade


class TestProductionPipeline(unittest.TestCase):
    def test_coerce_ohlcv_normalizes_aliases_and_removes_duplicate_timestamps(self):
        raw = pd.DataFrame(
            [
                {"Date": "2026-03-20", "Time": "09:15:00", "Open": 100, "High": 101, "Low": 99, "Close": 100.5, "Volume": 1000},
                {"Date": "2026-03-20", "Time": "09:15:00", "Open": 100.1, "High": 101.2, "Low": 99.1, "Close": 100.6, "Volume": 1100},
                {"Date": "2026-03-20", "Time": "09:20:00", "Open": 100.6, "High": 101.4, "Low": 100.2, "Close": 101.1, "Volume": 1200},
            ]
        )

        cleaned = coerce_ohlcv(raw, CleanerConfig(expected_interval_minutes=5, require_vwap=True))

        self.assertEqual(list(cleaned.columns), ["timestamp", "open", "high", "low", "close", "volume", "vwap"])
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(str(cleaned.iloc[0]["timestamp"]), "2026-03-20 09:15:00")
        self.assertEqual(float(cleaned.iloc[0]["close"]), 100.6)

    def test_coerce_ohlcv_rejects_invalid_ohlc(self):
        raw = pd.DataFrame(
            [{"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 99, "low": 101, "close": 100, "volume": 1000}]
        )
        with self.assertRaises(OHLCVValidationError):
            coerce_ohlcv(raw)

    def test_validate_trade_fails_vwap_misalignment_and_bad_rr(self):
        candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99.8, "close": 100.8, "volume": 1000},
                {"timestamp": "2026-03-20 09:20:00", "open": 100.8, "high": 101.0, "low": 99.9, "close": 100.0, "volume": 1000},
                {"timestamp": "2026-03-20 09:25:00", "open": 100.0, "high": 100.2, "low": 99.5, "close": 99.7, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 99.7, "high": 99.9, "low": 99.1, "close": 99.3, "volume": 1000},
            ]
        )
        setup = {"side": "BUY", "entry": 99.3, "stoploss": 98.9, "target": 99.7, "structure_score": 8.0, "rejection_score": 8.0}

        result = validate_trade(setup, candles, ValidationConfig(rr_threshold=2.0, min_score=7.0))

        self.assertEqual(result["decision"], "FAIL")
        self.assertIn("bad_rr", result["reasons"])
        self.assertIn("no_vwap_alignment", result["reasons"])

    def test_execution_guards_block_duplicate_cooldown_and_max_daily_loss(self):
        candidate = {
            "symbol": "NIFTY",
            "side": "BUY",
            "entry": 100,
            "stoploss": 99,
            "target": 102,
            "quantity": 50,
            "timestamp": "2026-03-20 10:00:00",
            "strategy": "BREAKOUT",
        }
        history = [
            {
                "symbol": "NIFTY",
                "side": "BUY",
                "entry": 100,
                "stoploss": 99,
                "target": 102,
                "quantity": 50,
                "timestamp": "2026-03-20 10:00:00",
                "signal_time": "2026-03-20 10:00:00",
                "strategy": "BREAKOUT",
                "pnl": -600.0,
            }
        ]

        result = evaluate_trade_guards(
            candidate,
            history,
            ExecutionGuardConfig(cooldown_minutes=15, max_trades_per_day=3, max_daily_loss=500.0),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("DUPLICATE_TRADE", result["reasons"])
        self.assertIn("COOLDOWN_ACTIVE", result["reasons"])
        self.assertIn("MAX_DAILY_LOSS", result["reasons"])

    def test_run_execution_passes_validation_fields_into_execution_candidates(self):
        request = TradingActionRequest(
            strategy="Breakout",
            symbol="NIFTY",
            timeframe="5m",
            capital=100000.0,
            risk_pct=1.0,
            rr_ratio=2.0,
            mode="Balanced",
            broker_choice="Paper",
            run_requested=True,
        )
        trades = [{"side": "BUY", "entry": 100.0, "stoploss": 99.0, "target": 102.0, "signal_time": "2026-03-20 10:00:00"}]
        candles = pd.DataFrame([
            {"timestamp": "2026-03-20 09:55:00", "open": 99.6, "high": 100.2, "low": 99.5, "close": 100.1, "volume": 1000},
            {"timestamp": "2026-03-20 10:00:00", "open": 100.1, "high": 100.4, "low": 99.8, "close": 100.3, "volume": 1200},
        ])

        captured = {}

        def _capture_candidates(strategy, symbol, candles_frame, output_rows):
            captured["rows"] = output_rows
            return output_rows

        with patch("src.runtime_workflow_service.prepare_candidates_for_execution", side_effect=_capture_candidates):
            with patch("src.runtime_workflow_service.execute_paper_trades", return_value=[]):
                with patch("src.runtime_workflow_service.execution_result_summary", return_value=[]):
                    with patch("src.runtime_workflow_service.refresh_paper_trade_summary", return_value={}):
                        with patch("src.runtime_workflow_service.mirror_output_file"):
                            run_execution(request, trades, candles)

        self.assertEqual(captured["rows"][0]["side"], "BUY")
        self.assertEqual(captured["rows"][0]["entry"], 100.0)

    def test_metrics_and_readiness(self):
        rows = [
            {"pnl": 200.0, "risk_per_unit": 2.0, "quantity": 50, "validation_status": "PASS", "validation_reasons": [], "duplicate_reason": ""},
            {"pnl": -80.0, "risk_per_unit": 2.0, "quantity": 40, "validation_status": "FAIL", "validation_reasons": ["bad_rr"], "duplicate_reason": ""},
        ]
        metrics = compute_trade_metrics(rows)

        self.assertEqual(metrics["total_trades"], 2)
        self.assertIn("bad_rr", metrics["rejection_reasons_count"])
        self.assertEqual(evaluate_production_readiness(metrics), "NOT_READY")
        readiness = evaluate_readiness(rows, [{"reasons": "bad_rr"}])
        self.assertEqual(readiness["verdict"], "NOT_READY")
        self.assertIn("thresholds", readiness)
        self.assertIn("failure_counts", readiness)
        self.assertEqual(
            evaluate_production_readiness(
                {
                    "total_trades": 150,
                    "expectancy": 25.0,
                    "profit_factor": 1.8,
                    "max_drawdown": 5.0,
                    "validation_pass_rate": 70.0,
                    "duplicate_prevention_proven": True,
                }
            ),
            "READY",
        )


if __name__ == "__main__":
    unittest.main()



