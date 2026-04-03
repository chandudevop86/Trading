import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.execution_engine as execution_engine
from src.execution.contracts import CONTRACT_VERSION, normalize_candidate_contract
from src.execution.guardrails import GuardConfig, check_all_guards
from src.execution.state import TradingState
from src.execution_engine import (
    build_analysis_queue,
    build_execution_candidates,
    default_quantity_for_symbol,
    execute_live_trades,
    execute_paper_trades,
    filter_unlogged_candidates,
    live_trading_unlock_status,
)


class TestExecutionEngine(unittest.TestCase):
    def _strict_candidate(
        self,
        *,
        zone_id: str,
        timestamp: str = '2026-03-06 10:00:00',
        strategy_name: str = 'BREAKOUT',
        setup_type: str = 'BREAKOUT',
        symbol: str = 'NIFTY',
        side: str = 'BUY',
        entry: float = 100.0,
        stop_loss: float = 99.0,
        target: float = 102.0,
        timeframe: str = '5m',
        execution_allowed: bool = True,
        validation_status: str = 'PASS',
        validation_score: float = 8.0,
    ) -> dict[str, object]:
        return normalize_candidate_contract(
            {
                'symbol': symbol,
                'timestamp': timestamp,
                'strategy_name': strategy_name,
                'setup_type': setup_type,
                'zone_id': zone_id,
                'side': side,
                'entry': entry,
                'stop_loss': stop_loss,
                'target': target,
                'quantity': 65,
                'timeframe': timeframe,
                'validation_status': validation_status,
                'validation_score': validation_score,
                'validation_reasons': [] if validation_status == 'PASS' else ['validation_fail'],
                'execution_allowed': execution_allowed,
                'contract_version': CONTRACT_VERSION,
            }
        )

    def test_build_indicator_candidate(self):
        rows = [{'timestamp': '2026-03-06 10:00:00', 'market_signal': 'BULLISH_TREND', 'close': 22350.0}]
        candidates = build_execution_candidates('Indicator (RSI/ADX/MACD+VWAP)', rows, 'NIFTY')
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['side'], 'BUY')
        self.assertEqual(candidates[0]['quantity'], 65)
        self.assertIn('share_price', candidates[0])
        self.assertIn('strike_price', candidates[0])

    def test_default_quantity_for_symbol(self):
        self.assertEqual(default_quantity_for_symbol('NIFTY'), 65)
        self.assertEqual(default_quantity_for_symbol('BANKNIFTY'), 1)

    def test_build_analysis_queue_filters_actionable_candidates(self):
        analyzed = build_analysis_queue(
            [
                {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100},
                {'strategy': 'INDICATOR', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:05:00', 'side': 'HOLD', 'price': 101},
            ],
            analyzed_at_utc='2026-03-06 10:10:00',
        )
        self.assertEqual(len(analyzed), 1)
        self.assertEqual(analyzed[0]['analysis_status'], 'ANALYZED')
        self.assertEqual(analyzed[0]['execution_ready'], 'YES')

    def test_execute_paper_trades_executes_strict_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            candidate = self._strict_candidate(zone_id='NIFTY_BREAKOUT_01')
            result = execute_paper_trades([candidate], out)
            self.assertTrue(out.exists())

        self.assertEqual(result.executed_count, 1)
        self.assertEqual(result.blocked_count, 0)

    def test_execute_paper_trades_blocks_non_strict_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            candidate = {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 65,
            }
            result = execute_paper_trades([candidate], out)

        self.assertEqual(result.executed_count, 0)
        self.assertEqual(result.blocked_count, 1)
        self.assertIn('MISSING_ZONE_ID', result.blocked_rows[0]['reason_codes'])
        self.assertIn('MISSING_VALIDATION_STATUS', result.blocked_rows[0]['reason_codes'])

    def test_execute_paper_trades_blocks_duplicate_zone(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            first = self._strict_candidate(zone_id='NIFTY_BREAKOUT_01')
            second = self._strict_candidate(zone_id='NIFTY_BREAKOUT_01', timestamp='2026-03-06 10:20:00')
            first_result = execute_paper_trades([first], out)
            second_result = execute_paper_trades([second], out)

        self.assertEqual(first_result.executed_count, 1)
        self.assertEqual(second_result.executed_count, 0)
        self.assertEqual(second_result.blocked_count, 1)
        self.assertEqual(second_result.blocked_rows[0]['blocked_reason'], 'DUPLICATE_ZONE')

    def test_execute_paper_trades_blocks_failed_validation(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            candidate = self._strict_candidate(zone_id='NIFTY_BREAKOUT_02', validation_status='FAIL', execution_allowed=False, validation_score=5.0)
            result = execute_paper_trades([candidate], out)

        self.assertEqual(result.executed_count, 0)
        self.assertEqual(result.blocked_count, 1)
        self.assertEqual(result.blocked_rows[0]['blocked_reason'], 'VALIDATION_NOT_PASS')

    def test_filter_unlogged_candidates_skips_logged_strict_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            seed = self._strict_candidate(zone_id='NIFTY_BREAKOUT_01')
            execute_paper_trades([seed], out)
            fresh, skipped = filter_unlogged_candidates([seed], out)

        self.assertEqual(len(fresh), 0)
        self.assertEqual(len(skipped), 1)

    def test_guardrails_block_cooldown_for_same_group(self):
        state = TradingState.from_rows([
            {
                **self._strict_candidate(zone_id='NIFTY_BREAKOUT_01', timestamp='2026-03-06 10:00:00'),
                'trade_id': 'trade-1',
                'execution_status': 'EXECUTED',
            }
        ])
        candidate = self._strict_candidate(zone_id='NIFTY_BREAKOUT_02', timestamp='2026-03-06 10:05:00')
        result = check_all_guards(candidate, state, GuardConfig(cooldown_minutes=15))
        self.assertFalse(result.allowed)
        self.assertIn('COOLDOWN_ACTIVE', result.reasons)

    def test_execute_engine_wrappers_delegate_to_guard_gateway(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            with patch('src.execution.guards.execute_paper_trades') as mock_paper, patch('src.execution.guards.execute_live_trades') as mock_live:
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
                mock_paper.return_value = _Result()
                mock_live.return_value = _Result()
                execution_engine.execute_paper_trades([], out)
                execution_engine.execute_live_trades([], out)

        mock_paper.assert_called_once()
        mock_live.assert_called_once()

    def test_execute_live_trades_uses_canonical_gateway_and_blocks_invalid_contract(self):
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            result = execute_live_trades([
                {
                    'strategy': 'BREAKOUT',
                    'symbol': 'NIFTY',
                    'signal_time': '2026-03-06 10:00:00',
                    'side': 'BUY',
                    'price': 100.0,
                    'quantity': 65,
                }
            ], out)

        self.assertEqual(result.executed_count, 0)
        self.assertEqual(result.blocked_count, 1)
        self.assertIn('MISSING_ZONE_ID', result.blocked_rows[0]['reason_codes'])

    def test_live_trading_unlock_status_handles_missing_log(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'missing.csv'
            live_enabled, trade_count, reason = live_trading_unlock_status(path)
        self.assertFalse(live_enabled)
        self.assertEqual(trade_count, 0)
        self.assertEqual(reason, '')


if __name__ == '__main__':
    unittest.main()


