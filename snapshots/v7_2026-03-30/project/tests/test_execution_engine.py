<<<<<<<< HEAD:snapshots/v7_2026-03-30/project/tests/test_execution_engine.py
﻿import csv
import io
import json
import os
import tempfile
========
﻿import tempfile
>>>>>>>> a91e444 ( modifyed with ltp verson2):tests/test_execution_engine.py
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
<<<<<<<< HEAD:snapshots/v7_2026-03-30/project/tests/test_execution_engine.py
            out = Path(td) / 'live.csv'
            out.write_text(
                'strategy,symbol,signal_time,side,price,quantity,execution_type,execution_status,executed_at_utc,broker_order_id,broker_status\n'
                'BREAKOUT,NIFTY,2026-03-06 09:30:00,BUY,100,65,LIVE,SENT,2026-03-06 09:30:00,ORD123,TRANSIT\n',
                encoding='utf-8',
            )
            rows = reconcile_live_trades(out, broker_client=broker, broker_name='DHAN')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['execution_status'], 'FILLED')
            self.assertEqual(rows[0]['broker_status'], 'TRADED')
            self.assertEqual(rows[0]['reconciliation_status'], 'RECONCILED')
            self.assertEqual(broker.fetch_calls, 1)
            saved = out.read_text(encoding='utf-8')
            self.assertIn('FILLED', saved)
            self.assertIn('RECONCILED', saved)

    def test_reconcile_live_positions_compares_expected_vs_broker(self):
        broker = _StubBrokerClient()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            out.write_text(
                'strategy,symbol,signal_time,side,quantity,execution_type,execution_status,broker_order_id\n'
                'BREAKOUT,NIFTY,2026-03-06 09:30:00,BUY,65,LIVE,FILLED,ORD123\n'
                'BREAKOUT,BANKNIFTY,2026-03-06 10:00:00,SELL,25,LIVE,FILLED,ORD124\n',
                encoding='utf-8',
            )
            rows = reconcile_live_positions(out, broker_client=broker, broker_name='DHAN')
            self.assertEqual(len(rows), 2)
            by_symbol = {row['symbol']: row for row in rows}
            self.assertEqual(by_symbol['NIFTY']['expected_net_qty'], 65)
            self.assertEqual(by_symbol['NIFTY']['broker_net_qty'], 65)
            self.assertEqual(by_symbol['NIFTY']['position_match'], 'YES')
            self.assertEqual(by_symbol['BANKNIFTY']['expected_net_qty'], -25)
            self.assertEqual(by_symbol['BANKNIFTY']['broker_net_qty'], -50)
            self.assertEqual(by_symbol['BANKNIFTY']['position_match'], 'NO')
            self.assertEqual(by_symbol['BANKNIFTY']['qty_delta'], -25)

    def test_live_trading_unlock_after_30_days(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-01-01 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'x'}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'dhan_paper.csv'
            execute_paper_trades(candidates, out)

            lines = out.read_text(encoding='utf-8').splitlines()
            header = lines[0]
            row = lines[1].split(',')
            cols = header.split(',')
            idx = cols.index('executed_at_utc')
            row[idx] = '2026-01-01 00:00:00'
            out.write_text('\n'.join([header, ','.join(row)]) + '\n', encoding='utf-8')

            unlocked, days, unlock_date = live_trading_unlock_status(
                out,
                min_days=30,
                now_utc=datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC),
            )
            self.assertTrue(unlocked)
            self.assertGreaterEqual(days, 30)
            self.assertIn('2026-01-31', unlock_date)



    def test_validate_candidate_rejects_low_risk_reward_trade(self):
        ok, reason, normalized = validate_candidate(
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 65,
                'stop_loss': 99.0,
                'target_price': 101.0,
            }
        )
        self.assertFalse(ok)
        self.assertEqual(reason, 'RISK_REWARD_TOO_LOW')
        self.assertEqual(normalized['risk_reward_ratio'], 1.0)

    def test_validate_dhan_preflight_blocks_missing_security_map(self):
        ok, reason, enriched = validate_dhan_preflight(
            {
                'strategy': 'BREAKOUT',
                'symbol': '^NSEI',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 75,
            },
            None,
        )
        self.assertFalse(ok)
        self.assertEqual(reason, 'DHAN_SECURITY_MAP_MISSING')
        self.assertEqual(enriched['symbol'], '^NSEI')

    def test_execute_live_trades_blocks_when_dhan_preflight_fails(self):
        broker = _StubBrokerClient()
        candidates = [
            {
                'strategy': 'BREAKOUT',
                'symbol': '^NSEI',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 75,
                'option_type': 'CE',
                'strike_price': 22500,
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            rows = execute_live_trades(
                candidates,
                out,
                broker_client=broker,
                broker_name='DHAN',
                security_map={},
                optimizer_report_path=self._write_optimizer_report(td),
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['execution_status'], 'BLOCKED')
        self.assertEqual(rows[0]['blocked_reason'], 'DHAN_SECURITY_MAP_MISSING')
        self.assertEqual(rows[0]['broker_status'], 'DHAN_PREFLIGHT')
        self.assertEqual(broker.calls, 0)

    def test_execute_live_trades_resolves_dhan_security_metadata(self):
        broker = _StubBrokerClient()
        security_map = {
            "NIFTY": {
                "security_id": "1001",
                "trading_symbol": "NIFTY",
                "exchange_segment": "IDX_I",
                "instrument_type": "INDEX",
                "underlying_symbol": "NIFTY",
            },
            "__meta__": {
                "by_security_id": {
                    "1001": {
                        "security_id": "1001",
                        "trading_symbol": "NIFTY",
                        "exchange_segment": "IDX_I",
                        "instrument_type": "INDEX",
                        "underlying_symbol": "NIFTY",
                    },
                    "2001": {
                        "security_id": "2001",
                        "trading_symbol": "NIFTY 2026-03-26 22500 CE",
                        "exchange_segment": "NSE_FNO",
                        "instrument_type": "OPTIDX",
                        "underlying_symbol": "NIFTY",
                        "expiry_date": "2026-03-26",
                        "strike_price": "22500",
                        "option_type": "CE",
                        "product_type": "INTRADAY",
                        "order_type": "MARKET",
                    },
                },
                "by_underlying": {
                    "NIFTY": [
                        {
                            "security_id": "1001",
                            "trading_symbol": "NIFTY",
                            "exchange_segment": "IDX_I",
                            "instrument_type": "INDEX",
                            "underlying_symbol": "NIFTY",
                        },
                        {
                            "security_id": "2001",
                            "trading_symbol": "NIFTY 2026-03-26 22500 CE",
                            "exchange_segment": "NSE_FNO",
                            "instrument_type": "OPTIDX",
                            "underlying_symbol": "NIFTY",
                            "expiry_date": "2026-03-26",
                            "strike_price": "22500",
                            "option_type": "CE",
                            "product_type": "INTRADAY",
                            "order_type": "MARKET",
                        },
                    ]
                },
                "option_index": {
                    ("NIFTY", "2026-03-26", "22500", "CE"): {
                        "security_id": "2001",
                        "trading_symbol": "NIFTY 2026-03-26 22500 CE",
                        "exchange_segment": "NSE_FNO",
                        "instrument_type": "OPTIDX",
                        "underlying_symbol": "NIFTY",
                        "expiry_date": "2026-03-26",
                        "strike_price": "22500",
                        "option_type": "CE",
                        "product_type": "INTRADAY",
                        "order_type": "MARKET",
                    }
                },
            },
        }
        candidates = [
            {
                'strategy': 'BREAKOUT',
                'symbol': '^NSEI',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 75,
                'option_expiry': '2026-03-26',
                'strike_price': 22500,
                'option_type': 'CE',
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            rows = execute_live_trades(candidates, out, broker_client=broker, broker_name='DHAN', security_map=security_map, optimizer_report_path=self._write_optimizer_report(td), order_history_path=Path(td) / 'order_history.csv')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['execution_status'], 'SENT')
            self.assertEqual(rows[0]['data_symbol'], '^NSEI')
            self.assertEqual(rows[0]['trade_symbol'], 'NIFTY')
            self.assertEqual(rows[0]['security_id'], '2001')
            self.assertEqual(rows[0]['exchange_segment'], 'NSE_FNO')
            self.assertEqual(rows[0]['instrument_type'], 'OPTIDX')
            self.assertEqual(rows[0]['option_expiry'], '2026-03-26')
            self.assertEqual(rows[0]['broker_order_id'], 'ORD123')
    def test_execute_paper_trades_derives_trade_levels_for_legacy_candidate(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "legacy_candidate"}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            result = execute_paper_trades(candidates, out)
            self.assertEqual(result.executed_count, 1)
            self.assertGreater(float(result[0]["stop_loss"]), 0.0)
            self.assertGreater(float(result[0]["target_price"]), float(result[0]["price"]))

    def test_execute_paper_trades_skips_duplicate_batch_trade(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "x", "stop_loss": 99.0, "target_price": 102.0},
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "x", "stop_loss": 99.0, "target_price": 102.0},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "executed.csv"
            result = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.skipped_count, 1)
            self.assertEqual(result.skipped_rows[0]["duplicate_reason"], "DUPLICATE_BATCH_TRADE")

    def test_execute_live_trades_blocks_when_optimizer_gate_fails(self):
        broker = _StubBrokerClient()
        candidates = [
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 65,
                'reason': 'x',
                'security_id': '12345',
            }
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            optimizer_report = self._write_optimizer_report(
                td,
                deployment_ready='NO',
                deployment_blockers='NEGATIVE_EXPECTANCY',
            )
            rows = execute_live_trades(
                candidates,
                out,
                broker_client=broker,
                broker_name='DHAN',
                security_map={},
                optimizer_report_path=optimizer_report,
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['execution_status'], 'BLOCKED')
        self.assertEqual(rows[0]['blocked_reason'], 'OPTIMIZER_GATE_BLOCKED')
        self.assertEqual(rows[0]['broker_status'], 'OPTIMIZER_GATE')
        self.assertIn('NEGATIVE_EXPECTANCY', rows[0]['broker_message'])
        self.assertEqual(broker.calls, 0)
    def test_execute_paper_trades_skips_duplicate_signal_key_same_candle(self):
        candidates = [
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "a", "stop_loss": 99.0, "target_price": 102.0},
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.2, "quantity": 65, "reason": "b", "stop_loss": 99.1, "target_price": 102.4},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.skipped_count, 1)
            self.assertEqual(result.skipped_rows[0]['duplicate_reason'], 'DUPLICATE_SIGNAL_KEY')

    def test_execute_paper_trades_enforces_duplicate_signal_cooldown(self):
        candidates = [
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "a", "stop_loss": 99.0, "target_price": 102.0, "duplicate_signal_cooldown_bars": 2},
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:05:00", "side": "BUY", "price": 100.4, "quantity": 65, "reason": "b", "stop_loss": 99.3, "target_price": 102.6, "duplicate_signal_cooldown_bars": 2},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.skipped_count, 1)
            self.assertEqual(result.skipped_rows[0]['duplicate_reason'], 'DUPLICATE_SIGNAL_COOLDOWN')

    def test_execute_paper_trades_blocks_when_max_open_trades_hit(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "seed", "stop_loss": 99.0, "target_price": 102.0},
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:10:00", "side": "SELL", "price": 99.0, "quantity": 65, "reason": "next", "stop_loss": 100.0, "target_price": 97.0},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            first = execute_paper_trades([candidates[0]], out, deduplicate=True, max_open_trades=1)
            second = execute_paper_trades([candidates[1]], out, deduplicate=True, max_open_trades=1)
            self.assertEqual(first.executed_count, 1)
            self.assertEqual(second.blocked_count, 1)
            self.assertEqual(second.blocked_rows[0]['blocked_reason'], 'MAX_OPEN_TRADES')

    def test_execute_paper_trades_uses_strategy_default_trade_cap(self):
        candidates = [
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "a", "stop_loss": 99.0, "target_price": 102.0},
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 11:00:00", "side": "SELL", "price": 101.0, "quantity": 65, "reason": "b", "stop_loss": 102.0, "target_price": 99.0},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.blocked_count, 1)
            self.assertEqual(result.blocked_rows[0]['blocked_reason'], 'MAX_TRADES_PER_DAY')

    def test_execute_paper_trades_uses_strategy_default_cooldown_minutes(self):
        candidates = [
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "quantity": 65, "reason": "a", "stop_loss": 99.0, "target_price": 102.0},
            {"strategy": "DEMAND_SUPPLY", "symbol": "NIFTY", "timeframe": "5m", "signal_time": "2026-03-06 10:10:00", "side": "SELL", "price": 100.5, "quantity": 65, "reason": "b", "stop_loss": 101.5, "target_price": 98.5},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out, deduplicate=True, max_trades_per_day=5)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.skipped_count, 1)
            self.assertEqual(result.skipped_rows[0]['duplicate_reason'], 'DUPLICATE_SIGNAL_COOLDOWN')
<<<<<<< HEAD:snapshots/v7_2026-03-30/project/tests/test_execution_engine.py
    def test_execute_paper_trades_writes_structured_validation_and_execution_logs(self):
        candidates = [
            {"strategy": "BREAKOUT", "symbol": "NIFTY", "signal_time": "2026-03-06 10:00:00", "side": "BUY", "price": 100.0, "reason": "missing quantity", "stop_loss": 99.0, "target_price": 102.0},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            execution_log = Path(td) / 'execution.log'
            rejection_log = Path(td) / 'rejections.log'
            with patch.object(execution_engine, 'EXECUTION_LOG_PATH', execution_log), patch.object(execution_engine, 'REJECTIONS_LOG_PATH', rejection_log):
                result = execute_paper_trades(candidates, out)

            self.assertEqual(result.skipped_count, 1)
            execution_events = [json.loads(line) for line in execution_log.read_text(encoding='utf-8').splitlines() if line.strip()]
            rejection_events = [json.loads(line) for line in rejection_log.read_text(encoding='utf-8').splitlines() if line.strip()]
            self.assertEqual(execution_events[0]['event'], 'execution_start')
            self.assertEqual(execution_events[-1]['event'], 'execution_complete')
            self.assertEqual(execution_events[-1]['skipped_count'], 1)
            self.assertEqual(rejection_events[0]['event'], 'trade_rejected')
            self.assertEqual(rejection_events[0]['reason'], 'MISSING_QUANTITY')
            self.assertEqual(rejection_events[0]['category'], 'validation')

    def test_execute_live_trades_writes_structured_error_log_on_broker_failure(self):
        candidates = [
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 65,
                'reason': 'x',
                'security_id': '12345',
                'stop_loss': 99.0,
                'target_price': 102.0,
            }
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            error_log = Path(td) / 'errors.log'
            broker_log = Path(td) / 'broker.log'
            with patch.object(execution_engine, 'ERRORS_LOG_PATH', error_log), patch.object(execution_engine, 'BROKER_LOG_PATH', broker_log):
                result = execute_live_trades(
                    candidates,
                    out,
                    broker_client=_FailingBrokerClient(),
                    broker_name='TEST',
                    live_enabled=True,
                    security_map={},
                    optimizer_report_path=self._write_optimizer_report(td),
                )

            self.assertEqual(result.error_count, 1)
            error_events = [json.loads(line) for line in error_log.read_text(encoding='utf-8').splitlines() if line.strip()]
            broker_events = [json.loads(line) for line in broker_log.read_text(encoding='utf-8').splitlines() if line.strip()]
            self.assertEqual(error_events[0]['event'], 'broker_execution_error')
            self.assertEqual(error_events[0]['error_type'], 'RuntimeError')
            self.assertEqual(error_events[0]['error_message'], 'network down')
            self.assertEqual(broker_events[0]['event'], 'broker_order_routed')
            self.assertEqual(broker_events[-1]['event'], 'broker_order_result')
========
            path = Path(td) / 'missing.csv'
            live_enabled, trade_count, reason = live_trading_unlock_status(path)
        self.assertFalse(live_enabled)
        self.assertEqual(trade_count, 0)
        self.assertEqual(reason, '')
>>>>>>>> a91e444 ( modifyed with ltp verson2):tests/test_execution_engine.py


=======
>>>>>>> fed8576 ( modifyed with ltp verson2):tests/test_execution_engine.py
if __name__ == '__main__':

    unittest.main()
<<<<<<<< HEAD:snapshots/v7_2026-03-30/project/tests/test_execution_engine.py
<<<<<<< HEAD:snapshots/v7_2026-03-30/project/tests/test_execution_engine.py
=======








>>>>>>> fed8576 ( modifyed with ltp verson2):tests/test_execution_engine.py
========


>>>>>>>> a91e444 ( modifyed with ltp verson2):tests/test_execution_engine.py
