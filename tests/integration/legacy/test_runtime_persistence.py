import json
import os
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.execution_engine import _count_open_trades, _load_daily_execution_state, _read_trade_rows, load_active_trade_keys
from src.runtime_persistence import (
    load_current_rows,
    load_execution_risk_state,
    persist_row,
    persist_rows,
    refresh_execution_risk_state,
)
from src.trading_core import write_rows


class TestRuntimePersistence(unittest.TestCase):
    def test_replace_write_persists_current_state(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / 'legacy_runtime.db'
            artifact = base / 'trades.csv'
            rows = [
                {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'entry_price': 101.5},
                {'strategy': 'INDICATOR', 'symbol': 'NIFTY', 'entry_price': 102.0},
            ]

            persist_rows(artifact, rows, write_mode='replace', db_path=db_path)

            conn = sqlite3.connect(db_path)
            try:
                current = conn.execute(
                    'SELECT artifact_type, record_index, payload_json FROM runtime_records_current WHERE artifact_path = ? ORDER BY record_index',
                    (str(artifact),),
                ).fetchall()
                self.assertEqual(len(current), 2)
                self.assertEqual(current[0][0], 'signals')
                self.assertEqual(json.loads(current[1][2])['strategy'], 'INDICATOR')
            finally:
                conn.close()

    def test_append_write_accumulates_order_history(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / 'legacy_runtime.db'
            artifact = base / 'order_history.csv'

            persist_row(artifact, {'order_id': 'A1', 'status': 'EXECUTED'}, db_path=db_path)
            persist_row(artifact, {'order_id': 'A2', 'status': 'EXECUTED'}, db_path=db_path)

            conn = sqlite3.connect(db_path)
            try:
                current_count = conn.execute(
                    'SELECT COUNT(*) FROM runtime_records_current WHERE artifact_path = ?',
                    (str(artifact),),
                ).fetchone()[0]
                history_count = conn.execute(
                    'SELECT COUNT(*) FROM runtime_records_history WHERE artifact_path = ?',
                    (str(artifact),),
                ).fetchone()[0]
                self.assertEqual(current_count, 2)
                self.assertEqual(history_count, 2)
            finally:
                conn.close()

    def test_write_rows_mirrors_csv_to_default_db(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            old_cwd = Path.cwd()
            try:
                os.chdir(base)
                rows = [{'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'entry_price': 100.0}]
                write_rows(Path('data/output.csv'), rows)

                db_path = base / 'data' / 'legacy_runtime.db'
                self.assertTrue(db_path.exists())
                conn = sqlite3.connect(db_path)
                try:
                    current = conn.execute(
                        'SELECT artifact_type, payload_json FROM runtime_records_current WHERE artifact_path = ?',
                        (str(Path('data/output.csv')),),
                    ).fetchone()
                    self.assertIsNotNone(current)
                    self.assertEqual(current[0], 'signals')
                    self.assertEqual(json.loads(current[1])['symbol'], 'NIFTY')
                finally:
                    conn.close()
            finally:
                os.chdir(old_cwd)

    def test_execution_engine_reads_current_rows_from_db_first(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / 'legacy_runtime.db'
            artifact = base / 'executed_trades.csv'
            rows = [
                {'trade_id': 't-1', 'strategy': 'BREAKOUT', 'execution_type': 'PAPER', 'execution_status': 'EXECUTED'}
            ]

            persist_rows(artifact, rows, write_mode='append', db_path=db_path)

            old_env = os.environ.get('LEGACY_RUNTIME_DB_PATH')
            os.environ['LEGACY_RUNTIME_DB_PATH'] = str(db_path)
            try:
                loaded = _read_trade_rows(artifact)
                self.assertEqual(len(loaded), 1)
                self.assertEqual(loaded[0]['trade_id'], 't-1')
            finally:
                if old_env is None:
                    os.environ.pop('LEGACY_RUNTIME_DB_PATH', None)
                else:
                    os.environ['LEGACY_RUNTIME_DB_PATH'] = old_env

    def test_refresh_execution_risk_state_persists_daily_and_open_trade_state(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / 'legacy_runtime.db'
            artifact = base / 'executed_trades.csv'
            rows = [
                {
                    'trade_id': 't-1',
                    'trade_key': 'k-1',
                    'duplicate_signal_key': 'dup-1',
                    'strategy': 'BREAKOUT',
                    'symbol': 'NIFTY',
                    'timeframe': '5m',
                    'signal_time': '2026-03-06 10:00:00',
                    'execution_type': 'PAPER',
                    'execution_status': 'EXECUTED',
                    'trade_status': 'OPEN',
                    'pnl': '-250.5',
                }
            ]
            persist_rows(artifact, rows, write_mode='append', db_path=db_path)

            state = refresh_execution_risk_state(artifact, 'PAPER', db_path=db_path)
            loaded = load_execution_risk_state(artifact, 'PAPER', db_path=db_path)

            self.assertEqual(state['open_trade_count'], 1)
            self.assertEqual(loaded['active_trade_keys'], ['k-1'])
            self.assertEqual(loaded['active_duplicate_signal_keys'], ['dup-1'])
            self.assertIn('2026-03-06', loaded['daily_state'])
            self.assertEqual(loaded['daily_state']['2026-03-06']['count'], 1.0)
            self.assertEqual(loaded['daily_state']['2026-03-06']['realized_pnl'], -250.5)

    def test_execution_risk_helpers_read_persisted_sqlite_snapshot(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / 'legacy_runtime.db'
            artifact = base / 'executed_trades.csv'
            rows = [
                {
                    'trade_id': 't-1',
                    'trade_key': 'trade-key-1',
                    'duplicate_signal_key': 'dup-key-1',
                    'strategy': 'BREAKOUT',
                    'symbol': 'NIFTY',
                    'timeframe': '5m',
                    'signal_time': '2026-03-06 10:00:00',
                    'execution_type': 'PAPER',
                    'execution_status': 'EXECUTED',
                    'trade_status': 'OPEN',
                    'pnl': '-100.0',
                }
            ]
            persist_rows(artifact, rows, write_mode='append', db_path=db_path)
            refresh_execution_risk_state(artifact, 'PAPER', db_path=db_path)

            old_env = os.environ.get('LEGACY_RUNTIME_DB_PATH')
            os.environ['LEGACY_RUNTIME_DB_PATH'] = str(db_path)
            try:
                self.assertEqual(load_active_trade_keys(artifact, 'PAPER'), {'trade-key-1'})
                self.assertEqual(_count_open_trades(artifact, 'PAPER'), 1)
                daily_state = _load_daily_execution_state(artifact, 'PAPER')
                self.assertEqual(daily_state['2026-03-06']['count'], 1.0)
            finally:
                if old_env is None:
                    os.environ.pop('LEGACY_RUNTIME_DB_PATH', None)
                else:
                    os.environ['LEGACY_RUNTIME_DB_PATH'] = old_env


if __name__ == '__main__':
    unittest.main()
