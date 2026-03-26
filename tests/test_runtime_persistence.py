import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.runtime_persistence import persist_row, persist_rows
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
                import os
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


if __name__ == '__main__':
    unittest.main()