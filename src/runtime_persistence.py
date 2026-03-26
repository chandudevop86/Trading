from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

DEFAULT_DB_PATH = Path('data/legacy_runtime.db')


def resolve_db_path(db_path: Path | str | None = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    configured = str(os.getenv('LEGACY_RUNTIME_DB_PATH', '') or '').strip()
    return Path(configured) if configured else DEFAULT_DB_PATH


def artifact_type_for_path(path: Path | str) -> str:
    name = Path(path).name.lower()
    if name in {'trades.csv', 'output.csv', 'live_ohlcv.csv', 'ohlcv.csv'}:
        return 'signals'
    if name in {'executed_trades.csv', 'paper_trading_logs_all.csv', 'live_trading_logs_all.csv'}:
        return 'executions'
    if name in {'order_history.csv', 'paper_order_history.csv'}:
        return 'orders'
    if 'summary' in name or 'report' in name or 'validation' in name or 'expectancy' in name or 'optimizer' in name:
        return 'summaries'
    return 'artifacts'


def _connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    resolved = resolve_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(resolved)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS runtime_batches (
            batch_id TEXT PRIMARY KEY,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            write_mode TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            recorded_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runtime_records_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            record_index INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            recorded_at_utc TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES runtime_batches(batch_id)
        );

        CREATE TABLE IF NOT EXISTS runtime_records_current (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            record_index INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            recorded_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_runtime_batches_path_time
        ON runtime_batches(artifact_path, recorded_at_utc DESC);

        CREATE INDEX IF NOT EXISTS idx_runtime_records_history_path
        ON runtime_records_history(artifact_path);

        CREATE INDEX IF NOT EXISTS idx_runtime_records_current_path
        ON runtime_records_current(artifact_path);
        '''
    )
    conn.commit()


def persist_rows(
    path: Path | str,
    rows: list[dict[str, Any]],
    *,
    write_mode: Literal['replace', 'append'] = 'replace',
    db_path: Path | str | None = None,
) -> None:
    artifact_path = str(Path(path))
    artifact_type = artifact_type_for_path(path)
    recorded_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    batch_id = uuid.uuid4().hex
    normalized_rows = [dict(row) for row in rows]

    conn = _connect(db_path)
    try:
        with conn:
            conn.execute(
                'INSERT INTO runtime_batches(batch_id, artifact_path, artifact_type, write_mode, row_count, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?)',
                (batch_id, artifact_path, artifact_type, write_mode, len(normalized_rows), recorded_at),
            )
            for index, row in enumerate(normalized_rows):
                payload_json = json.dumps(row, ensure_ascii=True, sort_keys=True, default=str)
                conn.execute(
                    'INSERT INTO runtime_records_history(batch_id, artifact_path, artifact_type, record_index, payload_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?)',
                    (batch_id, artifact_path, artifact_type, index, payload_json, recorded_at),
                )
            if write_mode == 'replace':
                conn.execute('DELETE FROM runtime_records_current WHERE artifact_path = ?', (artifact_path,))
                for index, row in enumerate(normalized_rows):
                    payload_json = json.dumps(row, ensure_ascii=True, sort_keys=True, default=str)
                    conn.execute(
                        'INSERT INTO runtime_records_current(artifact_path, artifact_type, record_index, payload_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?)',
                        (artifact_path, artifact_type, index, payload_json, recorded_at),
                    )
            else:
                start_index = conn.execute(
                    'SELECT COALESCE(MAX(record_index) + 1, 0) FROM runtime_records_current WHERE artifact_path = ?',
                    (artifact_path,),
                ).fetchone()[0]
                for offset, row in enumerate(normalized_rows):
                    payload_json = json.dumps(row, ensure_ascii=True, sort_keys=True, default=str)
                    conn.execute(
                        'INSERT INTO runtime_records_current(artifact_path, artifact_type, record_index, payload_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?)',
                        (artifact_path, artifact_type, int(start_index) + offset, payload_json, recorded_at),
                    )
    finally:
        conn.close()


def persist_row(path: Path | str, row: dict[str, Any], *, db_path: Path | str | None = None) -> None:
    persist_rows(path, [dict(row)], write_mode='append', db_path=db_path)


def load_current_rows(path: Path | str, *, db_path: Path | str | None = None) -> list[dict[str, Any]]:
    artifact_path = str(Path(path))
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            'SELECT payload_json FROM runtime_records_current WHERE artifact_path = ? ORDER BY record_index',
            (artifact_path,),
        ).fetchall()
    finally:
        conn.close()
    decoded: list[dict[str, Any]] = []
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if isinstance(payload, dict):
            decoded.append(payload)
    return decoded


def load_latest_batch_rows(path: Path | str, *, db_path: Path | str | None = None) -> list[dict[str, Any]]:
    artifact_path = str(Path(path))
    conn = _connect(db_path)
    try:
        row = conn.execute(
            'SELECT batch_id FROM runtime_batches WHERE artifact_path = ? ORDER BY recorded_at_utc DESC, row_count DESC LIMIT 1',
            (artifact_path,),
        ).fetchone()
        if row is None:
            return []
        batch_id = str(row[0])
        rows = conn.execute(
            'SELECT payload_json FROM runtime_records_history WHERE batch_id = ? ORDER BY record_index',
            (batch_id,),
        ).fetchall()
    finally:
        conn.close()
    decoded: list[dict[str, Any]] = []
    for (payload_json,) in rows:
        try:
            payload = json.loads(payload_json)
        except Exception:
            continue
        if isinstance(payload, dict):
            decoded.append(payload)
    return decoded