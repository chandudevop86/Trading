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

        CREATE TABLE IF NOT EXISTS runtime_risk_state_current (
            artifact_path TEXT NOT NULL,
            execution_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY(artifact_path, execution_type)
        );

        CREATE TABLE IF NOT EXISTS runtime_risk_state_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artifact_path TEXT NOT NULL,
            execution_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_runtime_risk_state_history_path
        ON runtime_risk_state_history(artifact_path, execution_type, updated_at_utc DESC);
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


def _normalize_text(value: object) -> str:
    return str(value or '').strip().upper()


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == '':
            return 0.0
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _parse_timestamp(text: object) -> datetime | None:
    raw = str(text or '').strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        dt = None
    if dt is None:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _row_is_closed(row: dict[str, Any]) -> bool:
    status = _normalize_text(row.get('trade_status'))
    position_status = _normalize_text(row.get('position_status'))
    execution_status = _normalize_text(row.get('execution_status'))
    return status == 'CLOSED' or position_status == 'CLOSED' or execution_status in {'CLOSED', 'EXITED'}


def _row_is_active(row: dict[str, Any]) -> bool:
    if _row_is_closed(row):
        return False
    status = _normalize_text(row.get('trade_status'))
    if status in {'REVIEWED', 'PENDING_EXECUTION', 'EXECUTED', 'OPEN'}:
        return True
    return _normalize_text(row.get('execution_status')) in {'EXECUTED', 'SENT', 'FILLED'}


def _timeframe_key(row: dict[str, Any]) -> str:
    return str(row.get('timeframe', row.get('interval', '')) or '').strip().lower() or 'na'


def _signal_time_text(row: dict[str, Any]) -> str:
    return str(row.get('signal_time', row.get('entry_time', row.get('timestamp', ''))) or '').strip()


def _duplicate_signal_key(row: dict[str, Any]) -> str:
    strategy = _normalize_text(row.get('strategy', 'TRADE_BOT'))
    symbol = _normalize_text(row.get('symbol', 'UNKNOWN'))
    side = _normalize_text(row.get('side'))
    return '|'.join([strategy, symbol, _timeframe_key(row), _signal_time_text(row), side])


def _cooldown_group_key(row: dict[str, Any]) -> str:
    strategy = _normalize_text(row.get('strategy', 'TRADE_BOT'))
    symbol = _normalize_text(row.get('symbol', 'UNKNOWN'))
    side = _normalize_text(row.get('side'))
    return '|'.join([strategy, symbol, _timeframe_key(row), side])


def _trade_day_key(row: dict[str, Any]) -> str:
    ts = (
        _parse_timestamp(row.get('signal_time'))
        or _parse_timestamp(row.get('entry_time'))
        or _parse_timestamp(row.get('timestamp'))
        or _parse_timestamp(row.get('executed_at_utc'))
    )
    if ts is None:
        return str(row.get('executed_at_utc', '') or '')[:10]
    return ts.strftime('%Y-%m-%d')


def build_execution_risk_state(rows: list[dict[str, Any]], execution_type: str) -> dict[str, Any]:
    normalized_execution_type = _normalize_text(execution_type)
    historical_trade_ids: set[str] = set()
    active_trade_keys: set[str] = set()
    active_duplicate_signal_keys: set[str] = set()
    recent_signal_times: dict[str, str] = {}
    daily_state: dict[str, dict[str, float]] = {}
    open_trade_count = 0

    for raw_row in rows:
        row = dict(raw_row)
        if _normalize_text(row.get('execution_type')) != normalized_execution_type:
            continue

        trade_id = str(row.get('trade_id', '') or '').strip()
        if trade_id:
            historical_trade_ids.add(trade_id)

        if _row_is_active(row):
            trade_key = str(row.get('trade_key', '') or '').strip()
            if trade_key:
                active_trade_keys.add(trade_key)
            duplicate_signal_key = str(row.get('duplicate_signal_key', '') or '').strip() or _duplicate_signal_key(row)
            if duplicate_signal_key:
                active_duplicate_signal_keys.add(duplicate_signal_key)
            open_trade_count += 1

        signal_time = (
            _parse_timestamp(row.get('signal_time'))
            or _parse_timestamp(row.get('entry_time'))
            or _parse_timestamp(row.get('timestamp'))
        )
        if signal_time is not None:
            group_key = _cooldown_group_key(row)
            current = recent_signal_times.get(group_key)
            signal_time_text = signal_time.strftime('%Y-%m-%d %H:%M:%S')
            if current is None or signal_time_text > current:
                recent_signal_times[group_key] = signal_time_text

        if _row_is_closed(row):
            continue
        if _normalize_text(row.get('execution_status')) not in {'EXECUTED', 'SENT', 'FILLED'} and _normalize_text(row.get('trade_status')) not in {'EXECUTED', 'OPEN', 'PENDING_EXECUTION'}:
            continue
        day_key = _trade_day_key(row)
        if not day_key:
            continue
        bucket = daily_state.setdefault(day_key, {'count': 0.0, 'realized_pnl': 0.0})
        bucket['count'] += 1.0
        bucket['realized_pnl'] += _safe_float(row.get('pnl'))

    return {
        'artifact_path': '',
        'execution_type': normalized_execution_type,
        'historical_trade_ids': sorted(historical_trade_ids),
        'active_trade_keys': sorted(active_trade_keys),
        'active_duplicate_signal_keys': sorted(active_duplicate_signal_keys),
        'recent_signal_times': recent_signal_times,
        'open_trade_count': int(open_trade_count),
        'daily_state': daily_state,
    }


def save_execution_risk_state(
    path: Path | str,
    execution_type: str,
    state: dict[str, Any],
    *,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    artifact_path = str(Path(path))
    normalized_state = dict(state)
    normalized_state['artifact_path'] = artifact_path
    normalized_state['execution_type'] = _normalize_text(execution_type)
    payload_json = json.dumps(normalized_state, ensure_ascii=True, sort_keys=True, default=str)
    updated_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute(
                'INSERT INTO runtime_risk_state_history(artifact_path, execution_type, payload_json, updated_at_utc) VALUES (?, ?, ?, ?)',
                (artifact_path, normalized_state['execution_type'], payload_json, updated_at),
            )
            conn.execute(
                '''
                INSERT INTO runtime_risk_state_current(artifact_path, execution_type, payload_json, updated_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(artifact_path, execution_type)
                DO UPDATE SET payload_json = excluded.payload_json, updated_at_utc = excluded.updated_at_utc
                ''',
                (artifact_path, normalized_state['execution_type'], payload_json, updated_at),
            )
    finally:
        conn.close()
    return normalized_state


def refresh_execution_risk_state(
    path: Path | str,
    execution_type: str,
    *,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    rows = load_current_rows(path, db_path=db_path)
    state = build_execution_risk_state(rows, execution_type)
    return save_execution_risk_state(path, execution_type, state, db_path=db_path)


def load_execution_risk_state(
    path: Path | str,
    execution_type: str,
    *,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    artifact_path = str(Path(path))
    normalized_execution_type = _normalize_text(execution_type)
    conn = _connect(db_path)
    try:
        row = conn.execute(
            'SELECT payload_json FROM runtime_risk_state_current WHERE artifact_path = ? AND execution_type = ?',
            (artifact_path, normalized_execution_type),
        ).fetchone()
    finally:
        conn.close()
    if row is not None:
        try:
            payload = json.loads(str(row[0]))
        except Exception:
            payload = None
        if isinstance(payload, dict):
            payload.setdefault('artifact_path', artifact_path)
            payload.setdefault('execution_type', normalized_execution_type)
            payload.setdefault('historical_trade_ids', [])
            payload.setdefault('active_trade_keys', [])
            payload.setdefault('active_duplicate_signal_keys', [])
            payload.setdefault('recent_signal_times', {})
            payload.setdefault('open_trade_count', 0)
            payload.setdefault('daily_state', {})
            return payload
    return refresh_execution_risk_state(path, normalized_execution_type, db_path=db_path)
