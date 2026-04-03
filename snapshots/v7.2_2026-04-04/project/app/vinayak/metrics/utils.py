from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
import re

import pandas as pd


TRADE_ALIASES = {
    'qty': 'quantity',
    'profit': 'pnl',
    'net_pnl': 'pnl',
    'grossprofit': 'gross_pnl',
    'gross_profit': 'gross_pnl',
    'entry': 'entry_price',
    'exit': 'exit_price',
    'sl': 'stop_loss',
    'target': 'target_price',
    'signal_timestamp': 'signal_time',
    'executed_at': 'execution_time',
    'trade_status': 'status',
    'execution_status': 'status',
    'strategy_name': 'strategy',
}

CANDLE_ALIASES = {
    'datetime': 'timestamp',
    'date': 'timestamp',
    'time': 'timestamp',
    'o': 'open',
    'h': 'high',
    'l': 'low',
    'c': 'close',
    'vol': 'volume',
}

HEALTH_ALIASES = {
    'datetime': 'timestamp',
    'date': 'timestamp',
    'time': 'timestamp',
}

_BOOLEAN_TRUE = {'1', 'true', 'yes', 'y', 'pass', 'passed', 'ok'}
_BOOLEAN_FALSE = {'0', 'false', 'no', 'n', 'fail', 'failed'}

def _collapse_duplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.columns.is_unique:
        return frame
    collapsed = pd.DataFrame(index=frame.index)
    for column in dict.fromkeys(frame.columns.tolist()):
        if column not in frame.columns:
            continue
        selected = frame.loc[:, frame.columns == column]
        if isinstance(selected, pd.Series):
            collapsed[column] = selected
        else:
            collapsed[column] = selected.bfill(axis=1).iloc[:, 0]
    return collapsed


def normalize_column_name(name: Any) -> str:
    text = re.sub(r'[^a-zA-Z0-9]+', '_', str(name or '').strip().lower())
    text = re.sub(r'_+', '_', text).strip('_')
    return text


def _parse_timestamp(value: Any) -> datetime | pd.NaT:
    if value is None or str(value).strip() == '':
        return pd.NaT
    parsed = pd.to_datetime(value, errors='coerce', utc=True)
    if pd.isna(parsed):
        return pd.NaT
    return pd.Timestamp(parsed).to_pydatetime()


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_bool(value: Any) -> bool | None:
    if value is None or str(value).strip() == '':
        return None
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered in _BOOLEAN_TRUE:
        return True
    if lowered in _BOOLEAN_FALSE:
        return False
    return None


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    if abs(float(denominator or 0.0)) < 1e-12:
        return default
    return float(numerator or 0.0) / float(denominator)


def normalize_score(value: Any) -> float | None:
    numeric = _safe_float(value, None)
    if numeric is None:
        return None
    return max(0.0, min(100.0, float(numeric)))


def _rename_columns(frame: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    renamed = {column: aliases.get(normalize_column_name(column), normalize_column_name(column)) for column in frame.columns}
    return _collapse_duplicate_columns(frame.rename(columns=renamed))


def _combine_timestamp_columns(frame: pd.DataFrame) -> pd.DataFrame:
    columns = set(frame.columns)
    if 'timestamp' not in columns:
        if 'datetime' in columns:
            frame = frame.rename(columns={'datetime': 'timestamp'})
        elif 'date' in columns and 'time' in columns:
            frame['timestamp'] = frame['date'].astype(str).str.strip() + ' ' + frame['time'].astype(str).str.strip()
        elif 'date' in columns:
            frame = frame.rename(columns={'date': 'timestamp'})
        elif 'time' in columns:
            frame = frame.rename(columns={'time': 'timestamp'})
    return frame


def _ensure_datetime(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].apply(_parse_timestamp)
    return frame


def _ensure_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors='coerce')
    return frame


def _ensure_boolean(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].apply(_coerce_bool)
    return frame


def _coerce_frame(records: Any) -> pd.DataFrame:
    if isinstance(records, pd.DataFrame):
        return records.copy()
    return pd.DataFrame(records or [])


def coerce_trade_records(records: Any, *, deduplicate: bool = True) -> pd.DataFrame:
    frame = _coerce_frame(records)
    if frame.empty:
        return pd.DataFrame(columns=[
            'trade_id', 'symbol', 'strategy', 'side', 'entry_time', 'exit_time', 'entry_price', 'exit_price',
            'stop_loss', 'target_price', 'quantity', 'pnl', 'gross_pnl', 'fees', 'slippage', 'status', 'execution_mode',
            'signal_time', 'execution_time', 'validation_passed', 'rejection_reason', 'zone_score', 'vwap_alignment',
            'adx_value', 'trend_ok', 'volatility_ok', 'chop_ok', 'duplicate_blocked', 'retest_confirmed',
            'move_away_score', 'freshness_score', 'rejection_strength', 'structure_clarity'
        ])
    frame.columns = [normalize_column_name(col) for col in frame.columns]
    frame = _rename_columns(frame, TRADE_ALIASES)
    frame = _combine_timestamp_columns(frame)
    frame = _ensure_datetime(frame, ['entry_time', 'exit_time', 'signal_time', 'execution_time', 'timestamp'])
    frame = _ensure_numeric(frame, [
        'entry_price', 'exit_price', 'stop_loss', 'target_price', 'quantity', 'pnl', 'gross_pnl', 'fees', 'slippage',
        'zone_score', 'adx_value', 'move_away_score', 'freshness_score', 'rejection_strength', 'structure_clarity'
    ])
    frame = _ensure_boolean(frame, ['validation_passed', 'vwap_alignment', 'trend_ok', 'volatility_ok', 'chop_ok', 'duplicate_blocked', 'retest_confirmed'])
    if 'gross_pnl' not in frame.columns:
        frame['gross_pnl'] = frame.get('pnl')
    if 'pnl' not in frame.columns:
        frame['pnl'] = frame.get('gross_pnl')
    if 'trade_id' not in frame.columns:
        frame['trade_id'] = ''
    if 'symbol' not in frame.columns:
        frame['symbol'] = 'UNKNOWN'
    if 'strategy' not in frame.columns:
        frame['strategy'] = 'UNKNOWN'
    if 'side' not in frame.columns:
        frame['side'] = ''
    if 'status' not in frame.columns:
        frame['status'] = ''
    if 'execution_mode' not in frame.columns:
        frame['execution_mode'] = 'paper'
    if 'entry_time' not in frame.columns:
        if 'timestamp' in frame.columns:
            frame['entry_time'] = frame['timestamp']
        elif 'signal_time' in frame.columns:
            frame['entry_time'] = frame['signal_time']
        else:
            frame['entry_time'] = pd.NaT
    if 'rejection_reason' not in frame.columns and 'validation_reasons' in frame.columns:
        frame['rejection_reason'] = frame['validation_reasons'].apply(lambda value: ', '.join(value) if isinstance(value, list) else str(value or ''))
    frame['trade_id'] = frame['trade_id'].fillna('').astype(str)
    frame['symbol'] = frame['symbol'].fillna('UNKNOWN').astype(str).str.upper()
    frame['strategy'] = frame['strategy'].fillna('UNKNOWN').astype(str)
    frame['side'] = frame['side'].fillna('').astype(str).str.upper()
    frame['execution_mode'] = frame['execution_mode'].fillna('paper').astype(str).str.lower()
    for numeric_column in ['entry_price', 'exit_price', 'stop_loss', 'target_price', 'quantity']:
        if numeric_column in frame.columns:
            frame[numeric_column] = frame[numeric_column].where(frame[numeric_column].isna() | (frame[numeric_column] >= 0))
    dedupe_key = frame['trade_id'].astype(str).str.strip()
    has_trade_id = dedupe_key.ne('')
    frame = frame.assign(_dedupe_key=dedupe_key)
    frame.loc[~has_trade_id, '_dedupe_key'] = (
        frame.loc[~has_trade_id, 'symbol'].astype(str)
        + '|'
        + frame.loc[~has_trade_id, 'side'].astype(str)
        + '|'
        + frame.loc[~has_trade_id, 'entry_time'].astype(str)
    )
    if deduplicate:
        frame = frame.drop_duplicates(subset=['_dedupe_key'], keep='last')
    frame = frame.drop(columns=['_dedupe_key'])
    return frame.reset_index(drop=True)


def coerce_candle_records(records: Any) -> pd.DataFrame:
    frame = _coerce_frame(records)
    if frame.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'rsi', 'adx', 'macd'])
    frame.columns = [normalize_column_name(col) for col in frame.columns]
    frame = _rename_columns(frame, CANDLE_ALIASES)
    frame = _combine_timestamp_columns(frame)
    frame = _ensure_datetime(frame, ['timestamp'])
    frame = _ensure_numeric(frame, ['open', 'high', 'low', 'close', 'volume', 'vwap', 'rsi', 'adx', 'macd'])
    required = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f'Missing OHLCV columns: {missing}')
    frame = frame.dropna(subset=['timestamp'])
    invalid = (
        frame['open'].isna() | frame['high'].isna() | frame['low'].isna() | frame['close'].isna() | frame['volume'].isna()
        | (frame['open'] <= 0) | (frame['high'] <= 0) | (frame['low'] <= 0) | (frame['close'] <= 0) | (frame['volume'] < 0)
        | (frame['high'] < frame['low']) | (frame['high'] < frame['open']) | (frame['high'] < frame['close'])
        | (frame['low'] > frame['open']) | (frame['low'] > frame['close'])
    )
    frame = frame.loc[~invalid].drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(drop=True)
    return frame


def coerce_health_snapshots(records: Any) -> pd.DataFrame:
    frame = _coerce_frame(records)
    if frame.empty:
        return pd.DataFrame(columns=['timestamp', 'data_latency_ms', 'api_latency_ms', 'signal_generation_success', 'execution_success', 'pipeline_ok', 'telegram_ok', 'broker_ok', 'error_message'])
    frame.columns = [normalize_column_name(col) for col in frame.columns]
    frame = _rename_columns(frame, HEALTH_ALIASES)
    frame = _combine_timestamp_columns(frame)
    frame = _ensure_datetime(frame, ['timestamp'])
    frame = _ensure_numeric(frame, ['data_latency_ms', 'api_latency_ms'])
    frame = _ensure_boolean(frame, ['signal_generation_success', 'execution_success', 'pipeline_ok', 'telegram_ok', 'broker_ok'])
    frame = frame.dropna(subset=['timestamp']).drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(drop=True)
    return frame


def closed_trades_only(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    statuses = trades.get('status', pd.Series([''] * len(trades), index=trades.index)).astype(str).str.upper()
    is_closed = trades.get('exit_time', pd.Series([pd.NaT] * len(trades), index=trades.index)).notna() | statuses.isin({'CLOSED', 'EXITED'})
    return trades.loc[is_closed].copy()


def utc_now() -> datetime:
    return datetime.now(UTC)



