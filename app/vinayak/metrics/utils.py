from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
import ast
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


def _parse_list_like(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or '').strip()
    if not text:
        return []
    if text.startswith('[') and text.endswith(']'):
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return [part.strip() for part in text.split(',') if part.strip()]


def _parse_dict_like(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or '').strip()
    if not text:
        return {}
    if text.startswith('{') and text.endswith('}'):
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            parsed = None
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def _infer_strict_score(frame: pd.DataFrame) -> pd.Series:
    existing = pd.to_numeric(frame.get('strict_validation_score', pd.Series([None] * len(frame), index=frame.index)), errors='coerce')
    validation_score = pd.to_numeric(frame.get('validation_score', frame.get('score', pd.Series([None] * len(frame), index=frame.index))), errors='coerce')
    validation_status = frame.get('validation_status', pd.Series([''] * len(frame), index=frame.index)).fillna('').astype(str).str.upper()
    rejection_reason = frame.get('rejection_reason', pd.Series([''] * len(frame), index=frame.index)).fillna('').astype(str).str.strip()

    inferred = existing.copy()
    small_score = validation_score.where(validation_score.le(10.0))
    large_score = (validation_score / 10.0).where(validation_score.gt(10.0))
    inferred = inferred.fillna(small_score)
    inferred = inferred.fillna(large_score)
    inferred = inferred.where(inferred.notna(), 7.0)
    inferred = inferred.where(~(validation_status.eq('FAIL') & rejection_reason.ne('')), inferred.clip(upper=6.0))
    inferred = inferred.where(~(validation_status.eq('PASS') & rejection_reason.eq('')), inferred.clip(lower=7.0))
    return inferred.fillna(0.0).round().astype(int)


def _backfill_strict_trade_fields(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame

    if 'validation_reasons' in frame.columns:
        frame['validation_reasons'] = frame['validation_reasons'].apply(_parse_list_like)
    else:
        frame['validation_reasons'] = [[] for _ in range(len(frame))]

    if 'rejection_reason' not in frame.columns:
        frame['rejection_reason'] = ''
    frame['rejection_reason'] = frame['rejection_reason'].fillna('').astype(str).str.strip()
    if 'duplicate_reason' in frame.columns:
        duplicate_reason = frame['duplicate_reason'].fillna('').astype(str).str.strip()
        frame.loc[frame['rejection_reason'].eq('') & duplicate_reason.ne(''), 'rejection_reason'] = duplicate_reason
    if 'validation_error' in frame.columns:
        validation_error = frame['validation_error'].fillna('').astype(str).str.strip()
        frame.loc[frame['rejection_reason'].eq('') & validation_error.ne(''), 'rejection_reason'] = validation_error
    frame.loc[frame['rejection_reason'].eq(''), 'rejection_reason'] = frame.loc[frame['rejection_reason'].eq(''), 'validation_reasons'].apply(lambda value: ', '.join(value) if isinstance(value, list) else str(value or ''))

    frame['strict_validation_score'] = _infer_strict_score(frame)

    if 'zone_score_components' in frame.columns:
        frame['zone_score_components'] = frame['zone_score_components'].apply(_parse_dict_like)
    else:
        frame['zone_score_components'] = [{} for _ in range(len(frame))]

    component_columns = [
        'zone_score',
        'freshness_score',
        'move_away_score',
        'rejection_strength',
        'structure_clarity',
        'retest_score',
        'validation_score',
    ]
    def _build_components(row: pd.Series) -> dict[str, Any]:
        existing = dict(row.get('zone_score_components', {}) or {})
        for column in component_columns:
            value = row.get(column)
            numeric = _safe_float(value, None)
            if numeric is not None and column not in existing:
                existing[column] = numeric
        if 'strict_validation_score' not in existing:
            existing['strict_validation_score'] = int(row.get('strict_validation_score', 0) or 0)
        return existing
    frame['zone_score_components'] = frame.apply(_build_components, axis=1)

    if 'validation_log' in frame.columns:
        frame['validation_log'] = frame['validation_log'].apply(_parse_dict_like)
    else:
        frame['validation_log'] = [{} for _ in range(len(frame))]

    def _build_validation_log(row: pd.Series) -> dict[str, Any]:
        existing = dict(row.get('validation_log', {}) or {})
        existing.setdefault('rejection_reason', str(row.get('rejection_reason', '') or ''))
        existing.setdefault('validation_reasons', list(row.get('validation_reasons', []) or []))
        existing.setdefault('strict_validation_score', int(row.get('strict_validation_score', 0) or 0))
        existing.setdefault('zone_score_components', dict(row.get('zone_score_components', {}) or {}))
        return existing
    frame['validation_log'] = frame.apply(_build_validation_log, axis=1)

    if 'execution_allowed' not in frame.columns:
        frame['execution_allowed'] = pd.Series([None] * len(frame), index=frame.index)
    validation_status = frame.get('validation_status', pd.Series([''] * len(frame), index=frame.index)).fillna('').astype(str).str.upper()
    frame['execution_allowed'] = frame['execution_allowed'].apply(_coerce_bool)
    default_execution_allowed = validation_status.eq('PASS') & frame['rejection_reason'].eq('')
    frame['execution_allowed'] = frame['execution_allowed'].where(frame['execution_allowed'].notna(), default_execution_allowed)
    frame['execution_allowed'] = frame['execution_allowed'].infer_objects(copy=False)
    frame['execution_allowed'] = frame['execution_allowed'].fillna(False).astype(bool)
    return frame


def coerce_trade_records(records: Any, *, deduplicate: bool = True) -> pd.DataFrame:
    frame = _coerce_frame(records)
    if frame.empty:
        return pd.DataFrame(columns=[
            'trade_id', 'symbol', 'strategy', 'side', 'entry_time', 'exit_time', 'entry_price', 'exit_price',
            'stop_loss', 'target_price', 'quantity', 'pnl', 'gross_pnl', 'fees', 'slippage', 'status', 'execution_mode',
            'signal_time', 'execution_time', 'validation_passed', 'rejection_reason', 'zone_score', 'vwap_alignment',
            'adx_value', 'trend_ok', 'volatility_ok', 'chop_ok', 'duplicate_blocked', 'retest_confirmed',
            'move_away_score', 'freshness_score', 'rejection_strength', 'structure_clarity', 'strict_validation_score',
            'zone_score_components', 'validation_log', 'execution_allowed', 'validation_reasons'
        ])
    frame.columns = [normalize_column_name(col) for col in frame.columns]
    frame = _rename_columns(frame, TRADE_ALIASES)
    frame = _combine_timestamp_columns(frame)
    frame = _ensure_datetime(frame, ['entry_time', 'exit_time', 'signal_time', 'execution_time', 'timestamp'])
    frame = _ensure_numeric(frame, [
        'entry_price', 'exit_price', 'stop_loss', 'target_price', 'quantity', 'pnl', 'gross_pnl', 'fees', 'slippage',
        'zone_score', 'adx_value', 'move_away_score', 'freshness_score', 'rejection_strength', 'structure_clarity',
        'validation_score', 'strict_validation_score', 'retest_score'
    ])
    frame = _ensure_boolean(frame, ['validation_passed', 'vwap_alignment', 'trend_ok', 'volatility_ok', 'chop_ok', 'duplicate_blocked', 'retest_confirmed', 'execution_allowed'])
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
    frame = _backfill_strict_trade_fields(frame)
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



