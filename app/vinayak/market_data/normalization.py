from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from vinayak.domain.exceptions import DataNormalizationError, MissingRequiredColumnError, TimestampParseError


CANONICAL_OHLCV_COLUMNS = ('timestamp', 'open', 'high', 'low', 'close', 'volume')
_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    'timestamp': ('timestamp', 'time', 'datetime', 'date', 'ts', 'candle_time'),
    'open': ('open', 'o', 'open_price'),
    'high': ('high', 'h', 'high_price'),
    'low': ('low', 'l', 'low_price'),
    'close': ('close', 'c', 'close_price', 'last_price', 'ltp'),
    'volume': ('volume', 'vol', 'v', 'trade_volume'),
}


@dataclass(frozen=True, slots=True)
class OhlcvNormalizationConfig:
    assume_timezone: str = 'UTC'
    drop_invalid_rows: bool = True
    sort_ascending: bool = True
    deduplicate_keep: str = 'last'


def _normalize_column_name(value: object) -> str:
    return str(value or '').strip().lower().replace('-', '_').replace(' ', '_')


def _coalesce_duplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    output = pd.DataFrame(index=frame.index)
    seen: set[str] = set()
    for column in frame.columns:
        name = str(column)
        if name in seen:
            continue
        seen.add(name)
        selection = frame.loc[:, name]
        if isinstance(selection, pd.DataFrame):
            output[name] = selection.bfill(axis=1).iloc[:, 0]
        else:
            output[name] = selection
    return output


def _rename_to_canonical(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [_normalize_column_name(column) for column in normalized.columns]
    normalized = _coalesce_duplicate_columns(normalized)

    rename_map: dict[str, str] = {}
    for canonical, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            candidate = _normalize_column_name(alias)
            if candidate in normalized.columns:
                rename_map[candidate] = canonical
                break
    normalized = normalized.rename(columns=rename_map)
    missing = [column for column in CANONICAL_OHLCV_COLUMNS if column not in normalized.columns]
    if missing:
        raise MissingRequiredColumnError(f'Missing required OHLCV columns: {missing}')
    return normalized


def _parse_timestamps(series: pd.Series, assume_timezone: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors='coerce', utc=False)
    if parsed.isna().all():
        raise TimestampParseError('All timestamps failed to parse.')
    if getattr(parsed.dt, 'tz', None) is None:
        parsed = parsed.dt.tz_localize(assume_timezone, nonexistent='NaT', ambiguous='NaT')
    parsed = parsed.dt.tz_convert('UTC')
    if parsed.isna().any():
        raise TimestampParseError('One or more timestamps failed to parse.')
    return parsed


def _coerce_numeric(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in ('open', 'high', 'low', 'close', 'volume'):
        normalized[column] = pd.to_numeric(normalized[column], errors='coerce')
        if normalized[column].isna().all():
            raise DataNormalizationError(f'Column {column} could not be converted to numeric.')
    return normalized


def normalize_ohlcv_frame(
    rows: Any,
    *,
    config: OhlcvNormalizationConfig | None = None,
) -> pd.DataFrame:
    active_config = config or OhlcvNormalizationConfig()
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows or [])
    if frame.empty:
        raise DataNormalizationError('Cannot normalize empty OHLCV payload.')

    normalized = _rename_to_canonical(frame)
    normalized['timestamp'] = _parse_timestamps(normalized['timestamp'], active_config.assume_timezone)
    normalized = _coerce_numeric(normalized)

    invalid_mask = (
        normalized['timestamp'].isna()
        | normalized[['open', 'high', 'low', 'close', 'volume']].isna().any(axis=1)
        | (normalized[['open', 'high', 'low', 'close']] <= 0).any(axis=1)
        | (normalized['volume'] < 0)
        | (normalized['high'] < normalized['low'])
        | (normalized['high'] < normalized[['open', 'close']].max(axis=1))
        | (normalized['low'] > normalized[['open', 'close']].min(axis=1))
    )
    if invalid_mask.any() and not active_config.drop_invalid_rows:
        bad_rows = normalized.index[invalid_mask].tolist()
        raise DataNormalizationError(f'Invalid OHLCV rows detected at indices {bad_rows}')
    normalized = normalized.loc[~invalid_mask].copy()
    if normalized.empty:
        raise DataNormalizationError('All OHLCV rows were rejected during normalization.')

    normalized = normalized.drop_duplicates(subset=['timestamp'], keep=active_config.deduplicate_keep)
    normalized = normalized.sort_values('timestamp', ascending=active_config.sort_ascending).reset_index(drop=True)
    return normalized.loc[:, list(CANONICAL_OHLCV_COLUMNS)].copy()


__all__ = ['CANONICAL_OHLCV_COLUMNS', 'OhlcvNormalizationConfig', 'normalize_ohlcv_frame']
