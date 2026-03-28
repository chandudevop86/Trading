from __future__ import annotations

from typing import Any

import pandas as pd

REQUIRED_OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

_COLUMN_ALIASES = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adj_close": "close",
    "adjclose": "close",
    "volume": "volume",
    "vol": "volume",
}


def _normalize_column_name(column: object) -> str:
    if isinstance(column, tuple):
        parts = [str(part).strip() for part in column if str(part).strip()]
        text = "_".join(parts)
    else:
        text = str(column).strip()
    return text.lower().replace(" ", "_").replace("-", "_")


def _coerce_dataframe(df: Any) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df.copy()
    return pd.DataFrame(df or [])


def _build_timestamp_series(prepared: pd.DataFrame) -> pd.Series | None:
    timestamp_series: pd.Series | None = None
    if "timestamp" in prepared.columns:
        timestamp_series = prepared["timestamp"]
    if "datetime" in prepared.columns:
        candidate = prepared["datetime"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "date" in prepared.columns and "time" in prepared.columns:
        date_text = prepared["date"].astype(str).str.strip()
        time_text = prepared["time"].astype(str).str.strip()
        candidate = date_text + " " + time_text
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "date" in prepared.columns:
        candidate = prepared["date"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    if "time" in prepared.columns:
        candidate = prepared["time"]
        timestamp_series = candidate if timestamp_series is None else timestamp_series.where(timestamp_series.notna(), candidate)
    return timestamp_series


def _base_prepare(df: Any) -> pd.DataFrame:
    prepared = _coerce_dataframe(df)
    if prepared.empty:
        return pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS)

    prepared.columns = [_normalize_column_name(column) for column in prepared.columns]
    prepared = prepared.rename(columns={column: _COLUMN_ALIASES.get(column, column) for column in prepared.columns})
    if prepared.columns.duplicated().any():
        collapsed: dict[str, pd.Series] = {}
        for column in dict.fromkeys(prepared.columns):
            duplicates = prepared.loc[:, prepared.columns == column]
            collapsed[column] = duplicates.apply(lambda row: next((value for value in row if pd.notna(value)), None), axis=1)
        prepared = pd.DataFrame(collapsed)

    timestamp_series = _build_timestamp_series(prepared)
    if timestamp_series is not None:
        prepared["timestamp"] = timestamp_series

    missing_columns = [column for column in REQUIRED_OHLCV_COLUMNS if column not in prepared.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    prepared = prepared.loc[:, REQUIRED_OHLCV_COLUMNS].copy()
    prepared["timestamp"] = pd.to_datetime(prepared["timestamp"], errors="coerce")
    for column in ["open", "high", "low", "close", "volume"]:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared["volume"] = prepared["volume"].fillna(0.0)

    prepared = prepared.dropna(subset=["timestamp", "open", "high", "low", "close"])
    prepared = prepared[
        (prepared["high"] >= prepared["low"])
        & (prepared["high"] >= prepared["open"])
        & (prepared["high"] >= prepared["close"])
        & (prepared["low"] <= prepared["open"])
        & (prepared["low"] <= prepared["close"])
        & (prepared["open"] >= 0)
        & (prepared["high"] >= 0)
        & (prepared["low"] >= 0)
        & (prepared["close"] >= 0)
        & (prepared["volume"] >= 0)
    ]
    prepared = prepared.drop_duplicates(subset=["timestamp"], keep="last")
    prepared = prepared.sort_values("timestamp").reset_index(drop=True)
    return prepared


def _time_block(timestamp: pd.Timestamp) -> str:
    hhmm = timestamp.strftime('%H:%M')
    if '09:15' <= hhmm <= '09:30':
        return 'opening_volatility'
    if '09:30' < hhmm <= '11:00':
        return 'morning'
    if '11:00' < hhmm <= '13:30':
        return 'midday'
    if '13:30' < hhmm <= '15:00':
        return 'afternoon'
    return 'other'


def enrich_trading_data(df: Any, *, expected_interval_minutes: int = 5) -> pd.DataFrame:
    prepared = _base_prepare(df)
    if prepared.empty:
        extra_columns = [
            'range', 'body', 'body_ratio', 'upper_wick', 'lower_wick', 'avg_range', 'avg_volume',
            'volume_ratio', 'vwap', 'above_vwap', 'opening_high', 'opening_low', 'opening_range',
            'day_bias', 'time_block', 'interval_minutes', 'interval_valid', 'gap_flag',
            'intraday_range', 'intraday_volatility_ratio', 'higher_high', 'higher_low', 'lower_high', 'lower_low',
            'trend_strength', 'pdh', 'pdl', 'day_open', 'round_level_100', 'round_level_50'
        ]
        return pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS + extra_columns)

    out = prepared.copy()
    out['range'] = (out['high'] - out['low']).clip(lower=0.0)
    out['body'] = (out['close'] - out['open']).abs()
    out['body_ratio'] = (out['body'] / out['range'].replace(0, pd.NA)).fillna(0.0)
    out['upper_wick'] = (out['high'] - out[['open', 'close']].max(axis=1)).clip(lower=0.0)
    out['lower_wick'] = (out[['open', 'close']].min(axis=1) - out['low']).clip(lower=0.0)
    out['avg_range'] = out['range'].rolling(20, min_periods=1).mean()
    out['avg_volume'] = out['volume'].rolling(20, min_periods=1).mean()
    out['volume_ratio'] = (out['volume'] / out['avg_volume'].replace(0, pd.NA)).fillna(0.0)

    out['session_day'] = out['timestamp'].dt.strftime('%Y-%m-%d')
    typical_price = (out['high'] + out['low'] + out['close']) / 3.0
    cumulative_pv = (typical_price * out['volume']).groupby(out['session_day']).cumsum()
    cumulative_volume = out['volume'].groupby(out['session_day']).cumsum()
    out['vwap'] = typical_price.where(cumulative_volume <= 0, cumulative_pv / cumulative_volume).astype(float)
    out['above_vwap'] = out['close'] >= out['vwap']
    out['day_bias'] = out['above_vwap'].map({True: 'bullish', False: 'bearish'})

    out['time_block'] = out['timestamp'].apply(_time_block)
    interval_minutes = out['timestamp'].diff().dt.total_seconds().div(60.0)
    out['interval_minutes'] = interval_minutes.fillna(float(expected_interval_minutes)).round(2)
    out['interval_valid'] = out['interval_minutes'].between(float(expected_interval_minutes) - 0.1, float(expected_interval_minutes) + 0.1)
    out.loc[out.index[0], 'interval_valid'] = True
    out['gap_flag'] = out['interval_minutes'] > float(expected_interval_minutes) * 1.5

    opening_window = out['timestamp'].dt.strftime('%H:%M').between('09:15', '09:29')
    opening_high = out['high'].where(opening_window).groupby(out['session_day']).transform('max')
    opening_low = out['low'].where(opening_window).groupby(out['session_day']).transform('min')
    out['opening_high'] = opening_high.fillna(out.groupby('session_day')['high'].transform('first'))
    out['opening_low'] = opening_low.fillna(out.groupby('session_day')['low'].transform('first'))
    out['opening_range'] = (out['opening_high'] - out['opening_low']).clip(lower=0.0)

    day_high = out.groupby('session_day')['high'].transform('max')
    day_low = out.groupby('session_day')['low'].transform('min')
    out['intraday_range'] = (day_high - day_low).clip(lower=0.0)
    daily_range_avg = out[['session_day', 'intraday_range']].drop_duplicates()['intraday_range'].rolling(5, min_periods=1).mean().reset_index(drop=True)
    day_range_map = dict(zip(out[['session_day']].drop_duplicates()['session_day'], daily_range_avg))
    out['avg_daily_range'] = out['session_day'].map(day_range_map).astype(float)
    out['intraday_volatility_ratio'] = (out['intraday_range'] / out['avg_daily_range'].replace(0, pd.NA)).fillna(0.0)

    out['prev_high'] = out['high'].shift(1)
    out['prev_low'] = out['low'].shift(1)
    out['higher_high'] = (out['high'] > out['prev_high']).fillna(False)
    out['higher_low'] = (out['low'] > out['prev_low']).fillna(False)
    out['lower_high'] = (out['high'] < out['prev_high']).fillna(False)
    out['lower_low'] = (out['low'] < out['prev_low']).fillna(False)
    out['trend_strength'] = (
        out['higher_high'].astype(int).rolling(5, min_periods=1).sum()
        + out['higher_low'].astype(int).rolling(5, min_periods=1).sum()
        - out['lower_high'].astype(int).rolling(5, min_periods=1).sum()
        - out['lower_low'].astype(int).rolling(5, min_periods=1).sum()
    )

    daily = out.groupby('session_day').agg(day_high=('high', 'max'), day_low=('low', 'min'), day_open=('open', 'first')).reset_index()
    daily['pdh'] = daily['day_high'].shift(1)
    daily['pdl'] = daily['day_low'].shift(1)
    out = out.merge(daily[['session_day', 'pdh', 'pdl', 'day_open']], on='session_day', how='left')
    out['round_level_100'] = (out['close'] / 100.0).round() * 100.0
    out['round_level_50'] = (out['close'] / 50.0).round() * 50.0
    return out


def prepare_trading_data(df: Any, *, include_derived: bool = False) -> pd.DataFrame:
    """Normalize market data into the project's standard OHLCV schema."""
    return enrich_trading_data(df) if include_derived else _base_prepare(df)


__all__ = ["REQUIRED_OHLCV_COLUMNS", "prepare_trading_data", "enrich_trading_data"]
