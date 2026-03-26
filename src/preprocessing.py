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
    if "timestamp" in prepared.columns:
        return prepared["timestamp"]
    if "datetime" in prepared.columns:
        return prepared["datetime"]
    if "date" in prepared.columns and "time" in prepared.columns:
        date_text = prepared["date"].astype(str).str.strip()
        time_text = prepared["time"].astype(str).str.strip()
        return date_text + " " + time_text
    if "date" in prepared.columns:
        return prepared["date"]
    if "time" in prepared.columns:
        return prepared["time"]
    return None


def prepare_trading_data(df: Any) -> pd.DataFrame:
    """Normalize market data into the project's standard OHLCV schema."""
    prepared = _coerce_dataframe(df)
    if prepared.empty:
        return pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS)

    prepared.columns = [_normalize_column_name(column) for column in prepared.columns]
    prepared = prepared.rename(columns={column: _COLUMN_ALIASES.get(column, column) for column in prepared.columns})

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


__all__ = ["REQUIRED_OHLCV_COLUMNS", "prepare_trading_data"]
