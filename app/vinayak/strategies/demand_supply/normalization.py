from __future__ import annotations

from datetime import datetime, time
from typing import Any

import pandas as pd

from vinayak.data.cleaner import OHLCVValidationError, coerce_ohlcv
from vinayak.strategies.demand_supply.models import ALIAS_MAP, OPTIONAL_FIELDS


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_time(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.columns = [ALIAS_MAP.get(str(column).strip().lower(), str(column).strip().lower()) for column in frame.columns]
    return out


def normalize_ohlcv_for_supply_demand(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise TypeError("normalize_ohlcv_for_supply_demand expects a pandas DataFrame")
    if df.empty:
        raise OHLCVValidationError("empty dataframe")
    normalized = normalize_columns(df)
    cleaned = coerce_ohlcv(normalized)
    for column in OPTIONAL_FIELDS:
        if column in normalized.columns and column not in cleaned.columns:
            cleaned[column] = normalized[column].reset_index(drop=True)
    if cleaned.empty:
        raise OHLCVValidationError("no valid OHLCV rows available after normalization")
    return cleaned.reset_index(drop=True)


__all__ = ["normalize_ohlcv_for_supply_demand", "parse_time", "safe_float"]
