from __future__ import annotations

from typing import Any

import pandas as pd

from src.data.cleaner import CleanerConfig, coerce_ohlcv
from src.data_processing import REQUIRED_OHLCV_COLUMNS, load_and_process_ohlcv


def enrich_trading_data(df: Any, *, expected_interval_minutes: int = 5) -> pd.DataFrame:
    cleaned = coerce_ohlcv(
        df,
        CleanerConfig(
            expected_interval_minutes=expected_interval_minutes,
            require_vwap=True,
            allow_vwap_compute=True,
        ),
    )
    prepared, _ = load_and_process_ohlcv(
        cleaned,
        include_derived=True,
        expected_interval_minutes=expected_interval_minutes,
    )
    return prepared


def prepare_trading_data(df: Any, *, include_derived: bool = False) -> pd.DataFrame:
    """Normalize market data into the project's standard OHLCV schema."""
    cleaned = coerce_ohlcv(
        df,
        CleanerConfig(
            require_vwap=bool(include_derived),
            allow_vwap_compute=bool(include_derived),
        ),
    )
    prepared, _ = load_and_process_ohlcv(cleaned, include_derived=include_derived)
    if include_derived:
        return prepared
    return prepared.loc[:, REQUIRED_OHLCV_COLUMNS].copy() if not prepared.empty else pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS)


__all__ = ["REQUIRED_OHLCV_COLUMNS", "prepare_trading_data", "enrich_trading_data"]
