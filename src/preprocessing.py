from __future__ import annotations

from typing import Any

import pandas as pd

from src.data_processing import REQUIRED_OHLCV_COLUMNS, load_and_process_ohlcv


def enrich_trading_data(df: Any, *, expected_interval_minutes: int = 5) -> pd.DataFrame:
    prepared, _ = load_and_process_ohlcv(
        df,
        include_derived=True,
        expected_interval_minutes=expected_interval_minutes,
    )
    return prepared


def prepare_trading_data(df: Any, *, include_derived: bool = False) -> pd.DataFrame:
    """Normalize market data into the project's standard OHLCV schema."""
    prepared, _ = load_and_process_ohlcv(df, include_derived=include_derived)
    if include_derived:
        return prepared
    return prepared.loc[:, REQUIRED_OHLCV_COLUMNS].copy() if not prepared.empty else pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS)


__all__ = ["REQUIRED_OHLCV_COLUMNS", "prepare_trading_data", "enrich_trading_data"]
