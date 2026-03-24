from __future__ import annotations

from typing import Any

import pandas as pd

from src.trading_core import prepare_trading_data as _prepare_trading_data


def prepare_trading_data(df: Any) -> pd.DataFrame:
    """Normalize market data into the project's standard OHLCV schema."""
    return _prepare_trading_data(df)


__all__ = ['prepare_trading_data']
