"""Named adapter boundary for legacy OHLCV processing helpers."""

from __future__ import annotations

try:
    from vinayak.data.data_processing import (
        REQUIRED_OHLCV_COLUMNS,
        load_and_process_ohlcv as load_legacy_ohlcv_processing,
    )
except ModuleNotFoundError:
    from src.data_processing import (
        REQUIRED_OHLCV_COLUMNS,
        load_and_process_ohlcv as load_legacy_ohlcv_processing,
    )

__all__ = ["REQUIRED_OHLCV_COLUMNS", "load_legacy_ohlcv_processing"]
