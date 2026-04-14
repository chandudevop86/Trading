from __future__ import annotations

import pandas as pd
import pytest

from vinayak.domain.exceptions import DataNormalizationError, MissingRequiredColumnError
from vinayak.market_data.normalization import OhlcvNormalizationConfig, normalize_ohlcv_frame


def test_normalizer_coalesces_aliases_and_sorts_rows() -> None:
    frame = pd.DataFrame(
        [
            {'Time': '2026-01-01T09:20:00+05:30', 'Open': '101', 'High': '102', 'Low': '100', 'Close': '101.5', 'Volume': '12'},
            {'Time': '2026-01-01T09:15:00+05:30', 'Open': '100', 'High': '101', 'Low': '99', 'Close': '100.5', 'Volume': '10'},
        ]
    )

    normalized = normalize_ohlcv_frame(frame)

    assert list(normalized.columns) == ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    assert normalized.iloc[0]['open'] == 100
    assert normalized['timestamp'].is_monotonic_increasing


def test_normalizer_raises_on_missing_required_columns() -> None:
    with pytest.raises(MissingRequiredColumnError):
        normalize_ohlcv_frame([{'timestamp': '2026-01-01T09:15:00Z', 'open': 1}])


def test_normalizer_can_raise_instead_of_drop_invalid_rows() -> None:
    frame = pd.DataFrame(
        [{'timestamp': '2026-01-01T09:15:00Z', 'open': 100, 'high': 99, 'low': 101, 'close': 100, 'volume': 10}]
    )

    with pytest.raises(DataNormalizationError):
        normalize_ohlcv_frame(frame, config=OhlcvNormalizationConfig(drop_invalid_rows=False))
