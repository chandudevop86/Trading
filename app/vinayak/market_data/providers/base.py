from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True, slots=True)
class MarketDataRequest:
    symbol: str
    timeframe: str
    lookback: int = 200
    provider: str = 'YFINANCE'


@dataclass(frozen=True, slots=True)
class ProviderResult:
    frame: pd.DataFrame
    fetched_at: datetime
    provider: str
    cache_hit: bool = False


class MarketDataProvider(Protocol):
    name: str

    def fetch(self, request: MarketDataRequest) -> ProviderResult:
        ...


class StaticFrameProvider:
    name = 'STATIC'

    def __init__(self, frame: pd.DataFrame) -> None:
        self._frame = frame.copy()

    def fetch(self, request: MarketDataRequest) -> ProviderResult:
        return ProviderResult(
            frame=self._frame.copy(),
            fetched_at=datetime.now(UTC),
            provider=self.name,
            cache_hit=False,
        )
