from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.market_data.providers.base import MarketDataRequest, ProviderResult


class RuntimeLiveOhlcvProvider:
    """App-owned live OHLCV provider for the supported runtime surface."""

    name = 'RUNTIME_LIVE_OHLCV'

    def fetch(self, request: MarketDataRequest) -> ProviderResult:
        rows = fetch_live_ohlcv(
            symbol=request.symbol,
            interval=request.timeframe,
            period='1d',
            provider=request.provider,
        )
        return ProviderResult(
            frame=pd.DataFrame(rows),
            fetched_at=datetime.now(UTC),
            provider=self.name,
            cache_hit=False,
        )
