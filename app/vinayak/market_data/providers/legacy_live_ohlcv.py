from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.market_data.providers.base import MarketDataProvider, MarketDataRequest, ProviderResult


class LegacyLiveOhlcvProvider:
    name = 'LEGACY_LIVE_OHLCV'

    def fetch(self, request: MarketDataRequest) -> ProviderResult:
        rows = fetch_live_ohlcv(
            symbol=request.symbol,
            interval=request.timeframe,
            period='1d',
            provider=request.provider,
            force_refresh=True,
        )
        frame = pd.DataFrame(rows)
        return ProviderResult(
            frame=frame,
            fetched_at=datetime.now(UTC),
            provider=self.name,
            cache_hit=False,
        )
