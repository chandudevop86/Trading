from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from typing import Protocol

import pandas as pd

from vinayak.market_data.normalization import normalize_ohlcv_frame
from vinayak.market_data.providers.base import MarketDataProvider, MarketDataRequest, ProviderResult
from vinayak.observability.correlation import get_correlation_id
from vinayak.observability.json_logging import get_logger
from vinayak.observability.prometheus import (
    record_cache_hit,
    record_cache_miss,
    record_provider_failure,
    record_provider_latency,
)


class CacheStore(Protocol):
    def get_frame(self, key: str) -> pd.DataFrame | None:
        ...

    def set_frame(self, key: str, frame: pd.DataFrame, ttl_seconds: int) -> None:
        ...


@dataclass(slots=True)
class InMemoryCacheStore:
    _store: dict[str, tuple[pd.DataFrame, datetime]]

    def __init__(self) -> None:
        self._store = {}

    def get_frame(self, key: str) -> pd.DataFrame | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        frame, expires_at = entry
        if expires_at < datetime.now(UTC):
            self._store.pop(key, None)
            return None
        return frame.copy()

    def set_frame(self, key: str, frame: pd.DataFrame, ttl_seconds: int) -> None:
        self._store[key] = (frame.copy(), datetime.now(UTC) + timedelta(seconds=ttl_seconds))


@dataclass(frozen=True, slots=True)
class CircuitBreakerState:
    failures: int = 0
    opened_until: datetime | None = None


class MarketDataService:
    def __init__(
        self,
        provider: MarketDataProvider,
        *,
        cache_store: CacheStore | None = None,
        cache_ttl_seconds: int = 30,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        stale_after_seconds: int = 120,
    ) -> None:
        self.provider = provider
        self.cache_store = cache_store or InMemoryCacheStore()
        self.cache_ttl_seconds = cache_ttl_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.stale_after_seconds = stale_after_seconds
        self.breaker = CircuitBreakerState()
        self.logger = get_logger('vinayak.market_data.service')

    def fetch_candles(self, request: MarketDataRequest) -> ProviderResult:
        cache_key = self._cache_key(request)
        cached = self.cache_store.get_frame(cache_key)
        if cached is not None:
            record_cache_hit(self.provider.name)
            return ProviderResult(frame=cached, fetched_at=datetime.now(UTC), provider=self.provider.name, cache_hit=True)

        record_cache_miss(self.provider.name)
        if self.breaker.opened_until and self.breaker.opened_until > datetime.now(UTC):
            raise RuntimeError(f'Circuit breaker open for provider {self.provider.name}')

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 2):
            started = monotonic()
            try:
                result = self.provider.fetch(request)
                record_provider_latency(self.provider.name, monotonic() - started)
                normalized = normalize_ohlcv_frame(result.frame)
                self._assert_fresh(normalized)
                self.cache_store.set_frame(cache_key, normalized, self.cache_ttl_seconds)
                self.breaker = CircuitBreakerState()
                self.logger.info(
                    'market_data_fetch_succeeded',
                    extra={
                        'provider': self.provider.name,
                        'symbol': request.symbol,
                        'timeframe': request.timeframe,
                        'correlation_id': get_correlation_id(),
                        'rows': len(normalized),
                        'cache_hit': False,
                    },
                )
                return ProviderResult(
                    frame=normalized,
                    fetched_at=result.fetched_at,
                    provider=result.provider,
                    cache_hit=False,
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                record_provider_failure(self.provider.name)
                self.breaker = CircuitBreakerState(
                    failures=self.breaker.failures + 1,
                    opened_until=datetime.now(UTC) + timedelta(seconds=30) if attempt > self.max_retries else None,
                )
                if attempt > self.max_retries:
                    break
                sleep(min(0.5 * attempt, 2.0))

        assert last_error is not None
        raise last_error

    def _assert_fresh(self, frame: pd.DataFrame) -> None:
        last_timestamp = frame.iloc[-1]['timestamp']
        if hasattr(last_timestamp, 'to_pydatetime'):
            last_dt = last_timestamp.to_pydatetime()
        else:
            last_dt = last_timestamp
        if (datetime.now(UTC) - last_dt).total_seconds() > self.stale_after_seconds:
            raise RuntimeError('Market data is stale.')

    def _cache_key(self, request: MarketDataRequest) -> str:
        return f'market-data:{self.provider.name}:{request.symbol.upper()}:{request.timeframe}:{request.lookback}'
