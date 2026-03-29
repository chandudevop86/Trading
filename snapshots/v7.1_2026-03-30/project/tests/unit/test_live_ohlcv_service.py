from __future__ import annotations

from pathlib import Path

from vinayak.api.services import live_ohlcv as service


class _StubCache:
    def __init__(self, payload=None) -> None:
        self.payload = payload
        self.calls: list[tuple[str, object, int | None]] = []

    def get_json(self, key: str):
        return self.payload.get(key) if isinstance(self.payload, dict) else None

    def set_json(self, key: str, value, ttl_seconds: int | None = None) -> bool:
        self.calls.append((key, value, ttl_seconds))
        return True


class _StubYF:
    def __init__(self, frame=None, error: Exception | None = None) -> None:
        self.frame = frame
        self.error = error
        self.calls: list[dict[str, object]] = []

    def download(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.frame


def test_fetch_live_ohlcv_reads_from_redis_before_yahoo(monkeypatch) -> None:
    rows = [
        {
            'timestamp': '2026-03-24 09:15:00',
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 1000,
            'price': 100.5,
            'interval': '1m',
            'provider': 'YAHOO',
            'symbol': '^NSEI',
            'source': 'REDIS_CACHE',
            'is_closed': True,
        }
    ]
    cache = _StubCache(
        {
            service._redis_exact_key('YAHOO', '^NSEI', '1m', '1d'): rows,
        }
    )
    stub = _StubYF(error=RuntimeError('network should not be used'))

    monkeypatch.setattr(service, 'RedisCache', type('RedisCacheStub', (), {'from_env': staticmethod(lambda: cache)}))
    monkeypatch.setattr(service, 'yf', stub)

    result = service.fetch_live_ohlcv('^NSEI', '1m', '1d')

    assert result == rows
    assert stub.calls == []


def test_fetch_live_ohlcv_uses_redis_latest_on_download_failure(monkeypatch) -> None:
    latest_rows = [
        {
            'timestamp': '2026-03-24 09:20:00',
            'open': 101.0,
            'high': 102.0,
            'low': 100.5,
            'close': 101.6,
            'volume': 900,
            'price': 101.6,
            'interval': '5m',
            'provider': 'YAHOO',
            'symbol': '^NSEI',
            'source': 'REDIS_STALE',
            'is_closed': True,
        }
    ]
    cache = _StubCache(
        {
            service._redis_latest_key('YAHOO', '^NSEI', '5m'): latest_rows,
        }
    )

    monkeypatch.setattr(service, 'RedisCache', type('RedisCacheStub', (), {'from_env': staticmethod(lambda: cache)}))
    monkeypatch.setattr(service, 'yf', _StubYF(error=RuntimeError('timeout')))

    result = service.fetch_live_ohlcv('^NSEI', '5m', '1d', use_cache=True)

    assert result == latest_rows


def test_write_csv_creates_output(tmp_path) -> None:
    output = tmp_path / 'candles.csv'
    rows = [
        {
            'timestamp': '2026-03-24 09:15:00',
            'open': 100.0,
            'high': 101.0,
            'low': 99.0,
            'close': 100.5,
            'volume': 1000,
            'price': 100.5,
            'interval': '1m',
            'provider': 'YAHOO',
            'symbol': '^NSEI',
            'source': 'YAHOO_DOWNLOAD',
            'is_closed': True,
        }
    ]

    service.write_csv(rows, output)

    text = output.read_text(encoding='utf-8')
    assert 'timestamp,open,high,low,close,volume,price,interval,provider,symbol,source,is_closed' in text
    assert '^NSEI' in text
