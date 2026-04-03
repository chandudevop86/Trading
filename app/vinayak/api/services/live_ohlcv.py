from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
import csv
import os
import re

from vinayak.cache.redis_client import RedisCache, build_cache_key

try:
    from src.live_ohlcv import fetch_live_ohlcv as canonical_fetch_live_ohlcv
except Exception:  # pragma: no cover
    canonical_fetch_live_ohlcv = None  # type: ignore

try:
    from src.dhan_api import load_security_map
except Exception:  # pragma: no cover
    load_security_map = None  # type: ignore

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # type: ignore

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

DEFAULT_YFINANCE_TIMEOUT = 15.0
DEFAULT_CANDLE_CACHE_DIR = Path('app/vinayak/data/cache/candles')
DEFAULT_FALLBACK_PATHS = [
    Path('app/vinayak/data/live_ohlcv.csv'),
    Path('data/live_ohlcv.csv'),
]
REDIS_CANDLE_TTL_SECONDS = 900
_PERIOD_RE = re.compile(r'^(?P<count>\d+)(?P<unit>m[oo]?|d|wk|w|y)$', re.IGNORECASE)


def _sanitize_key(text: object) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9_-]+', '_', str(text or '').strip())
    return cleaned.strip('_') or 'UNKNOWN'


def _period_to_range(period: str) -> tuple[datetime, datetime]:
    end_dt = datetime.now(UTC)
    text = str(period or '1d').strip().lower()
    match = _PERIOD_RE.match(text)
    if not match:
        return end_dt - timedelta(days=1), end_dt

    count = int(match.group('count'))
    unit = match.group('unit')
    if unit in {'d'}:
        delta = timedelta(days=count)
    elif unit in {'w', 'wk'}:
        delta = timedelta(weeks=count)
    elif unit in {'mo', 'moo'}:
        delta = timedelta(days=30 * count)
    elif unit in {'y'}:
        delta = timedelta(days=365 * count)
    else:
        delta = timedelta(days=1)
    return end_dt - delta, end_dt


def build_candle_cache_path(
    *,
    provider: str,
    symbol: str,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    cache_dir: Path | None = None,
) -> Path:
    base_dir = Path(cache_dir or DEFAULT_CANDLE_CACHE_DIR)
    filename = (
        f"{_sanitize_key(provider).lower()}_{_sanitize_key(symbol).upper()}_{_sanitize_key(interval)}_"
        f"{start_dt.astimezone(UTC).strftime('%Y%m%dT%H%M%S')}_"
        f"{end_dt.astimezone(UTC).strftime('%Y%m%dT%H%M%S')}.csv"
    )
    return base_dir / filename


def _redis_exact_key(provider: str, symbol: str, interval: str, period: str) -> str:
    return build_cache_key('vinayak', 'ohlcv', provider.upper(), symbol.upper(), interval, period)


def _redis_latest_key(provider: str, symbol: str, interval: str) -> str:
    return build_cache_key('vinayak', 'ohlcv', provider.upper(), symbol.upper(), interval, 'latest')


def _normalize_frame(df: Any, *, symbol: str, interval: str) -> list[dict[str, Any]]:
    if df is None or getattr(df, 'empty', False):
        return []

    df = df.reset_index()
    if pd is not None and isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    rename_map = {
        'Datetime': 'timestamp',
        'Date': 'timestamp',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if 'timestamp' not in df.columns and len(df.columns) > 0:
        df = df.rename(columns={df.columns[0]: 'timestamp'})

    required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    existing = [c for c in required if c in df.columns]
    if not existing:
        return []
    df = df[existing].copy()

    rows: list[dict[str, Any]] = []
    for row in df.to_dict('records'):
        ts = row.get('timestamp', '')
        if isinstance(ts, datetime):
            ts = ts.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
        else:
            ts = str(ts)
        close_value = row.get('close', 0.0)
        rows.append(
            {
                'timestamp': ts,
                'open': row.get('open', 0.0),
                'high': row.get('high', 0.0),
                'low': row.get('low', 0.0),
                'close': close_value,
                'volume': row.get('volume', 0),
                'price': close_value,
                'interval': interval,
                'provider': 'YAHOO',
                'symbol': symbol,
                'source': 'YAHOO_DOWNLOAD',
                'is_closed': True,
            }
        )
    return rows


def read_candle_cache(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                'timestamp': row.get('timestamp', ''),
                'open': float(row.get('open', 0.0) or 0.0),
                'high': float(row.get('high', 0.0) or 0.0),
                'low': float(row.get('low', 0.0) or 0.0),
                'close': float(row.get('close', 0.0) or 0.0),
                'volume': int(float(row.get('volume', 0) or 0)),
                'price': float(row.get('price', row.get('close', 0.0)) or 0.0),
                'interval': row.get('interval', ''),
                'provider': row.get('provider', ''),
                'symbol': row.get('symbol', ''),
                'source': row.get('source', ''),
                'is_closed': str(row.get('is_closed', 'True')).strip().lower() != 'false',
            }
        )
    return normalized


def write_candle_cache(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_latest_candle_cache(
    *,
    provider: str,
    symbol: str,
    interval: str,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    base_dir = Path(cache_dir or DEFAULT_CANDLE_CACHE_DIR)
    if not base_dir.exists():
        return []
    pattern = (
        f"{_sanitize_key(provider).lower()}_{_sanitize_key(symbol).upper()}_{_sanitize_key(interval)}_*.csv"
    )
    try:
        candidates = sorted(base_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    except Exception:
        return []

    for candidate in candidates:
        try:
            rows = read_candle_cache(candidate)
        except Exception:
            continue
        if rows:
            return rows
    return []


def _fallback_candidates(explicit_path: str | Path | None = None) -> list[Path]:
    paths: list[Path] = []
    if explicit_path is not None:
        paths.append(Path(explicit_path))
    paths.extend(DEFAULT_FALLBACK_PATHS)

    deduped: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _read_fallback_ohlcv(path: str | Path | None = None) -> list[dict[str, Any]]:
    if pd is None:
        return []
    for candidate in _fallback_candidates(path):
        if not candidate.exists():
            continue
        try:
            df = pd.read_csv(candidate)
        except Exception:
            continue
        rows = _normalize_frame(df, symbol='', interval='')
        if rows:
            return rows
    return []


def _read_redis_rows(key: str) -> list[dict[str, Any]]:
    payload = RedisCache.from_env().get_json(key)
    if isinstance(payload, list) and payload:
        return [dict(item) for item in payload if isinstance(item, dict)]
    return []


def _write_redis_rows(key: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    RedisCache.from_env().set_json(key, rows, ttl_seconds=REDIS_CANDLE_TTL_SECONDS)


def fetch_live_ohlcv(
    symbol: str,
    interval: str = '1m',
    period: str = '1d',
    *,
    fallback_path: str | Path | None = None,
    provider: str | None = None,
    security_map_path: str | Path | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    selected_provider = str(provider or os.getenv('VINAYAK_MARKET_DATA_PROVIDER', os.getenv('MARKET_DATA_PROVIDER', 'AUTO')) or 'AUTO').strip().upper()
    if canonical_fetch_live_ohlcv is not None:
        security_map: dict[str, Any] | None = None
        if selected_provider in {'DHAN', 'AUTO'} and load_security_map is not None:
            resolved_map_path = Path(str(security_map_path or os.getenv('DHAN_SECURITY_MAP', 'app/vinayak/data/dhan_security_map.csv')))
            if resolved_map_path.exists():
                try:
                    security_map = load_security_map(resolved_map_path)
                except Exception:
                    security_map = None
        if selected_provider in {'DHAN', 'AUTO', 'YAHOO'}:
            try:
                rows = canonical_fetch_live_ohlcv(
                    symbol,
                    interval,
                    period,
                    provider=selected_provider,
                    security_map=security_map,
                    use_cache=use_cache,
                    force_refresh=force_refresh,
                    cache_dir=cache_dir,
                )
                if rows:
                    return rows
            except Exception:
                if selected_provider == 'DHAN':
                    raise
    if yf is None:
        raise ModuleNotFoundError('yfinance is required for fetch_live_ohlcv (pip install yfinance)')

    exact_redis_key = _redis_exact_key('YAHOO', symbol, interval, period)
    latest_redis_key = _redis_latest_key('YAHOO', symbol, interval)

    start_dt, end_dt = _period_to_range(period)
    cache_path = build_candle_cache_path(
        provider='YAHOO',
        symbol=symbol,
        interval=interval,
        start_dt=start_dt,
        end_dt=end_dt,
        cache_dir=cache_dir,
    )
    stale_rows = _read_redis_rows(latest_redis_key) if use_cache else []
    if not stale_rows and use_cache:
        stale_rows = read_latest_candle_cache(
            provider='YAHOO',
            symbol=symbol,
            interval=interval,
            cache_dir=cache_dir,
        )
    csv_fallback_rows = _read_fallback_ohlcv(fallback_path)

    if use_cache and not force_refresh:
        cached_rows = _read_redis_rows(exact_redis_key)
        if cached_rows:
            return cached_rows
        cached_rows = read_candle_cache(cache_path)
        if cached_rows:
            _write_redis_rows(exact_redis_key, cached_rows)
            _write_redis_rows(latest_redis_key, cached_rows)
            return cached_rows

    timeout = DEFAULT_YFINANCE_TIMEOUT
    try:
        timeout = float(os.getenv('YFINANCE_TIMEOUT', str(DEFAULT_YFINANCE_TIMEOUT)))
    except Exception:
        pass

    try:
        df = yf.download(
            tickers=symbol,
            interval=interval,
            period=period,
            auto_adjust=False,
            progress=False,
            threads=False,
            timeout=timeout,
        )
    except Exception:
        if stale_rows:
            return stale_rows
        if csv_fallback_rows:
            return csv_fallback_rows
        raise

    rows = _normalize_frame(df, symbol=symbol, interval=interval)
    if use_cache and rows:
        write_candle_cache(cache_path, rows)
        _write_redis_rows(exact_redis_key, rows)
        _write_redis_rows(latest_redis_key, rows)
    if rows:
        return rows
    if stale_rows:
        return stale_rows
    return csv_fallback_rows


def write_csv(rows: list[dict[str, Any]], path: str | Path) -> None:
    if not rows:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)



