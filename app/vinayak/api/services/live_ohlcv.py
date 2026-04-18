from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import csv
import os
import re
from zoneinfo import ZoneInfo

from vinayak.cache.redis_client import RedisCache, build_cache_key
from vinayak.execution.broker.dhan_client import DhanClient
from vinayak.infrastructure.market_data.dhan_security_map import (
    find_cash_instrument,
    load_security_map,
    normalize_trading_symbol,
)

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
INTRADAY_MAX_DAYS = 89
_PERIOD_RE = re.compile(r'^(?P<count>\d+)(?P<unit>m[oo]?|d|wk|w|y)$', re.IGNORECASE)
_REDIS_CACHE = RedisCache.from_env()
_SECURITY_MAP_CACHE: dict[str, dict[str, Any]] = {}
YAHOO_SYMBOL_ALIASES = {
    '^NSEI': '^NSEI',
    'NSEI': '^NSEI',
    'NIFTY': '^NSEI',
    'NIFTY50': '^NSEI',
    'NIFTY 50': '^NSEI',
    '^NSEBANK': '^NSEBANK',
    'NSEBANK': '^NSEBANK',
    'BANKNIFTY': '^NSEBANK',
    'NIFTYBANK': '^NSEBANK',
    'NIFTY BANK': '^NSEBANK',
}
try:
    IST = ZoneInfo('Asia/Kolkata')
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
_INTERVAL_ALIASES: dict[str, tuple[str, int, int | None]] = {
    '1m': ('intraday', 1, None),
    '2m': ('intraday', 1, 2),
    '5m': ('intraday', 5, None),
    '15m': ('intraday', 15, None),
    '25m': ('intraday', 25, None),
    '30m': ('intraday', 15, 30),
    '60m': ('intraday', 60, None),
    '1h': ('intraday', 60, None),
    '90m': ('intraday', 15, 90),
    '1d': ('daily', 1, None),
    '5d': ('daily', 1, 5),
    '1wk': ('daily', 1, -1),
    '1mo': ('daily', 1, -2),
    '3mo': ('daily', 1, -3),
}


def _sanitize_key(text: object) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9_-]+', '_', str(text or '').strip())
    return cleaned.strip('_') or 'UNKNOWN'


def _period_to_range(period: str) -> tuple[datetime, datetime]:
    end_dt = datetime.now(IST)
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


def _normalize_yahoo_symbol(symbol: str) -> str:
    raw = str(symbol or '').strip()
    upper = raw.upper()
    compact = re.sub(r'[^A-Z0-9^]', '', upper)
    if upper in YAHOO_SYMBOL_ALIASES:
        return YAHOO_SYMBOL_ALIASES[upper]
    return YAHOO_SYMBOL_ALIASES.get(compact, raw)


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


def _build_normalized_candle(
    *,
    timestamp: object,
    open_price: object,
    high_price: object,
    low_price: object,
    close_price: object,
    volume: object,
    interval: str = '',
    provider: str = '',
    symbol: str = '',
    source: str = '',
    exchange_segment: str = '',
    security_id: str = '',
    instrument: str = '',
    open_interest: object = 0,
    is_closed: bool = True,
) -> dict[str, Any]:
    if isinstance(timestamp, datetime):
        timestamp_text = timestamp.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp_text = str(timestamp)
    close_value = float(close_price or 0.0)
    return {
        'timestamp': timestamp_text,
        'open': float(open_price or 0.0),
        'high': float(high_price or 0.0),
        'low': float(low_price or 0.0),
        'close': close_value,
        'volume': int(float(volume or 0)),
        'price': close_value,
        'interval': interval,
        'provider': str(provider or '').upper(),
        'symbol': str(symbol or ''),
        'source': str(source or '').upper(),
        'exchange_segment': str(exchange_segment or '').upper(),
        'security_id': str(security_id or ''),
        'instrument': str(instrument or '').upper(),
        'open_interest': int(float(open_interest or 0)),
        'is_closed': bool(is_closed),
    }


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
    payload = _REDIS_CACHE.get_json(key)
    if isinstance(payload, list) and payload:
        return [dict(item) for item in payload if isinstance(item, dict)]
    return []


def _write_redis_rows(key: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    _REDIS_CACHE.set_json(key, rows, ttl_seconds=REDIS_CANDLE_TTL_SECONDS)


def _load_security_map_cached(path: Path) -> dict[str, Any] | None:
    if load_security_map is None or not path.exists():
        return None
    try:
        stat = path.stat()
    except Exception:
        return None
    cache_key = str(path.resolve())
    signature = (stat.st_mtime_ns, stat.st_size)
    cached = _SECURITY_MAP_CACHE.get(cache_key)
    if cached and cached.get('signature') == signature:
        return cached.get('value')
    try:
        value = load_security_map(path)
    except Exception:
        return None
    _SECURITY_MAP_CACHE[cache_key] = {'signature': signature, 'value': value}
    return value


def _resolve_dhan_instrument(symbol: str, security_map: dict[str, Any] | None = None) -> dict[str, str]:
    record = find_cash_instrument(security_map, symbol)
    if not record:
        normalized = normalize_trading_symbol(symbol)
        raise ValueError(f'No Dhan cash instrument found for {symbol or normalized}')

    security_id = str(record.get('security_id', '') or '').strip()
    exchange_segment = str(record.get('exchange_segment', '') or '').strip().upper()
    instrument = str(record.get('instrument_type', '') or record.get('instrument_name', '') or '').strip().upper()
    if not security_id or not exchange_segment or not instrument:
        raise ValueError(f'Incomplete Dhan instrument mapping for {symbol}')
    return {
        'security_id': security_id,
        'exchange_segment': exchange_segment,
        'instrument': instrument,
    }


def _format_dhan_date(dt: datetime) -> str:
    return dt.astimezone(IST).strftime('%Y-%m-%d')


def _format_dhan_timestamp(dt: datetime) -> str:
    return dt.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S')


def _normalize_dhan_epoch_timestamp(value: object) -> str:
    epoch = int(value)
    return datetime.fromtimestamp(epoch, tz=UTC).strftime('%Y-%m-%d %H:%M:%S')


def _validate_dhan_ohlcv_rows(payload: dict[str, Any], *, symbol: str, interval: str, source: str) -> None:
    opens = payload.get('open') if isinstance(payload, dict) else None
    highs = payload.get('high') if isinstance(payload, dict) else None
    lows = payload.get('low') if isinstance(payload, dict) else None
    closes = payload.get('close') if isinstance(payload, dict) else None
    volumes = payload.get('volume') if isinstance(payload, dict) else None
    timestamps = payload.get('timestamp') if isinstance(payload, dict) else None
    if not all(isinstance(series, list) for series in (opens, highs, lows, closes, volumes, timestamps)):
        raise ValueError(f'Dhan candle payload missing OHLCV arrays for {symbol} {interval} ({source})')
    expected_length = len(timestamps)
    for series_name, series in (('open', opens), ('high', highs), ('low', lows), ('close', closes), ('volume', volumes)):
        if len(series) != expected_length:
            raise ValueError(f'Dhan candle validation failed for {symbol} {interval} ({source}): {series_name} length mismatch.')


def _dhan_payload_to_rows(
    payload: dict[str, Any],
    *,
    symbol: str,
    interval: str,
    exchange_segment: str,
    security_id: str,
    instrument: str,
    source: str,
) -> list[dict[str, Any]]:
    opens = payload.get('open') or []
    highs = payload.get('high') or []
    lows = payload.get('low') or []
    closes = payload.get('close') or []
    volumes = payload.get('volume') or []
    timestamps = payload.get('timestamp') or []
    open_interest = payload.get('open_interest') if isinstance(payload.get('open_interest'), list) else []

    rows: list[dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes) or i >= len(volumes):
            break
        rows.append(
            _build_normalized_candle(
                timestamp=_normalize_dhan_epoch_timestamp(ts),
                open_price=opens[i],
                high_price=highs[i],
                low_price=lows[i],
                close_price=closes[i],
                volume=volumes[i],
                interval=interval,
                provider='DHAN',
                symbol=symbol,
                source=source,
                exchange_segment=exchange_segment,
                security_id=security_id,
                instrument=instrument,
                open_interest=open_interest[i] if i < len(open_interest) else 0,
                is_closed=True,
            )
        )
    return rows


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        timestamp = str(row.get('timestamp', '') or '')
        if timestamp:
            deduped[timestamp] = row
    return [deduped[key] for key in sorted(deduped.keys())]


def _floor_bucket(dt: datetime, minutes: int) -> datetime:
    minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def _aggregate_minute_rows(rows: list[dict[str, Any]], target_minutes: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        dt = datetime.strptime(str(row['timestamp']), '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)
        bucket = _floor_bucket(dt, target_minutes).strftime('%Y-%m-%d %H:%M:%S')
        buckets.setdefault(bucket, []).append(row)

    aggregated: list[dict[str, Any]] = []
    for bucket in sorted(buckets.keys()):
        chunk = buckets[bucket]
        aggregated.append(
            _build_normalized_candle(
                timestamp=bucket,
                open_price=chunk[0]['open'],
                high_price=max(float(item['high']) for item in chunk),
                low_price=min(float(item['low']) for item in chunk),
                close_price=chunk[-1]['close'],
                volume=sum(int(float(item.get('volume', 0) or 0)) for item in chunk),
                interval=f'{target_minutes}m',
                provider=chunk[-1].get('provider', ''),
                symbol=chunk[-1].get('symbol', ''),
                source=chunk[-1].get('source', ''),
                exchange_segment=chunk[-1].get('exchange_segment', ''),
                security_id=chunk[-1].get('security_id', ''),
                instrument=chunk[-1].get('instrument', ''),
                open_interest=chunk[-1].get('open_interest', 0),
                is_closed=True,
            )
        )
    return aggregated


def _aggregate_daily_rows(rows: list[dict[str, Any]], mode: int, output_interval: str) -> list[dict[str, Any]]:
    parsed = [{**row, '_dt': datetime.strptime(str(row['timestamp']), '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)} for row in rows]
    parsed.sort(key=lambda item: item['_dt'])
    buckets: dict[str, list[dict[str, Any]]] = {}

    for index, row in enumerate(parsed):
        dt = row['_dt']
        if mode == 5:
            bucket = f'{index // 5:08d}'
        elif mode == -1:
            iso_year, iso_week, _ = dt.isocalendar()
            bucket = f'{iso_year}-W{iso_week:02d}'
        elif mode == -2:
            bucket = f'{dt.year}-{dt.month:02d}'
        elif mode == -3:
            quarter = ((dt.month - 1) // 3) + 1
            bucket = f'{dt.year}-Q{quarter}'
        else:
            bucket = dt.strftime('%Y-%m-%d')
        buckets.setdefault(bucket, []).append(row)

    aggregated: list[dict[str, Any]] = []
    for bucket in sorted(buckets.keys()):
        chunk = buckets[bucket]
        aggregated.append(
            _build_normalized_candle(
                timestamp=chunk[0]['_dt'],
                open_price=chunk[0]['open'],
                high_price=max(float(item['high']) for item in chunk),
                low_price=min(float(item['low']) for item in chunk),
                close_price=chunk[-1]['close'],
                volume=sum(int(float(item.get('volume', 0) or 0)) for item in chunk),
                interval=output_interval,
                provider=chunk[-1].get('provider', ''),
                symbol=chunk[-1].get('symbol', ''),
                source=chunk[-1].get('source', ''),
                exchange_segment=chunk[-1].get('exchange_segment', ''),
                security_id=chunk[-1].get('security_id', ''),
                instrument=chunk[-1].get('instrument', ''),
                open_interest=chunk[-1].get('open_interest', 0),
                is_closed=True,
            )
        )
    return aggregated


def fetch_dhan_ohlcv(
    symbol: str,
    interval: str,
    period: str,
    *,
    security_map: dict[str, Any] | None = None,
    broker_client: object | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    mode, base_interval, aggregate_to = _INTERVAL_ALIASES.get(interval, ('intraday', 5, None))
    instrument = _resolve_dhan_instrument(symbol, security_map=security_map)
    start_dt, end_dt = _period_to_range(period)
    cache_path = build_candle_cache_path(
        provider='DHAN',
        symbol=symbol,
        interval=interval,
        start_dt=start_dt.astimezone(UTC),
        end_dt=end_dt.astimezone(UTC),
        cache_dir=cache_dir,
    )

    if use_cache and not force_refresh:
        cached_rows = read_candle_cache(cache_path)
        if cached_rows:
            return cached_rows

    client = broker_client if broker_client is not None else DhanClient.from_env()
    rows: list[dict[str, Any]] = []
    if mode == 'daily':
        payload = client.get_historical_data(
            security_id=instrument['security_id'],
            exchange_segment=instrument['exchange_segment'],
            instrument=instrument['instrument'],
            from_date=_format_dhan_date(start_dt),
            to_date=_format_dhan_date(end_dt + timedelta(days=1)),
            oi=False,
        )
        _validate_dhan_ohlcv_rows(payload, symbol=symbol, interval='1d', source='DHAN_HISTORICAL')
        rows = _dhan_payload_to_rows(
            payload,
            symbol=symbol,
            interval='1d',
            exchange_segment=instrument['exchange_segment'],
            security_id=instrument['security_id'],
            instrument=instrument['instrument'],
            source='DHAN_HISTORICAL',
        )
    else:
        cursor = start_dt
        while cursor < end_dt:
            chunk_end = min(cursor + timedelta(days=INTRADAY_MAX_DAYS), end_dt)
            payload = client.get_intraday_data(
                security_id=instrument['security_id'],
                exchange_segment=instrument['exchange_segment'],
                instrument=instrument['instrument'],
                interval=base_interval,
                from_date=_format_dhan_timestamp(cursor),
                to_date=_format_dhan_timestamp(chunk_end),
                oi=False,
            )
            _validate_dhan_ohlcv_rows(payload, symbol=symbol, interval=f'{base_interval}m', source='DHAN_HISTORICAL')
            rows.extend(
                _dhan_payload_to_rows(
                    payload,
                    symbol=symbol,
                    interval=f'{base_interval}m',
                    exchange_segment=instrument['exchange_segment'],
                    security_id=instrument['security_id'],
                    instrument=instrument['instrument'],
                    source='DHAN_HISTORICAL',
                )
            )
            if chunk_end <= cursor:
                break
            cursor = chunk_end

    rows = _dedupe_rows(rows)
    if aggregate_to:
        rows = _aggregate_daily_rows(rows, aggregate_to, interval) if mode == 'daily' else _aggregate_minute_rows(rows, aggregate_to)
    else:
        rows = [dict(row, interval=interval) for row in rows]

    if use_cache and rows:
        write_candle_cache(cache_path, rows)
    return rows


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
    security_map: dict[str, Any] | None = None
    if selected_provider in {'DHAN', 'AUTO'}:
        resolved_map_path = Path(str(security_map_path or os.getenv('DHAN_SECURITY_MAP', 'app/vinayak/data/dhan_security_map.csv')))
        security_map = _load_security_map_cached(resolved_map_path)
        if selected_provider == 'DHAN':
            return fetch_dhan_ohlcv(
                symbol,
                interval,
                period,
                security_map=security_map,
                use_cache=use_cache,
                force_refresh=force_refresh,
                cache_dir=cache_dir,
            )
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
            tickers=_normalize_yahoo_symbol(symbol),
            interval=interval,
            period=period,
            auto_adjust=False,
            progress=False,
            threads=False,
            timeout=timeout,
        )
    except Exception:
        if selected_provider == 'AUTO':
            return fetch_dhan_ohlcv(
                symbol,
                interval,
                period,
                security_map=security_map,
                use_cache=use_cache,
                force_refresh=force_refresh,
                cache_dir=cache_dir,
            )
        if stale_rows:
            return stale_rows
        csv_fallback_rows = _read_fallback_ohlcv(fallback_path)
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
    if selected_provider == 'AUTO':
        return fetch_dhan_ohlcv(
            symbol,
            interval,
            period,
            security_map=security_map,
            use_cache=use_cache,
            force_refresh=force_refresh,
            cache_dir=cache_dir,
        )
    if stale_rows:
        return stale_rows
    return _read_fallback_ohlcv(fallback_path)


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
