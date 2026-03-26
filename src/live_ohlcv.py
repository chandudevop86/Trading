from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import csv
import os
import re

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # type: ignore

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

DEFAULT_DATA_PROVIDER = 'AUTO'
DEFAULT_CANDLE_CACHE_DIR = Path('data/cache/candles')
DEFAULT_YFINANCE_TIMEOUT = 15.0
try:
    IST = ZoneInfo('Asia/Kolkata')
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
INTRADAY_MAX_DAYS = 89


@dataclass(slots=True)
class NormalizedCandle:
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    price: float
    interval: str = ''
    provider: str = ''
    symbol: str = ''
    source: str = ''
    exchange_segment: str = ''
    security_id: str = ''
    instrument: str = ''
    open_interest: int = 0
    is_closed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'price': self.price,
            'interval': self.interval,
            'provider': self.provider,
            'symbol': self.symbol,
            'source': self.source,
            'exchange_segment': self.exchange_segment,
            'security_id': self.security_id,
            'instrument': self.instrument,
            'open_interest': self.open_interest,
            'is_closed': self.is_closed,
        }


def _to_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    chart = payload.get('chart') if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        return []

    results = chart.get('result')
    if not isinstance(results, list) or not results:
        return []

    result = results[0]
    if not isinstance(result, dict):
        return []

    timestamps = result.get('timestamp')
    if not isinstance(timestamps, list) or not timestamps:
        return []

    indicators = result.get('indicators')
    if not isinstance(indicators, dict):
        return []

    quotes = indicators.get('quote')
    if not isinstance(quotes, list) or not quotes:
        return []

    quote = quotes[0]
    if not isinstance(quote, dict):
        return []

    opens = quote.get('open') or []
    highs = quote.get('high') or []
    lows = quote.get('low') or []
    closes = quote.get('close') or []
    volumes = quote.get('volume') or []

    rows: list[dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes) or i >= len(volumes):
            break
        o = opens[i]
        h = highs[i]
        l = lows[i]
        c = closes[i]
        v = volumes[i]
        if o is None or h is None or l is None or c is None:
            continue
        try:
            ts_int = int(ts)
        except Exception:
            continue
        rows.append(
            _build_normalized_candle(
                timestamp=datetime.fromtimestamp(ts_int, tz=UTC),
                open_price=o,
                high_price=h,
                low_price=l,
                close_price=c,
                volume=v,
                provider='YAHOO',
                source='YAHOO_CHART',
            )
        )
    return rows


def _clean(value: object) -> str:
    return str(value or '').strip()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_timestamp_value(value: object) -> str:
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    text = _clean(value)
    if not text:
        return ''
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            dt = datetime.strptime(text, fmt)
            if fmt == '%Y-%m-%d':
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt.replace(tzinfo=UTC).strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return text


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
    close_value = _safe_float(close_price)
    return NormalizedCandle(
        timestamp=_normalize_timestamp_value(timestamp),
        open=round(_safe_float(open_price), 4),
        high=round(_safe_float(high_price), 4),
        low=round(_safe_float(low_price), 4),
        close=round(close_value, 4),
        volume=_safe_int(volume),
        price=round(close_value, 4),
        interval=_clean(interval),
        provider=_clean(provider).upper(),
        symbol=_clean(symbol),
        source=_clean(source).upper(),
        exchange_segment=_clean(exchange_segment).upper(),
        security_id=_clean(security_id),
        instrument=_clean(instrument).upper(),
        open_interest=_safe_int(open_interest),
        is_closed=bool(is_closed),
    ).to_dict()


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


def _period_to_range(period: str, *, now: datetime | None = None) -> tuple[datetime, datetime]:
    end = now.astimezone(IST) if now is not None else datetime.now(IST)
    end = end.replace(second=0, microsecond=0)
    text = _clean(period).lower()
    if not text:
        return end - timedelta(days=1), end
    if text == 'ytd':
        start = datetime(end.year, 1, 1, tzinfo=IST)
        return start, end
    if text == 'max':
        return end - timedelta(days=3650), end

    digits = ''.join(ch for ch in text if ch.isdigit())
    unit = ''.join(ch for ch in text if ch.isalpha())
    count = max(1, _safe_int(digits, default=1))
    if unit in {'m', 'min'}:
        delta = timedelta(minutes=count)
    elif unit in {'h', 'hr'}:
        delta = timedelta(hours=count)
    elif unit in {'d', 'day'}:
        delta = timedelta(days=count)
    elif unit in {'wk', 'w', 'week'}:
        delta = timedelta(days=count * 7)
    elif unit in {'mo', 'mon', 'month'}:
        delta = timedelta(days=count * 30)
    elif unit in {'y', 'yr', 'year'}:
        delta = timedelta(days=count * 365)
    else:
        delta = timedelta(days=1)
    return end - delta, end


def _format_dhan_date(dt: datetime) -> str:
    return dt.astimezone(IST).strftime('%Y-%m-%d')


def _format_dhan_timestamp(dt: datetime) -> str:
    return dt.astimezone(IST).strftime('%Y-%m-%d %H:%M:%S')


def _sanitize_key(text: object) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9_-]+', '_', _clean(text))
    return cleaned.strip('_') or 'UNKNOWN'


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


def read_candle_cache(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            _build_normalized_candle(
                timestamp=row.get('timestamp', ''),
                open_price=row.get('open', 0.0),
                high_price=row.get('high', 0.0),
                low_price=row.get('low', 0.0),
                close_price=row.get('close', 0.0),
                volume=row.get('volume', 0),
                interval=row.get('interval', ''),
                provider=row.get('provider', ''),
                symbol=row.get('symbol', ''),
                source=row.get('source', ''),
                exchange_segment=row.get('exchange_segment', ''),
                security_id=row.get('security_id', ''),
                instrument=row.get('instrument', ''),
                open_interest=row.get('open_interest', 0),
                is_closed=str(row.get('is_closed', 'True')).strip().lower() != 'false',
            )
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


def _resolve_dhan_instrument(symbol: str, security_map: dict[str, Any] | None = None) -> dict[str, str]:
    from src.dhan_api import find_cash_instrument, load_security_map, normalize_trading_symbol

    loaded_map = security_map
    if loaded_map is None:
        loaded_map = load_security_map(os.getenv('DHAN_SECURITY_MAP', 'data/dhan_security_map.csv'))

    record = find_cash_instrument(loaded_map, symbol)
    if not record:
        normalized = normalize_trading_symbol(symbol)
        raise ValueError(f'No Dhan cash instrument found for {symbol or normalized}')

    security_id = _clean(record.get('security_id') or record.get('securityId'))
    exchange_segment = _clean(record.get('exchange_segment') or record.get('exchangeSegment')).upper()
    instrument = _clean(record.get('instrument_type') or record.get('instrument_name')).upper()
    if not security_id or not exchange_segment or not instrument:
        raise ValueError(f'Incomplete Dhan instrument mapping for {symbol}')

    return {
        'security_id': security_id,
        'exchange_segment': exchange_segment,
        'instrument': instrument,
    }

def _normalize_dhan_epoch_timestamp(value: object) -> str:
    try:
        epoch = int(value)
    except (TypeError, ValueError):
        raise ValueError(f'invalid timestamp value {value!r}') from None
    return datetime.fromtimestamp(epoch, tz=UTC).strftime('%Y-%m-%d %H:%M:%S')


def _validate_dhan_ohlcv_rows(
    payload: dict[str, Any],
    *,
    symbol: str,
    interval: str,
    source: str,
) -> None:
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
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): {series_name} length {len(series)} does not match timestamp length {expected_length}'
            )

    seen_timestamps: set[str] = set()
    row_count = expected_length
    for index in range(row_count):
        try:
            normalized_timestamp = _normalize_dhan_epoch_timestamp(timestamps[index])
        except ValueError as exc:
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): missing or invalid timestamp at row {index}: {exc}'
            ) from None
        if normalized_timestamp in seen_timestamps:
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): duplicate candle at {normalized_timestamp}'
            )
        seen_timestamps.add(normalized_timestamp)

        open_price = _safe_float(opens[index], default=float('nan'))
        high_price = _safe_float(highs[index], default=float('nan'))
        low_price = _safe_float(lows[index], default=float('nan'))
        close_price = _safe_float(closes[index], default=float('nan'))
        if any(value != value for value in (open_price, high_price, low_price, close_price)):
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): non-numeric OHLC at {normalized_timestamp}'
            )
        if min(open_price, high_price, low_price, close_price) <= 0:
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): OHLC must be positive at {normalized_timestamp}'
            )
        if high_price < max(open_price, low_price, close_price) or low_price > min(open_price, high_price, close_price):
            raise ValueError(
                f'Dhan candle validation failed for {symbol} {interval} ({source}): invalid OHLC range at {normalized_timestamp}'
            )


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
    opens = payload.get('open') if isinstance(payload, dict) else None
    highs = payload.get('high') if isinstance(payload, dict) else None
    lows = payload.get('low') if isinstance(payload, dict) else None
    closes = payload.get('close') if isinstance(payload, dict) else None
    volumes = payload.get('volume') if isinstance(payload, dict) else None
    timestamps = payload.get('timestamp') if isinstance(payload, dict) else None
    open_interest = payload.get('open_interest') if isinstance(payload, dict) else None

    if not all(isinstance(series, list) for series in (opens, highs, lows, closes, volumes, timestamps)):
        return []

    oi_values = open_interest if isinstance(open_interest, list) else []
    rows: list[dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes) or i >= len(volumes):
            break
        try:
            epoch = int(ts)
        except (TypeError, ValueError):
            continue
        rows.append(
            _build_normalized_candle(
                timestamp=datetime.fromtimestamp(epoch, tz=UTC),
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
                open_interest=oi_values[i] if i < len(oi_values) else 0,
                is_closed=True,
            )
        )
    return rows


def normalize_dhan_live_payload(
    payload: dict[str, Any],
    *,
    symbol: str = '',
    interval: str = '',
    exchange_segment: str = '',
    security_id: str = '',
    instrument: str = '',
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get('open'), list) and isinstance(payload.get('timestamp'), list):
        _validate_dhan_ohlcv_rows(payload, symbol=symbol, interval=interval, source='DHAN_LIVE_FEED')
        return _dhan_payload_to_rows(
            payload,
            symbol=symbol,
            interval=interval,
            exchange_segment=exchange_segment,
            security_id=security_id,
            instrument=instrument,
            source='DHAN_LIVE_FEED',
        )

    candidates = []
    data = payload.get('data')
    if isinstance(data, list):
        candidates.extend(item for item in data if isinstance(item, dict))
    elif isinstance(data, dict):
        candidates.append(data)
    else:
        candidates.append(payload)

    normalized: list[dict[str, Any]] = []
    for item in candidates:
        ts = item.get('timestamp') or item.get('last_trade_time') or item.get('LTT') or item.get('time')
        if not ts:
            continue
        normalized.append(
            _build_normalized_candle(
                timestamp=ts,
                open_price=item.get('open', item.get('o', item.get('O', item.get('ltp', 0.0)))),
                high_price=item.get('high', item.get('h', item.get('H', item.get('ltp', 0.0)))),
                low_price=item.get('low', item.get('l', item.get('L', item.get('ltp', 0.0)))),
                close_price=item.get('close', item.get('c', item.get('C', item.get('ltp', item.get('close_price', 0.0))))),
                volume=item.get('volume', item.get('v', item.get('V', 0))),
                interval=interval,
                provider='DHAN',
                symbol=symbol or _clean(item.get('symbol')),
                source='DHAN_LIVE_FEED',
                exchange_segment=exchange_segment or _clean(item.get('exchangeSegment')),
                security_id=security_id or _clean(item.get('securityId')),
                instrument=instrument or _clean(item.get('instrument')),
                open_interest=item.get('open_interest', item.get('oi', 0)),
                is_closed=bool(item.get('is_closed', False)),
            )
        )
    return normalized


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        ts = _clean(row.get('timestamp'))
        if ts:
            deduped[ts] = row
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
        close_price = float(chunk[-1]['close'])
        aggregated.append(
            _build_normalized_candle(
                timestamp=bucket,
                open_price=chunk[0]['open'],
                high_price=max(float(item['high']) for item in chunk),
                low_price=min(float(item['low']) for item in chunk),
                close_price=close_price,
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
        close_price = float(chunk[-1]['close'])
        aggregated.append(
            _build_normalized_candle(
                timestamp=chunk[0]['_dt'],
                open_price=chunk[0]['open'],
                high_price=max(float(item['high']) for item in chunk),
                low_price=min(float(item['low']) for item in chunk),
                close_price=close_price,
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
    from src.dhan_api import DhanClient

    mode, base_interval, aggregate_to = _INTERVAL_ALIASES.get(interval, ('intraday', 5, None))
    instrument = _resolve_dhan_instrument(symbol, security_map=security_map)
    start_dt, end_dt = _period_to_range(period)
    cache_path = build_candle_cache_path(
        provider='DHAN',
        symbol=symbol,
        interval=interval,
        start_dt=start_dt,
        end_dt=end_dt,
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
        if mode == 'daily':
            rows = _aggregate_daily_rows(rows, aggregate_to, interval)
        else:
            rows = _aggregate_minute_rows(rows, aggregate_to)
    else:
        rows = [dict(row, interval=interval) for row in rows]

    if use_cache and rows:
        write_candle_cache(cache_path, rows)
    return rows


def _fetch_yfinance_ohlcv(
    symbol: str,
    interval: str,
    period: str,
    *,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    if yf is None:
        raise ModuleNotFoundError('yfinance is required for fetch_live_ohlcv (pip install yfinance)')

    start_dt, end_dt = _period_to_range(period)
    cache_path = build_candle_cache_path(
        provider='YAHOO',
        symbol=symbol,
        interval=interval,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    stale_rows = read_latest_candle_cache(
        provider='YAHOO',
        symbol=symbol,
        interval=interval,
        cache_dir=cache_dir,
    ) if use_cache else []

    if use_cache and not force_refresh:
        cached_rows = read_candle_cache(cache_path)
        if cached_rows:
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
        raise

    if df is None or getattr(df, 'empty', False):
        return stale_rows

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
    if 'timestamp' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'timestamp'})

    required = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    existing = [c for c in required if c in df.columns]
    df = df[existing].copy()

    rows: list[dict[str, Any]] = []
    for row in df.to_dict('records'):
        rows.append(
            _build_normalized_candle(
                timestamp=row.get('timestamp', ''),
                open_price=row.get('open', 0.0),
                high_price=row.get('high', 0.0),
                low_price=row.get('low', 0.0),
                close_price=row.get('close', 0.0),
                volume=row.get('volume', 0),
                interval=interval,
                provider='YAHOO',
                symbol=symbol,
                source='YAHOO_DOWNLOAD',
                is_closed=True,
            )
        )
    if use_cache and rows:
        write_candle_cache(cache_path, rows)
    if rows:
        return rows
    return stale_rows


def fetch_live_ohlcv(
    symbol: str,
    interval: str,
    period: str,
    *,
    provider: str | None = None,
    security_map: dict[str, Any] | None = None,
    broker_client: object | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_dir: Path | None = None,
) -> list[dict[str, Any]]:
    selected_provider = _clean(provider or os.getenv('MARKET_DATA_PROVIDER', DEFAULT_DATA_PROVIDER)).upper() or DEFAULT_DATA_PROVIDER
    if selected_provider == 'AUTO':
        try:
            yahoo_rows = _fetch_yfinance_ohlcv(
                symbol,
                interval,
                period,
                use_cache=use_cache,
                force_refresh=force_refresh,
                cache_dir=cache_dir,
            )
            if yahoo_rows:
                return yahoo_rows
        except Exception:
            pass
        return fetch_dhan_ohlcv(
            symbol,
            interval,
            period,
            security_map=security_map,
            broker_client=broker_client,
            use_cache=use_cache,
            force_refresh=force_refresh,
            cache_dir=cache_dir,
        )
    if selected_provider == 'DHAN':
        return fetch_dhan_ohlcv(
            symbol,
            interval,
            period,
            security_map=security_map,
            broker_client=broker_client,
            use_cache=use_cache,
            force_refresh=force_refresh,
            cache_dir=cache_dir,
        )
    return _fetch_yfinance_ohlcv(
        symbol,
        interval,
        period,
        use_cache=use_cache,
        force_refresh=force_refresh,
        cache_dir=cache_dir,
    )


def write_csv(rows: list[dict[str, Any]], path: str) -> None:
    if not rows:
        return

    keys = list(rows[0].keys())
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)



