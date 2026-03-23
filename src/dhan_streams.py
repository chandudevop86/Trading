from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from src.live_ohlcv import normalize_dhan_live_payload


@dataclass(slots=True)
class MarketFeedEvent:
    symbol: str
    security_id: str
    exchange_segment: str
    instrument: str
    interval: str
    timestamp: str
    ltp: float
    volume: int = 0
    open_interest: int = 0
    bid_price: float = 0.0
    ask_price: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OrderUpdateEvent:
    order_id: str
    correlation_id: str
    status: str
    filled_qty: int
    remaining_qty: int
    average_price: float
    update_time: str
    traded_price: float = 0.0
    message: str = ''
    trade_id: str = ''
    symbol: str = ''
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CandleAggregator:
    symbol: str
    interval: str
    exchange_segment: str = ''
    security_id: str = ''
    instrument: str = ''
    provider: str = 'DHAN'
    source: str = 'DHAN_LIVE_FEED'
    _current_bucket: datetime | None = None
    _current_row: dict[str, Any] | None = None

    def ingest(self, event: MarketFeedEvent) -> list[dict[str, Any]]:
        event_dt = _parse_timestamp(event.timestamp)
        bucket = _bucket_start(event_dt, self.interval)
        closed_rows: list[dict[str, Any]] = []
        if self._current_bucket is None or self._current_row is None:
            self._current_bucket = bucket
            self._current_row = _start_row(event, bucket, self)
            return closed_rows

        if bucket != self._current_bucket:
            closed = dict(self._current_row)
            closed['is_closed'] = True
            closed_rows.append(closed)
            self._current_bucket = bucket
            self._current_row = _start_row(event, bucket, self)
            return closed_rows

        row = self._current_row
        price = float(event.ltp)
        row['high'] = round(max(float(row.get('high', price)), price), 4)
        row['low'] = round(min(float(row.get('low', price)), price), 4)
        row['close'] = round(price, 4)
        row['price'] = round(price, 4)
        row['volume'] = max(int(row.get('volume', 0) or 0), int(event.volume or 0))
        row['open_interest'] = int(event.open_interest or row.get('open_interest', 0) or 0)
        row['timestamp'] = bucket.strftime('%Y-%m-%d %H:%M:%S')
        row['is_closed'] = False
        return closed_rows

    def snapshot(self) -> dict[str, Any] | None:
        if self._current_row is None:
            return None
        return dict(self._current_row)


def normalize_dhan_market_feed_payload(
    payload: dict[str, Any],
    *,
    symbol: str = '',
    interval: str = '1m',
    exchange_segment: str = '',
    security_id: str = '',
    instrument: str = '',
) -> list[MarketFeedEvent]:
    rows = normalize_dhan_live_payload(
        payload,
        symbol=symbol,
        interval=interval,
        exchange_segment=exchange_segment,
        security_id=security_id,
        instrument=instrument,
    )
    events: list[MarketFeedEvent] = []
    data = payload.get('data') if isinstance(payload, dict) else None
    raw_items: list[dict[str, Any]] = []
    if isinstance(data, list):
        raw_items = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        raw_items = [data]
    elif isinstance(payload, dict):
        raw_items = [payload]

    for idx, row in enumerate(rows):
        raw = raw_items[idx] if idx < len(raw_items) else dict(payload)
        events.append(
            MarketFeedEvent(
                symbol=str(row.get('symbol', symbol) or symbol),
                security_id=str(row.get('security_id', security_id) or security_id),
                exchange_segment=str(row.get('exchange_segment', exchange_segment) or exchange_segment),
                instrument=str(row.get('instrument', instrument) or instrument),
                interval=str(row.get('interval', interval) or interval),
                timestamp=str(row.get('timestamp', '')),
                ltp=float(row.get('price', row.get('close', 0.0)) or 0.0),
                volume=int(float(row.get('volume', 0) or 0)),
                open_interest=int(float(row.get('open_interest', 0) or 0)),
                bid_price=_safe_float(raw.get('best_bid_price') or raw.get('bid_price') or raw.get('bp')),
                ask_price=_safe_float(raw.get('best_ask_price') or raw.get('ask_price') or raw.get('ap')),
                raw=dict(raw),
            )
        )
    return events


def normalize_dhan_order_update_payload(payload: dict[str, Any]) -> list[OrderUpdateEvent]:
    if not isinstance(payload, dict):
        return []

    data = payload.get('data')
    if isinstance(data, list):
        items = [item for item in data if isinstance(item, dict)]
    elif isinstance(data, dict):
        items = [data]
    else:
        items = [payload]

    events: list[OrderUpdateEvent] = []
    for item in items:
        order_id = _first_text(item, 'orderId', 'order_id', 'orderNo', 'oms_order_id')
        correlation_id = _first_text(item, 'correlationId', 'correlation_id', 'correlationid')
        status = _first_text(item, 'orderStatus', 'status', 'order_state').upper()
        filled_qty = _safe_int(item.get('filled_qty') or item.get('filledQty') or item.get('tradedQuantity') or item.get('quantityTraded'))
        total_qty = _safe_int(item.get('quantity') or item.get('orderQuantity') or item.get('qty'))
        remaining_qty = _safe_int(item.get('remaining_qty') or item.get('remainingQty') or max(total_qty - filled_qty, 0))
        average_price = _safe_float(item.get('average_price') or item.get('averagePrice') or item.get('avgTradedPrice') or item.get('price'))
        traded_price = _safe_float(item.get('traded_price') or item.get('tradedPrice') or item.get('lastTradedPrice'))
        update_time = _first_text(item, 'updateTime', 'updated_at', 'orderDateTime', 'exchangeTime', 'timestamp')
        message = _first_text(item, 'message', 'remarks', 'omsErrorDescription', 'errorMessage')
        trade_id = _first_text(item, 'trade_id', 'tradeId')
        symbol = _first_text(item, 'tradingSymbol', 'symbol', 'securitySymbol')
        events.append(
            OrderUpdateEvent(
                order_id=order_id,
                correlation_id=correlation_id,
                status=status,
                filled_qty=filled_qty,
                remaining_qty=remaining_qty,
                average_price=average_price,
                traded_price=traded_price,
                update_time=update_time,
                message=message,
                trade_id=trade_id,
                symbol=symbol,
                raw=dict(item),
            )
        )
    return events


def _start_row(event: MarketFeedEvent, bucket: datetime, aggregator: CandleAggregator) -> dict[str, Any]:
    price = round(float(event.ltp), 4)
    return {
        'timestamp': bucket.strftime('%Y-%m-%d %H:%M:%S'),
        'open': price,
        'high': price,
        'low': price,
        'close': price,
        'volume': int(event.volume or 0),
        'price': price,
        'interval': aggregator.interval,
        'provider': aggregator.provider,
        'symbol': event.symbol or aggregator.symbol,
        'source': aggregator.source,
        'exchange_segment': event.exchange_segment or aggregator.exchange_segment,
        'security_id': event.security_id or aggregator.security_id,
        'instrument': event.instrument or aggregator.instrument,
        'open_interest': int(event.open_interest or 0),
        'is_closed': False,
    }


def _bucket_start(value: datetime, interval: str) -> datetime:
    minutes = _interval_minutes(interval)
    normalized = value.replace(second=0, microsecond=0)
    minute = (normalized.minute // minutes) * minutes
    return normalized.replace(minute=minute)


def _interval_minutes(interval: str) -> int:
    raw = str(interval or '1m').strip().lower()
    if raw.endswith('m') and raw[:-1].isdigit():
        return max(1, int(raw[:-1]))
    if raw.endswith('h') and raw[:-1].isdigit():
        return max(1, int(raw[:-1]) * 60)
    return 1


def _parse_timestamp(value: str) -> datetime:
    text = str(value or '').strip()
    if not text:
        raise ValueError('timestamp is required')
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z'):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is not None:
                return dt.astimezone().replace(tzinfo=None)
            return dt
        except ValueError:
            continue
    return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)


def _safe_int(value: object) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _first_text(item: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = item.get(key)
        if value not in {None, ''}:
            return str(value)
    return ''