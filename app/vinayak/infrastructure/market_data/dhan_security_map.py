"""Canonical Dhan instrument-map utilities for the app runtime."""

from __future__ import annotations

import csv
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

YAHOO_SYMBOL_ALIASES = {
    '^NSEI': 'NIFTY',
    'NSEI': 'NIFTY',
    'NIFTY50': 'NIFTY',
    'NIFTY 50': 'NIFTY',
    '^NSEBANK': 'BANKNIFTY',
    'NSEBANK': 'BANKNIFTY',
    'NIFTYBANK': 'BANKNIFTY',
    'BANK NIFTY': 'BANKNIFTY',
}

_SECURITY_MAP_CACHE: dict[tuple[str, float], dict[str, Any]] = {}


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


def _normalize_key(value: object) -> str:
    return ''.join(ch for ch in _clean(value).upper() if ch.isalnum())


def _format_strike(value: object) -> str:
    number = _safe_float(value, default=float('nan'))
    if number != number:
        text = _clean(value)
        if not text:
            return ''
        return text.rstrip('0').rstrip('.') if '.' in text else text
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f'{number:.4f}'.rstrip('0').rstrip('.')


def _normalize_option_type(value: object) -> str:
    raw = _clean(value).upper()
    if raw in {'CALL', 'CE'}:
        return 'CE'
    if raw in {'PUT', 'PE'}:
        return 'PE'
    return ''


def normalize_expiry(value: object) -> str:
    raw = _clean(value)
    if not raw:
        return ''
    for fmt in (
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%d-%m-%Y',
        '%d/%m/%Y',
        '%Y/%m/%d',
        '%d %b %Y',
        '%d %B %Y',
        '%d-%b-%Y',
        '%d-%B-%Y',
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace('Z', '+00:00')).date().isoformat()
    except ValueError:
        return raw


def normalize_trading_symbol(symbol: str) -> str:
    raw = _clean(symbol)
    if not raw:
        return ''
    upper = raw.upper()
    if upper in YAHOO_SYMBOL_ALIASES:
        return YAHOO_SYMBOL_ALIASES[upper]
    compact = re.sub(r'[^A-Z0-9]', '', upper)
    if compact in YAHOO_SYMBOL_ALIASES:
        return YAHOO_SYMBOL_ALIASES[compact]
    return compact or upper


def _csv_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and _clean(row[key]):
            return _clean(row[key])
    upper_map = {str(k).upper(): v for k, v in row.items()}
    for key in keys:
        value = upper_map.get(key.upper())
        if _clean(value):
            return _clean(value)
    return ''


def _infer_underlying_symbol(
    underlying_symbol: str,
    symbol_name: str,
    display_name: str,
    trading_symbol: str,
    instrument_name: str,
) -> str:
    explicit = normalize_trading_symbol(underlying_symbol)
    if explicit:
        return explicit

    instrument_norm = _clean(instrument_name).upper()
    for raw in (trading_symbol, display_name, symbol_name):
        text = _clean(raw)
        if not text:
            continue
        if instrument_norm.startswith('OPT') or instrument_norm.startswith('FUT'):
            if '-' in text:
                return normalize_trading_symbol(text.split('-', 1)[0])
            parts = text.split()
            if parts:
                return normalize_trading_symbol(parts[0])
        normalized = normalize_trading_symbol(text)
        if normalized:
            return normalized
    return ''


def _infer_exchange_segment(
    exchange: str,
    segment: str,
    instrument_name: str,
    instrument_type: str,
    symbol_name: str,
) -> str:
    exchange_norm = _clean(exchange).upper()
    segment_norm = _clean(segment).upper()
    instrument_norm = _clean(instrument_name).upper()
    instrument_type_norm = _clean(instrument_type).upper()
    symbol_norm = normalize_trading_symbol(symbol_name)

    if instrument_norm in {'INDEX', 'IDX', 'INDEXES'} or instrument_type_norm in {'INDEX', 'IDX_I'} or symbol_norm in {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'}:
        return 'IDX_I'
    if exchange_norm == 'NSE' and segment_norm == 'D':
        return 'NSE_FNO'
    if exchange_norm == 'NSE' and segment_norm == 'E':
        return 'NSE_EQ'
    if exchange_norm == 'BSE' and segment_norm == 'D':
        return 'BSE_FNO'
    if exchange_norm == 'BSE' and segment_norm == 'E':
        return 'BSE_EQ'
    if exchange_norm == 'MCX':
        return 'MCX_COMM'
    return _clean(exchange_norm or segment_norm)


def _standardize_security_row(row: dict[str, str]) -> dict[str, Any]:
    security_id = _csv_value(row, 'security_id', 'securityId', 'SEM_SMST_SECURITY_ID', 'SM_SECURITY_ID', 'SECURITY_ID')
    trading_symbol = _csv_value(row, 'trading_symbol', 'tradingSymbol', 'SEM_TRADING_SYMBOL', 'DISPLAY_NAME', 'SEM_CUSTOM_SYMBOL')
    display_name = _csv_value(row, 'display_name', 'DISPLAY_NAME', 'SEM_CUSTOM_SYMBOL') or trading_symbol
    symbol_name = _csv_value(row, 'symbol', 'symbol_name', 'SM_SYMBOL_NAME', 'SYMBOL_NAME') or display_name or trading_symbol
    exchange = _csv_value(row, 'exchange', 'EXCH_ID', 'SEM_EXM_EXCH_ID')
    segment = _csv_value(row, 'segment', 'SEGMENT', 'SEM_SEGMENT')
    instrument_name = _csv_value(row, 'instrument', 'INSTRUMENT', 'SEM_INSTRUMENT_NAME')
    underlying_symbol = _infer_underlying_symbol(
        _csv_value(row, 'underlying', 'underlying_symbol', 'UNDERLYING_SYMBOL'),
        symbol_name,
        display_name,
        trading_symbol,
        instrument_name,
    )
    instrument_type = _csv_value(row, 'instrument_type', 'INSTRUMENT_TYPE', 'SEM_EXCH_INSTRUMENT_TYPE') or instrument_name
    exchange_segment = _csv_value(row, 'exchange_segment', 'exchangeSegment') or _infer_exchange_segment(exchange, segment, instrument_name, instrument_type, underlying_symbol or symbol_name)
    expiry_date = normalize_expiry(_csv_value(row, 'expiry_date', 'SM_EXPIRY_DATE', 'SEM_EXPIRY_DATE', 'drv_expiry_date'))
    option_type = _normalize_option_type(_csv_value(row, 'option_type', 'OPTION_TYPE', 'SEM_OPTION_TYPE', 'drv_option_type'))
    strike_price = _format_strike(_csv_value(row, 'strike_price', 'STRIKE_PRICE', 'SEM_STRIKE_PRICE', 'drv_strike_price'))

    return {
        'security_id': security_id,
        'trading_symbol': trading_symbol or display_name or symbol_name,
        'display_name': display_name,
        'symbol_name': symbol_name,
        'underlying_symbol': normalize_trading_symbol(underlying_symbol or symbol_name),
        'exchange_segment': exchange_segment.upper(),
        'instrument_type': instrument_type.upper(),
        'instrument_name': instrument_name.upper(),
        'expiry_date': expiry_date,
        'strike_price': strike_price,
        'option_type': option_type,
        'lot_size': _safe_int(_csv_value(row, 'lot_size', 'LOT_SIZE', 'SEM_LOT_UNITS'), default=0),
        'raw_row': dict(row),
    }


def _instrument_aliases(record: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    candidates = [
        record.get('trading_symbol'),
        record.get('display_name'),
        record.get('symbol_name'),
        record.get('underlying_symbol'),
        record.get('security_id'),
    ]
    for raw in candidates:
        cleaned = _clean(raw)
        if cleaned and cleaned not in aliases:
            aliases.append(cleaned)
        normalized = _normalize_key(raw)
        if normalized and normalized not in aliases:
            aliases.append(normalized)
    return aliases


def _build_security_map(records: list[dict[str, Any]], *, source_path: str) -> dict[str, Any]:
    security_map: dict[str, Any] = {}
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        underlying = str(record.get('underlying_symbol', '') or '')
        if underlying:
            by_underlying.setdefault(underlying, []).append(record)
        for alias in _instrument_aliases(record):
            security_map.setdefault(alias, record)
    security_map['__meta__'] = {
        'source_path': source_path,
        'loaded_at_utc': datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S'),
        'records': records,
        'by_underlying': by_underlying,
    }
    return security_map


def load_security_map(csv_path: str | Path | None = None) -> dict[str, Any]:
    compact_path = Path(csv_path or os.getenv('DHAN_SECURITY_MAP', 'data/dhan_security_map.csv'))
    if not compact_path.exists():
        raise FileNotFoundError(f'Dhan security map CSV not found: {compact_path}')
    cache_key = (str(compact_path.resolve()), compact_path.stat().st_mtime)
    cached = _SECURITY_MAP_CACHE.get(cache_key)
    if cached is not None:
        return cached

    records: list[dict[str, Any]] = []
    with compact_path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {str(key).strip(): _clean(value) for key, value in raw_row.items() if key is not None}
            record = _standardize_security_row(row)
            if _clean(record.get('security_id')):
                records.append(record)

    built = _build_security_map(records, source_path=str(compact_path))
    _SECURITY_MAP_CACHE.clear()
    _SECURITY_MAP_CACHE[cache_key] = built
    return built


def find_cash_instrument(
    security_map: dict[str, Any] | None,
    symbol: str,
    *,
    exchange_segment: str | None = None,
    instrument_type: str | None = None,
) -> dict[str, Any] | None:
    if not security_map:
        return None
    normalized_symbol = normalize_trading_symbol(symbol)
    meta = security_map.get('__meta__') if isinstance(security_map, dict) else {}
    candidates = list(meta.get('by_underlying', {}).get(normalized_symbol, [])) if isinstance(meta, dict) else []
    if not candidates:
        direct = security_map.get(normalized_symbol) or security_map.get(_normalize_key(normalized_symbol))
        if isinstance(direct, dict):
            candidates = [direct]
    if not candidates:
        return None

    exchange_filter = _clean(exchange_segment).upper()
    instrument_filter = _clean(instrument_type).upper()
    filtered: list[dict[str, Any]] = []
    for record in candidates:
        if _normalize_option_type(record.get('option_type')):
            continue
        if exchange_filter and _clean(record.get('exchange_segment')).upper() != exchange_filter:
            continue
        if instrument_filter and instrument_filter not in _clean(record.get('instrument_type')).upper():
            continue
        filtered.append(record)
    return filtered[0] if filtered else candidates[0]


__all__ = ['find_cash_instrument', 'load_security_map', 'normalize_expiry', 'normalize_trading_symbol']
