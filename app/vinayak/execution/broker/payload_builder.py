from __future__ import annotations

import csv
import os
from pathlib import Path

from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.execution.broker.order_request import DhanOrderRequest


def _clean(value: object) -> str:
    return str(value or '').strip()


def _normalize(value: object) -> str:
    return ''.join(ch for ch in _clean(value).upper() if ch.isalnum())


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _aliases_for_row(row: dict[str, str]) -> list[str]:
    aliases: list[str] = []
    for key in (
        'alias',
        'aliases',
        'symbol',
        'underlying',
        'trading_symbol',
        'tradingSymbol',
        'contract_symbol',
        'contractSymbol',
        'instrument',
        'display_name',
    ):
        raw = _clean(row.get(key))
        if not raw:
            continue
        for part in [part.strip() for part in raw.replace('|', ',').split(',')]:
            normalized = _normalize(part)
            if normalized and normalized not in aliases:
                aliases.append(normalized)
    return aliases


def load_security_map(path: Path) -> dict[str, dict[str, str]]:
    security_map: dict[str, dict[str, str]] = {}
    with Path(path).open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {str(key).strip(): _clean(value) for key, value in raw_row.items() if key is not None}
            if not row:
                continue
            aliases = _aliases_for_row(row)
            security_id = row.get('security_id') or row.get('securityId') or row.get('sem_smst_security_id')
            if not aliases or not security_id:
                continue
            row.setdefault('security_id', security_id)
            row.setdefault('exchange_segment', row.get('exchangeSegment', row.get('exchange_segment', row.get('exchange', 'NSE_FNO'))))
            row.setdefault('product_type', row.get('productType', row.get('product_type', 'INTRADAY')))
            row.setdefault('order_type', row.get('orderType', row.get('order_type', 'MARKET')))
            for alias in aliases:
                security_map[alias] = dict(row)
    return security_map


def _default_security_map() -> dict[str, dict[str, str]]:
    raw_path = str(os.getenv('DHAN_SECURITY_MAP', 'data/dhan_security_map.csv') or 'data/dhan_security_map.csv').strip()
    path = Path(raw_path)
    if not path.exists():
        raise ValueError(f'Dhan security map was not found at {path}.')
    return load_security_map(path)


def _candidate_keys(reviewed_trade: ReviewedTradeRecord | None, signal: SignalRecord | None) -> list[str]:
    keys: list[str] = []
    values = [
        reviewed_trade.symbol if reviewed_trade is not None else '',
        signal.symbol if signal is not None else '',
        reviewed_trade.strategy_name if reviewed_trade is not None else '',
    ]
    for raw in values:
        normalized = _normalize(raw)
        if normalized and normalized not in keys:
            keys.append(normalized)
    return keys


def _resolve_security(reviewed_trade: ReviewedTradeRecord | None, signal: SignalRecord | None, security_map: dict[str, dict[str, str]]) -> dict[str, str]:
    normalized_map = {(_normalize(key)): dict(value) for key, value in security_map.items()}
    for key in _candidate_keys(reviewed_trade, signal):
        if key in normalized_map:
            return dict(normalized_map[key])
    symbol = reviewed_trade.symbol if reviewed_trade is not None else (signal.symbol if signal is not None else '')
    raise ValueError(f'No Dhan security map match found for symbol={symbol}.')


def build_dhan_order_request(
    *,
    reviewed_trade: ReviewedTradeRecord | None,
    signal: SignalRecord | None,
    fallback_price: float | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
) -> DhanOrderRequest:
    if reviewed_trade is None and signal is None:
        raise ValueError('reviewed_trade or signal is required to build a Dhan order request.')

    resolved_security_map = security_map if security_map is not None else _default_security_map()
    security = _resolve_security(reviewed_trade, signal, resolved_security_map)

    symbol = reviewed_trade.symbol if reviewed_trade is not None else signal.symbol
    side = reviewed_trade.side if reviewed_trade is not None else signal.side
    quantity = reviewed_trade.quantity if reviewed_trade is not None else 1
    entry_price = fallback_price
    if entry_price is None and reviewed_trade is not None:
        entry_price = reviewed_trade.entry_price
    if entry_price is None and signal is not None:
        entry_price = signal.entry_price

    metadata = {
        'strategy_name': reviewed_trade.strategy_name if reviewed_trade is not None else signal.strategy_name,
        'reviewed_trade_id': reviewed_trade.id if reviewed_trade is not None else None,
        'signal_id': reviewed_trade.signal_id if reviewed_trade is not None else signal.id,
        'symbol': symbol,
    }

    trading_symbol = (
        _clean(security.get('trading_symbol'))
        or _clean(security.get('tradingSymbol'))
        or _clean(security.get('contract_symbol'))
        or _clean(security.get('contractSymbol'))
        or symbol
    )

    order_type = _clean(security.get('order_type') or 'MARKET').upper()
    product_type = _clean(security.get('product_type') or 'INTRADAY').upper()
    price = 0.0 if order_type == 'MARKET' else _safe_float(entry_price, default=0.0)

    return DhanOrderRequest(
        security_id=_clean(security.get('security_id')),
        exchange_segment=_clean(security.get('exchange_segment') or 'NSE_FNO').upper(),
        transaction_type='BUY' if str(side).upper() == 'BUY' else 'SELL',
        quantity=int(quantity),
        order_type=order_type,
        product_type=product_type,
        price=price,
        trigger_price=0.0,
        validity='DAY',
        trading_symbol=trading_symbol,
        tag=f"VINAYAK-{_clean(metadata.get('strategy_name'))}"[:50],
        drv_expiry_date=_clean(security.get('expiry_date') or security.get('drv_expiry_date')),
        drv_option_type=_clean(security.get('option_type') or security.get('drv_option_type')).upper(),
        drv_strike_price=_safe_float(security.get('strike_price') or security.get('drv_strike_price'), default=0.0),
        metadata=metadata,
    )

