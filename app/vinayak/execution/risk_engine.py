from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from math import floor
from typing import Any

import pandas as pd


_EXECUTED_STATUSES = {'FILLED', 'EXECUTED', 'SENT', 'ACCEPTED'}
_CLOSED_STATUSES = {'CLOSED', 'EXITED', 'CANCELLED', 'REJECTED'}


@dataclass(slots=True)
class PortfolioRiskConfig:
    capital: float
    per_trade_risk_pct: float | None = None
    max_position_value: float | None = None
    max_open_positions: int | None = None
    max_symbol_exposure_pct: float | None = None
    max_portfolio_exposure_pct: float | None = None
    max_open_risk_pct: float | None = None
    kill_switch_enabled: bool = False


@dataclass(slots=True)
class CapitalAllocationDecision:
    quantity: int
    requested_quantity: int
    block_reasons: list[str] = field(default_factory=list)
    adjustment_reasons: list[str] = field(default_factory=list)
    snapshot: dict[str, Any] = field(default_factory=dict)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    parsed = pd.to_datetime(raw, errors='coerce')
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).to_pydatetime()


def _is_open_execution(row: dict[str, Any]) -> bool:
    status = str(row.get('execution_status') or row.get('status') or row.get('trade_status') or '').upper()
    trade_status = str(row.get('trade_status') or '').upper()
    if trade_status in _CLOSED_STATUSES:
        return False
    return status in _EXECUTED_STATUSES


def _position_notional(row: dict[str, Any]) -> float:
    price = _safe_float(row.get('entry_price', row.get('price', row.get('entry', 0.0))))
    quantity = _safe_int(row.get('quantity', 0))
    return round(max(price, 0.0) * max(quantity, 0), 4)


def _position_risk(row: dict[str, Any]) -> float:
    entry = _safe_float(row.get('entry_price', row.get('entry', 0.0)))
    stop = _safe_float(row.get('stop_loss', row.get('stoploss', 0.0)))
    quantity = _safe_int(row.get('quantity', 0))
    return round(abs(entry - stop) * max(quantity, 0), 4)


def _open_rows(historical_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in historical_rows if _is_open_execution(dict(row))]


def _qty_limit(limit_value: float, per_unit_value: float) -> int:
    if limit_value <= 0 or per_unit_value <= 0:
        return 0
    return max(int(floor(limit_value / per_unit_value)), 0)


def allocate_position_size(
    candidate: dict[str, Any],
    historical_rows: list[dict[str, Any]],
    config: PortfolioRiskConfig,
) -> CapitalAllocationDecision:
    capital = max(_safe_float(config.capital, 0.0), 0.0)
    requested_quantity = max(_safe_int(candidate.get('quantity', 0)), 0)
    entry_price = _safe_float(candidate.get('entry_price', candidate.get('entry', 0.0)))
    risk_per_unit = abs(
        _safe_float(candidate.get('entry_price', candidate.get('entry', 0.0)))
        - _safe_float(candidate.get('stop_loss', candidate.get('stoploss', 0.0)))
    )
    open_rows = _open_rows(historical_rows)
    candidate_symbol = str(candidate.get('symbol') or '').upper()
    open_notional = sum(_position_notional(row) for row in open_rows)
    open_risk = sum(_position_risk(row) for row in open_rows)
    symbol_notional = sum(_position_notional(row) for row in open_rows if str(row.get('symbol') or '').upper() == candidate_symbol)

    snapshot = {
        'capital': round(capital, 4),
        'requested_quantity': requested_quantity,
        'entry_price': round(entry_price, 4),
        'risk_per_unit': round(risk_per_unit, 4),
        'open_positions': len(open_rows),
        'open_notional': round(open_notional, 4),
        'open_risk': round(open_risk, 4),
        'symbol_notional': round(symbol_notional, 4),
        'kill_switch_enabled': bool(config.kill_switch_enabled),
    }
    block_reasons: list[str] = []
    adjustment_reasons: list[str] = []

    if requested_quantity <= 0:
        block_reasons.append('INVALID_QUANTITY')
        return CapitalAllocationDecision(0, requested_quantity, block_reasons, adjustment_reasons, snapshot)

    if bool(config.kill_switch_enabled):
        block_reasons.append('PORTFOLIO_KILL_SWITCH_ACTIVE')
        return CapitalAllocationDecision(0, requested_quantity, block_reasons, adjustment_reasons, snapshot)

    max_open_positions = _safe_int(config.max_open_positions, 0)
    if max_open_positions > 0 and len(open_rows) >= max_open_positions:
        block_reasons.append('MAX_OPEN_POSITIONS')
        return CapitalAllocationDecision(0, requested_quantity, block_reasons, adjustment_reasons, snapshot)

    quantity_cap = requested_quantity

    per_trade_risk_pct = _safe_float(config.per_trade_risk_pct, 0.0)
    if capital > 0 and per_trade_risk_pct > 0 and risk_per_unit > 0:
        per_trade_risk_value = capital * (per_trade_risk_pct / 100.0)
        snapshot['per_trade_risk_value'] = round(per_trade_risk_value, 4)
        cap_qty = _qty_limit(per_trade_risk_value, risk_per_unit)
        quantity_cap = min(quantity_cap, cap_qty) if quantity_cap > 0 else cap_qty
        if cap_qty < requested_quantity:
            adjustment_reasons.append('CAPPED_BY_PER_TRADE_RISK')

    max_position_value = _safe_float(config.max_position_value, 0.0)
    if max_position_value > 0 and entry_price > 0:
        cap_qty = _qty_limit(max_position_value, entry_price)
        snapshot['max_position_value'] = round(max_position_value, 4)
        quantity_cap = min(quantity_cap, cap_qty) if quantity_cap > 0 else cap_qty
        if cap_qty < requested_quantity:
            adjustment_reasons.append('CAPPED_BY_MAX_POSITION_VALUE')

    max_portfolio_exposure_pct = _safe_float(config.max_portfolio_exposure_pct, 0.0)
    if capital > 0 and max_portfolio_exposure_pct > 0 and entry_price > 0:
        remaining = max(0.0, capital * (max_portfolio_exposure_pct / 100.0) - open_notional)
        snapshot['remaining_portfolio_exposure'] = round(remaining, 4)
        cap_qty = _qty_limit(remaining, entry_price)
        quantity_cap = min(quantity_cap, cap_qty) if quantity_cap > 0 else cap_qty
        if cap_qty < requested_quantity:
            adjustment_reasons.append('CAPPED_BY_PORTFOLIO_EXPOSURE')

    max_symbol_exposure_pct = _safe_float(config.max_symbol_exposure_pct, 0.0)
    if capital > 0 and max_symbol_exposure_pct > 0 and entry_price > 0:
        remaining = max(0.0, capital * (max_symbol_exposure_pct / 100.0) - symbol_notional)
        snapshot['remaining_symbol_exposure'] = round(remaining, 4)
        cap_qty = _qty_limit(remaining, entry_price)
        quantity_cap = min(quantity_cap, cap_qty) if quantity_cap > 0 else cap_qty
        if cap_qty < requested_quantity:
            adjustment_reasons.append('CAPPED_BY_SYMBOL_EXPOSURE')

    max_open_risk_pct = _safe_float(config.max_open_risk_pct, 0.0)
    if capital > 0 and max_open_risk_pct > 0 and risk_per_unit > 0:
        remaining = max(0.0, capital * (max_open_risk_pct / 100.0) - open_risk)
        snapshot['remaining_open_risk'] = round(remaining, 4)
        cap_qty = _qty_limit(remaining, risk_per_unit)
        quantity_cap = min(quantity_cap, cap_qty) if quantity_cap > 0 else cap_qty
        if cap_qty < requested_quantity:
            adjustment_reasons.append('CAPPED_BY_OPEN_RISK')

    if quantity_cap <= 0:
        if per_trade_risk_pct > 0:
            block_reasons.append('PER_TRADE_RISK_LIMIT')
        if max_position_value > 0:
            block_reasons.append('MAX_POSITION_VALUE')
        if max_portfolio_exposure_pct > 0:
            block_reasons.append('MAX_PORTFOLIO_EXPOSURE')
        if max_symbol_exposure_pct > 0:
            block_reasons.append('MAX_SYMBOL_EXPOSURE')
        if max_open_risk_pct > 0:
            block_reasons.append('MAX_OPEN_RISK')
        deduped_blocks: list[str] = []
        for reason in block_reasons:
            if reason and reason not in deduped_blocks:
                deduped_blocks.append(reason)
        return CapitalAllocationDecision(0, requested_quantity, deduped_blocks, adjustment_reasons, snapshot)

    return CapitalAllocationDecision(quantity_cap, requested_quantity, block_reasons, adjustment_reasons, snapshot)


def evaluate_portfolio_risk(
    candidate: dict[str, Any],
    historical_rows: list[dict[str, Any]],
    config: PortfolioRiskConfig,
) -> tuple[list[str], dict[str, Any]]:
    capital = max(_safe_float(config.capital, 0.0), 0.0)
    open_rows = _open_rows(historical_rows)
    candidate_symbol = str(candidate.get('symbol') or '').upper()
    candidate_notional = _position_notional(candidate)
    candidate_risk = _position_risk(candidate)
    open_notional = sum(_position_notional(row) for row in open_rows)
    open_risk = sum(_position_risk(row) for row in open_rows)
    symbol_notional = sum(_position_notional(row) for row in open_rows if str(row.get('symbol') or '').upper() == candidate_symbol)
    snapshot = {
        'capital': round(capital, 4),
        'candidate_notional': round(candidate_notional, 4),
        'candidate_risk': round(candidate_risk, 4),
        'open_positions': len(open_rows),
        'open_notional': round(open_notional, 4),
        'open_risk': round(open_risk, 4),
        'symbol_notional': round(symbol_notional, 4),
        'kill_switch_enabled': bool(config.kill_switch_enabled),
    }
    reasons: list[str] = []

    if bool(config.kill_switch_enabled):
        reasons.append('PORTFOLIO_KILL_SWITCH_ACTIVE')

    per_trade_risk_pct = _safe_float(config.per_trade_risk_pct, 0.0)
    if capital > 0 and per_trade_risk_pct > 0:
        per_trade_risk_cap = capital * (per_trade_risk_pct / 100.0)
        snapshot['per_trade_risk_cap'] = round(per_trade_risk_cap, 4)
        if candidate_risk > per_trade_risk_cap:
            reasons.append('PER_TRADE_RISK_LIMIT')

    max_position_value = _safe_float(config.max_position_value, 0.0)
    if max_position_value > 0 and candidate_notional > max_position_value:
        reasons.append('MAX_POSITION_VALUE')

    max_open_positions = _safe_int(config.max_open_positions, 0)
    if max_open_positions > 0 and len(open_rows) >= max_open_positions:
        reasons.append('MAX_OPEN_POSITIONS')

    max_portfolio_exposure_pct = _safe_float(config.max_portfolio_exposure_pct, 0.0)
    if capital > 0 and max_portfolio_exposure_pct > 0:
        portfolio_cap = capital * (max_portfolio_exposure_pct / 100.0)
        snapshot['portfolio_exposure_cap'] = round(portfolio_cap, 4)
        if open_notional + candidate_notional > portfolio_cap:
            reasons.append('MAX_PORTFOLIO_EXPOSURE')

    max_symbol_exposure_pct = _safe_float(config.max_symbol_exposure_pct, 0.0)
    if capital > 0 and max_symbol_exposure_pct > 0:
        symbol_cap = capital * (max_symbol_exposure_pct / 100.0)
        snapshot['symbol_exposure_cap'] = round(symbol_cap, 4)
        if symbol_notional + candidate_notional > symbol_cap:
            reasons.append('MAX_SYMBOL_EXPOSURE')

    max_open_risk_pct = _safe_float(config.max_open_risk_pct, 0.0)
    if capital > 0 and max_open_risk_pct > 0:
        risk_cap = capital * (max_open_risk_pct / 100.0)
        snapshot['open_risk_cap'] = round(risk_cap, 4)
        if open_risk + candidate_risk > risk_cap:
            reasons.append('MAX_OPEN_RISK')

    return reasons, snapshot


__all__ = ['CapitalAllocationDecision', 'PortfolioRiskConfig', 'allocate_position_size', 'evaluate_portfolio_risk']
