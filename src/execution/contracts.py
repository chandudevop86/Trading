from __future__ import annotations

import hashlib
import uuid
from dataclasses import asdict, dataclass
from typing import Any, TypedDict

import pandas as pd


CONTRACT_VERSION = 'strict_trade_candidate_v1'
REQUIRED_CANDIDATE_FIELDS = [
    'trade_id',
    'symbol',
    'timestamp',
    'strategy_name',
    'setup_type',
    'zone_id',
    'side',
    'entry',
    'stop_loss',
    'target',
    'entry_price',
    'target_price',
    'quantity',
    'timeframe',
    'validation_status',
    'validation_score',
    'validation_reasons',
    'execution_allowed',
]


class StrictTradeCandidateDict(TypedDict, total=False):
    trade_id: str
    symbol: str
    timestamp: str
    strategy_name: str
    setup_type: str
    zone_id: str
    side: str
    entry: float
    stop_loss: float
    target: float
    entry_price: float
    target_price: float
    quantity: int
    timeframe: str
    validation_status: str
    validation_score: float
    validation_reasons: list[str]
    execution_allowed: bool
    zone_type: str
    rr_ratio: float
    vwap_alignment: bool
    structure_score: float
    retest_score: float
    trend_alignment: bool
    session_tag: str
    source_strategy_version: str
    contract_version: str


@dataclass(slots=True)
class StrictTradeCandidate:
    trade_id: str
    symbol: str
    timestamp: str
    strategy_name: str
    setup_type: str
    zone_id: str
    side: str
    entry: float
    stop_loss: float
    target: float
    entry_price: float
    target_price: float
    quantity: int
    timeframe: str
    validation_status: str = 'PENDING'
    validation_score: float = 0.0
    validation_reasons: list[str] | None = None
    execution_allowed: bool = False
    zone_type: str = ''
    rr_ratio: float = 0.0
    vwap_alignment: bool | None = None
    structure_score: float = 0.0
    retest_score: float = 0.0
    trend_alignment: bool | None = None
    session_tag: str = ''
    source_strategy_version: str = CONTRACT_VERSION
    contract_version: str = CONTRACT_VERSION

    def to_dict(self) -> StrictTradeCandidateDict:
        payload = asdict(self)
        payload['validation_reasons'] = list(payload.get('validation_reasons') or [])
        return payload


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_timestamp(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    parsed = pd.to_datetime(raw, errors='coerce')
    if pd.isna(parsed):
        return raw
    return pd.Timestamp(parsed).strftime('%Y-%m-%d %H:%M:%S')


def _canonical_strategy_name(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return 'UNKNOWN_STRATEGY'
    return raw.upper().replace(' ', '_')


def _infer_setup_type(candidate: dict[str, Any]) -> str:
    for key in ('setup_type', 'zone_type', 'strategy_name', 'strategy', 'source_strategy'):
        raw = str(candidate.get(key, '') or '').strip()
        if raw:
            return raw.upper().replace(' ', '_')
    return 'GENERIC'


def _build_zone_id(candidate: dict[str, Any], strategy_name: str, setup_type: str, timestamp: str, side: str, symbol: str) -> str:
    existing = str(candidate.get('zone_id', candidate.get('setup_id', '')) or '').strip()
    if existing:
        return existing
    payload = '|'.join([symbol.upper(), strategy_name, setup_type, timestamp, side.upper()])
    digest = hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]
    return f'{symbol.upper()}_{strategy_name}_{digest}'


def _build_trade_id(candidate: dict[str, Any], zone_id: str, symbol: str, strategy_name: str, timestamp: str, side: str, entry: float) -> str:
    existing = str(candidate.get('trade_id', '') or '').strip()
    if existing:
        return existing
    if zone_id:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, zone_id.upper()))
    payload = '|'.join([symbol.upper(), strategy_name, timestamp, side.upper(), f'{entry:.6f}'])
    return str(uuid.uuid5(uuid.NAMESPACE_URL, payload))


def normalize_candidate_contract(
    candidate: dict[str, Any],
    *,
    symbol: str | None = None,
    strategy_name: str | None = None,
    timeframe: str | None = None,
) -> StrictTradeCandidateDict:
    raw = dict(candidate)
    for drop_key in ('pnl', 'gross_pnl', 'exit_time', 'exit_reason'):
        raw.pop(drop_key, None)
    resolved_symbol = str(raw.get('symbol') or symbol or 'UNKNOWN').strip().upper() or 'UNKNOWN'
    resolved_strategy = _canonical_strategy_name(raw.get('strategy_name') or raw.get('strategy') or raw.get('source_strategy') or strategy_name)
    resolved_timestamp = _normalize_timestamp(raw.get('timestamp') or raw.get('signal_time') or raw.get('entry_time') or raw.get('time'))
    resolved_side = str(raw.get('side') or raw.get('type') or '').strip().upper()
    resolved_setup_type = _infer_setup_type(raw)
    resolved_timeframe = str(raw.get('timeframe') or raw.get('interval') or timeframe or 'NA').strip() or 'NA'
    entry = _safe_float(raw.get('entry', raw.get('entry_price', raw.get('price', raw.get('close', 0.0)))))
    stop_loss = _safe_float(raw.get('stop_loss', raw.get('stoploss', raw.get('sl', raw.get('trailing_stop_loss', 0.0)))))
    target = _safe_float(raw.get('target', raw.get('target_price', raw.get('tp', 0.0))))
    quantity = int(round(_safe_float(raw.get('quantity', 0))))
    rr_ratio = _safe_float(raw.get('rr_ratio', 0.0))
    if rr_ratio <= 0 and entry > 0 and stop_loss > 0 and target > 0:
        risk = abs(entry - stop_loss)
        rr_ratio = abs(target - entry) / risk if risk > 1e-9 else 0.0
    validation_reasons = raw.get('validation_reasons', raw.get('fail_reasons', raw.get('reason_codes', [])))
    if isinstance(validation_reasons, str):
        validation_reasons = [part.strip() for part in validation_reasons.split(',') if part.strip()]
    elif not isinstance(validation_reasons, list):
        validation_reasons = []

    zone_id = _build_zone_id(raw, resolved_strategy, resolved_setup_type, resolved_timestamp, resolved_side, resolved_symbol)
    trade_id = _build_trade_id(raw, zone_id, resolved_symbol, resolved_strategy, resolved_timestamp, resolved_side, entry)
    normalized: StrictTradeCandidateDict = {
        'trade_id': trade_id,
        'symbol': resolved_symbol,
        'timestamp': resolved_timestamp,
        'strategy_name': resolved_strategy,
        'setup_type': resolved_setup_type,
        'zone_id': zone_id,
        'side': resolved_side,
        'entry': round(entry, 4),
        'stop_loss': round(stop_loss, 4),
        'target': round(target, 4),
        'entry_price': round(entry, 4),
        'target_price': round(target, 4),
        'quantity': quantity,
        'timeframe': resolved_timeframe,
        'validation_status': str(raw.get('validation_status', 'PENDING') or 'PENDING').strip().upper(),
        'validation_score': round(_safe_float(raw.get('validation_score', raw.get('score', 0.0))), 2),
        'validation_reasons': [str(item) for item in validation_reasons if str(item).strip()],
        'execution_allowed': bool(raw.get('execution_allowed', False)),
        'zone_type': str(raw.get('zone_type', '') or ''),
        'rr_ratio': round(rr_ratio, 4),
        'vwap_alignment': raw.get('vwap_alignment') if 'vwap_alignment' in raw else None,
        'structure_score': round(_safe_float(raw.get('structure_score', 0.0)), 2),
        'retest_score': round(_safe_float(raw.get('retest_score', raw.get('retest_quality', 0.0))), 2),
        'trend_alignment': raw.get('trend_alignment') if 'trend_alignment' in raw else None,
        'session_tag': str(raw.get('session_tag', '') or ''),
        'source_strategy_version': str(raw.get('source_strategy_version', raw.get('contract_version', CONTRACT_VERSION)) or CONTRACT_VERSION),
        'contract_version': CONTRACT_VERSION,
    }
    normalized.update(raw)
    normalized.update({
        'trade_id': str(normalized.get('trade_id', '') or trade_id),
        'symbol': resolved_symbol,
        'timestamp': resolved_timestamp,
        'strategy_name': resolved_strategy,
        'setup_type': resolved_setup_type,
        'zone_id': zone_id,
        'side': resolved_side,
        'entry': round(entry, 4),
        'entry_price': round(entry, 4),
        'stop_loss': round(stop_loss, 4),
        'stoploss': round(stop_loss, 4),
        'target': round(target, 4),
        'target_price': round(target, 4),
        'quantity': quantity,
        'timeframe': resolved_timeframe,
        'validation_status': normalized['validation_status'],
        'validation_score': normalized['validation_score'],
        'validation_reasons': list(normalized['validation_reasons']),
        'execution_allowed': bool(normalized['execution_allowed']),
        'rr_ratio': round(rr_ratio, 4),
        'strategy': resolved_strategy,
        'signal_time': str(raw.get('signal_time') or resolved_timestamp),
        'entry_time': str(raw.get('entry_time') or resolved_timestamp),
        'contract_version': CONTRACT_VERSION,
    })
    return normalized


def validate_candidate_contract(candidate: dict[str, Any]) -> tuple[bool, list[str], StrictTradeCandidateDict]:
    raw = dict(candidate)
    normalized = normalize_candidate_contract(raw)
    reasons: list[str] = []

    for field in REQUIRED_CANDIDATE_FIELDS:
        if field not in raw:
            reasons.append(f'MISSING_{field.upper()}')
            continue
        value = raw.get(field)
        if field == 'validation_reasons':
            if not isinstance(value, list):
                reasons.append('INVALID_VALIDATION_REASONS')
            continue
        if field == 'execution_allowed':
            if not isinstance(value, bool):
                reasons.append('INVALID_EXECUTION_ALLOWED')
            continue
        if value in (None, ''):
            reasons.append(f'MISSING_{field.upper()}')

    if str(normalized.get('trade_id', '')).strip() == '':
        reasons.append('INVALID_TRADE_ID')
    if str(normalized.get('side', '')).upper() not in {'BUY', 'SELL'}:
        reasons.append('INVALID_SIDE')
    if _safe_float(raw.get('validation_score', normalized.get('validation_score'))) <= 0:
        reasons.append('INVALID_VALIDATION_SCORE')
    if _safe_float(normalized.get('entry')) <= 0:
        reasons.append('INVALID_ENTRY')
    if _safe_float(normalized.get('stop_loss')) <= 0:
        reasons.append('INVALID_STOP_LOSS')
    if _safe_float(normalized.get('target')) <= 0:
        reasons.append('INVALID_TARGET')
    if int(_safe_float(normalized.get('quantity'))) <= 0:
        reasons.append('INVALID_QUANTITY')
    if str(normalized.get('timestamp', '')).strip() == '':
        reasons.append('INVALID_TIMESTAMP')

    validation_status = str(raw.get('validation_status', normalized.get('validation_status', '')) or '').upper()
    if validation_status not in {'PASS', 'FAIL', 'PENDING'}:
        reasons.append('INVALID_VALIDATION_STATUS')

    unique_reasons: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if reason not in seen:
            seen.add(reason)
            unique_reasons.append(reason)
    return len(unique_reasons) == 0, unique_reasons, normalized


__all__ = [
    'CONTRACT_VERSION',
    'REQUIRED_CANDIDATE_FIELDS',
    'StrictTradeCandidate',
    'StrictTradeCandidateDict',
    'normalize_candidate_contract',
    'validate_candidate_contract',
]
