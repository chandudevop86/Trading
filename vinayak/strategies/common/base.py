from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import uuid


STRICT_TRADE_SIGNAL_VERSION = 'strict_trade_signal_v1'
_ACTIONABLE_SIDES = {'BUY', 'SELL'}


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


@dataclass(slots=True)
class StrategySignal:
    strategy_name: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    target_price: float
    signal_time: datetime
    quantity: int = 0
    trade_id: str = ''
    zone_id: str = ''
    setup_type: str = ''
    timeframe: str = ''
    validation_status: str = 'PENDING'
    validation_score: float = 0.0
    validation_reasons: list[str] = field(default_factory=list)
    execution_allowed: bool = False
    contract_version: str = STRICT_TRADE_SIGNAL_VERSION
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        metadata = dict(self.metadata or {})
        self.symbol = str(self.symbol or '').strip().upper()
        self.side = str(self.side or '').strip().upper()
        self.strategy_name = str(self.strategy_name or '').strip() or 'UNKNOWN_STRATEGY'
        self.entry_price = round(_safe_float(self.entry_price), 4)
        self.stop_loss = round(_safe_float(self.stop_loss), 4)
        self.target_price = round(_safe_float(self.target_price), 4)
        self.quantity = _safe_int(self.quantity if self.quantity else metadata.get('quantity'), 0)
        self.zone_id = str(self.zone_id or metadata.get('zone_id') or '').strip()
        inferred_setup = metadata.get('setup_type') or metadata.get('zone_type') or self.strategy_name
        self.setup_type = str(self.setup_type or inferred_setup or '').strip().upper().replace(' ', '_')
        self.timeframe = str(self.timeframe or metadata.get('timeframe') or metadata.get('interval') or '').strip()
        status_value = metadata.get('validation_status') if str(self.validation_status).strip().upper() == 'PENDING' and metadata.get('validation_status') else self.validation_status
        self.validation_status = str(status_value or 'PENDING').strip().upper()
        self.validation_score = round(
            _safe_float(self.validation_score if self.validation_score else metadata.get('validation_score', metadata.get('score', 0.0))),
            2,
        )
        raw_reasons = self.validation_reasons or metadata.get('validation_reasons') or metadata.get('reason_codes') or []
        if isinstance(raw_reasons, str):
            reasons = [part.strip() for part in raw_reasons.split(',') if part.strip()]
        else:
            reasons = [str(item).strip() for item in list(raw_reasons or []) if str(item).strip()]
        self.validation_reasons = list(dict.fromkeys(reasons))
        if 'execution_allowed' in metadata:
            self.execution_allowed = bool(metadata.get('execution_allowed'))
        elif self.validation_status == 'PASS' and not self.validation_reasons:
            self.execution_allowed = True
        if self.side not in _ACTIONABLE_SIDES:
            self.execution_allowed = False
        if not self.trade_id:
            self.trade_id = self._build_trade_id()
        metadata.setdefault('quantity', self.quantity)
        metadata.setdefault('setup_type', self.setup_type)
        if self.zone_id:
            metadata.setdefault('zone_id', self.zone_id)
        metadata.setdefault('validation_status', self.validation_status)
        metadata.setdefault('validation_score', self.validation_score)
        metadata.setdefault('validation_reasons', list(self.validation_reasons))
        metadata.setdefault('execution_allowed', self.execution_allowed)
        metadata.setdefault('trade_id', self.trade_id)
        metadata.setdefault('contract_version', self.contract_version)
        self.metadata = metadata

    def _build_trade_id(self) -> str:
        zone_key = self.zone_id.strip().upper()
        if zone_key:
            return str(uuid.uuid5(uuid.NAMESPACE_URL, zone_key))
        payload = '|'.join([
            self.symbol,
            self.strategy_name.upper().replace(' ', '_'),
            self.signal_time.strftime('%Y-%m-%d %H:%M:%S'),
            self.side,
            f'{self.entry_price:.4f}',
            self.setup_type,
        ])
        return str(uuid.uuid5(uuid.NAMESPACE_URL, payload))

    def to_row(self) -> dict[str, Any]:
        timestamp = self.signal_time.strftime('%Y-%m-%d %H:%M:%S')
        return {
            'trade_id': self.trade_id,
            'strategy': self.strategy_name,
            'strategy_name': self.strategy_name,
            'symbol': self.symbol,
            'side': self.side,
            'entry': self.entry_price,
            'entry_price': self.entry_price,
            'stop_loss': self.stop_loss,
            'target': self.target_price,
            'target_price': self.target_price,
            'timestamp': timestamp,
            'entry_time': timestamp,
            'signal_time': timestamp,
            'quantity': self.quantity,
            'zone_id': self.zone_id,
            'setup_type': self.setup_type,
            'timeframe': self.timeframe,
            'validation_status': self.validation_status,
            'validation_score': self.validation_score,
            'validation_reasons': list(self.validation_reasons),
            'execution_allowed': self.execution_allowed,
            'contract_version': self.contract_version,
            **self.metadata,
        }


TradeSignal = StrategySignal

