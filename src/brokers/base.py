from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class TradeCandidate:
    trade_id: str
    trade_key: str
    strategy: str
    symbol: str
    side: str
    quantity: int
    price: float
    signal_time: str
    reason: str
    execution_type: str
    stop_loss: float | None = None
    target: float | None = None
    order_type: str = 'MARKET'
    product_type: str = 'INTRADAY'
    validity: str = 'DAY'
    trigger_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BrokerOrderRequest:
    trade_id: str
    strategy: str
    symbol: str
    side: str
    quantity: int
    order_type: str
    product_type: str
    validity: str
    price: float | None = None
    trigger_price: float | None = None
    execution_type: str = 'PAPER'
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            'trade_id': self.trade_id,
            'strategy': self.strategy,
            'symbol': self.symbol,
            'side': self.side,
            'quantity': self.quantity,
            'order_type': self.order_type,
            'product_type': self.product_type,
            'validity': self.validity,
            'execution_type': self.execution_type,
        }
        if self.price is not None:
            payload['price'] = self.price
        if self.trigger_price is not None:
            payload['trigger_price'] = self.trigger_price
        payload.update(self.metadata)
        return payload


@dataclass(slots=True)
class BrokerOrderResult:
    broker_name: str
    order_id: str = ''
    status: str = 'PENDING'
    message: str = ''
    accepted: bool = False
    raw_response: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BrokerHealth:
    ok: bool
    broker_name: str
    message: str = ''
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BrokerBalance:
    available_cash: float = 0.0
    utilized_margin: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class BrokerError(RuntimeError):
    """Base broker exception used by the execution layer."""


class BrokerConfigurationError(BrokerError):
    """Raised when a broker adapter is not configured correctly."""


class BrokerExecutionError(BrokerError):
    """Raised when a broker call fails after validation."""


class Broker(ABC):
    name: str = 'BROKER'
    live: bool = False

    @abstractmethod
    def place_order(self, order_request: BrokerOrderRequest) -> BrokerOrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> BrokerOrderResult:
        raise NotImplementedError

    @abstractmethod
    def get_positions(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def get_balance(self) -> BrokerBalance:
        raise NotImplementedError

    @abstractmethod
    def get_orders(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> BrokerHealth:
        raise NotImplementedError


