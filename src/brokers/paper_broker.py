from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from src.runtime_persistence import persist_row

from src.brokers.base import Broker, BrokerBalance, BrokerHealth, BrokerOrderRequest, BrokerOrderResult


@dataclass(slots=True)
class PaperBrokerConfig:
    orders_path: Path = Path('data/order_history.csv')
    broker_name: str = 'PAPER'


class PaperBroker(Broker):
    name = 'PAPER'
    live = False

    def __init__(self, config: PaperBrokerConfig | None = None) -> None:
        self.config = config or PaperBrokerConfig()
        self.config.orders_path.parent.mkdir(parents=True, exist_ok=True)

    def place_order(self, order_request: BrokerOrderRequest) -> BrokerOrderResult:
        order_id = f'PAPER-{uuid4().hex[:12].upper()}'
        submitted_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
        record = {
            'order_id': order_id,
            'trade_id': order_request.trade_id,
            'broker_name': self.name,
            'status': 'EXECUTED',
            'message': 'Paper order simulated successfully',
            'symbol': order_request.symbol,
            'side': order_request.side,
            'quantity': order_request.quantity,
            'price': order_request.price,
            'order_type': order_request.order_type,
            'product_type': order_request.product_type,
            'validity': order_request.validity,
            'execution_type': order_request.execution_type,
            'submitted_at_utc': submitted_at,
        }
        self._append_order_history(record)
        return BrokerOrderResult(
            broker_name=self.name,
            order_id=order_id,
            status='EXECUTED',
            message='Paper order simulated successfully',
            accepted=True,
            raw_response=record,
            metadata={'submitted_at_utc': submitted_at},
        )

    def cancel_order(self, order_id: str) -> BrokerOrderResult:
        return BrokerOrderResult(
            broker_name=self.name,
            order_id=order_id,
            status='CANCELLED',
            message='Paper order cancelled',
            accepted=True,
        )

    def get_positions(self) -> list[dict[str, Any]]:
        return []

    def get_balance(self) -> BrokerBalance:
        return BrokerBalance(available_cash=0.0, utilized_margin=0.0, metadata={'mode': 'paper'})

    def get_orders(self) -> list[dict[str, Any]]:
        if not self.config.orders_path.exists() or self.config.orders_path.stat().st_size == 0:
            return []
        frame = pd.read_csv(self.config.orders_path)
        return frame.to_dict(orient='records')

    def health_check(self) -> BrokerHealth:
        return BrokerHealth(ok=True, broker_name=self.name, message='Paper broker ready')

    def _append_order_history(self, record: dict[str, Any]) -> None:
        path = self.config.orders_path
        if path.exists() and path.stat().st_size > 0:
            existing = pd.read_csv(path)
            frame = pd.concat([existing, pd.DataFrame([record])], ignore_index=True)
        else:
            frame = pd.DataFrame([record])
        frame.to_csv(path, index=False)
        try:
            persist_row(path, record)
        except Exception:
            pass
