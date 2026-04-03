from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.brokers.base import (
    Broker,
    BrokerBalance,
    BrokerConfigurationError,
    BrokerExecutionError,
    BrokerHealth,
    BrokerOrderRequest,
    BrokerOrderResult,
)
from src.dhan_api import DhanClient, DhanExecutionError, build_order_request_from_candidate, load_security_map, resolve_security


@dataclass(slots=True)
class DhanBrokerConfig:
    security_map_path: Path = Path(os.getenv('DHAN_SECURITY_MAP', 'data/dhan_security_map.csv'))
    detailed_security_map_path: Path | None = Path(os.getenv('DHAN_SECURITY_MAP_DETAILED')) if os.getenv('DHAN_SECURITY_MAP_DETAILED') else None
    validate_with_option_chain: bool = True
    allow_live: bool = False


class DhanBroker(Broker):
    name = 'DHAN'
    live = True

    def __init__(self, client: DhanClient, *, security_map: dict[str, Any] | None = None, config: DhanBrokerConfig | None = None) -> None:
        self.client = client
        self.config = config or DhanBrokerConfig()
        self.security_map = security_map if security_map is not None else self._load_security_map()

    @classmethod
    def from_env(cls, *, allow_live: bool = False, security_map: dict[str, Any] | None = None) -> 'DhanBroker':
        if not allow_live:
            raise BrokerConfigurationError('Live trading is disabled. Set explicit live enablement before creating DhanBroker.')
        try:
            client = DhanClient.from_env()
        except Exception as exc:
            raise BrokerConfigurationError(f'Dhan credentials are unavailable: {exc}') from exc
        return cls(client, security_map=security_map, config=DhanBrokerConfig(allow_live=allow_live))

    def place_order(self, order_request: BrokerOrderRequest) -> BrokerOrderResult:
        if not self.config.allow_live:
            raise BrokerConfigurationError('Dhan live trading is not enabled for this broker instance.')
        candidate = dict(order_request.metadata)
        try:
            resolution = resolve_security(
                candidate,
                self.security_map,
                broker_client=self.client,
                validate_with_option_chain=self.config.validate_with_option_chain,
            )
            request = build_order_request_from_candidate(
                candidate,
                client_id=str(getattr(self.client, 'client_id', '') or ''),
                security_map=self.security_map,
                resolved_security=resolution,
                broker_client=self.client,
                validate_with_option_chain=self.config.validate_with_option_chain,
            )
            response = self.client.place_order(request)
        except DhanExecutionError as exc:
            raise BrokerExecutionError(str(exc)) from exc
        except Exception as exc:
            raise BrokerExecutionError(f'Dhan order placement failed: {exc}') from exc

        payload = response if isinstance(response, dict) else {'raw': str(response)}
        status = str(payload.get('orderStatus', payload.get('status', 'SENT')) or 'SENT').upper()
        message = str(payload.get('message', payload.get('remarks', payload.get('omsErrorDescription', '')) or ''))
        accepted = status not in {'REJECTED', 'FAILED', 'ERROR', 'CANCELLED', 'CANCELED'}
        metadata = dict(resolution)
        metadata['broker_response_json'] = json.dumps(payload, ensure_ascii=True)
        return BrokerOrderResult(
            broker_name=self.name,
            order_id=str(payload.get('orderId', payload.get('order_id', '')) or ''),
            status=status,
            message=message,
            accepted=accepted,
            raw_response=payload,
            metadata=metadata,
        )

    def cancel_order(self, order_id: str) -> BrokerOrderResult:
        if hasattr(self.client, 'cancel_order'):
            try:
                response = self.client.cancel_order(order_id)
                payload = response if isinstance(response, dict) else {'raw': str(response)}
                return BrokerOrderResult(
                    broker_name=self.name,
                    order_id=order_id,
                    status=str(payload.get('status', 'CANCELLED') or 'CANCELLED'),
                    message=str(payload.get('message', 'Cancelled') or 'Cancelled'),
                    accepted=True,
                    raw_response=payload,
                )
            except Exception as exc:
                raise BrokerExecutionError(f'Dhan cancel failed: {exc}') from exc
        raise BrokerExecutionError('Dhan client does not support cancel_order in this configuration.')

    def get_order_by_id(self, order_id: str) -> Any:
        if hasattr(self.client, 'get_order_by_id'):
            return self.client.get_order_by_id(order_id)
        raise BrokerExecutionError('Dhan client does not support get_order_by_id in this configuration.')

    def get_order_by_correlation_id(self, correlation_id: str) -> Any:
        if hasattr(self.client, 'get_order_by_correlation_id'):
            return self.client.get_order_by_correlation_id(correlation_id)
        raise BrokerExecutionError('Dhan client does not support get_order_by_correlation_id in this configuration.')

    def get_positions(self) -> list[dict[str, Any]]:
        raw = self.client.get_positions()
        return raw if isinstance(raw, list) else raw.get('data', []) if isinstance(raw, dict) else []

    def get_balance(self) -> BrokerBalance:
        if hasattr(self.client, 'get_fund_limits'):
            raw = self.client.get_fund_limits()
            if isinstance(raw, dict):
                return BrokerBalance(
                    available_cash=float(raw.get('availabelBalance', raw.get('availableBalance', 0.0)) or 0.0),
                    utilized_margin=float(raw.get('utilizedAmount', 0.0) or 0.0),
                    metadata=raw,
                )
        return BrokerBalance(metadata={'message': 'Balance endpoint unavailable'})

    def get_orders(self) -> list[dict[str, Any]]:
        if hasattr(self.client, 'get_orders'):
            raw = self.client.get_orders()
            return raw if isinstance(raw, list) else raw.get('data', []) if isinstance(raw, dict) else []
        return []

    def health_check(self) -> BrokerHealth:
        try:
            self.get_positions()
            return BrokerHealth(ok=True, broker_name=self.name, message='Dhan broker reachable')
        except Exception as exc:
            return BrokerHealth(ok=False, broker_name=self.name, message=str(exc))

    def _load_security_map(self) -> dict[str, Any]:
        try:
            return load_security_map(self.config.security_map_path, self.config.detailed_security_map_path)
        except Exception as exc:
            raise BrokerConfigurationError(f'Unable to load Dhan security map: {exc}') from exc


