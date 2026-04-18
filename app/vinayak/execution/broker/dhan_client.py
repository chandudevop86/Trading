from __future__ import annotations

import json
import os
from urllib import error, request

from vinayak.execution.broker.order_request import DhanOrderRequest


class DhanClientConfigError(RuntimeError):
    """Raised when Dhan credentials are missing."""


class DhanClientRequestError(RuntimeError):
    """Raised when the broker request fails."""


class DhanClient:
    def __init__(self, client_id: str | None, access_token: str | None, *, base_url: str | None = None, timeout: int | None = None) -> None:
        self.client_id = client_id 
        self.access_token = access_token
        self.base_url = str(base_url or os.getenv('DHAN_BASE_URL', 'https://api-hq.dhan.co')).rstrip('/')
        self.timeout = int(timeout or os.getenv('DHAN_TIMEOUT', '30'))

    def is_ready(self) -> bool:
        return bool(self.client_id and self.access_token)

    def _headers(self) -> dict[str, str]:
        return {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'access-token': str(self.access_token or ''),
            'client-id': str(self.client_id or ''),
        }

    def _request(self, method: str, path: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = json.dumps(payload).encode('utf-8') if payload is not None else None
        req = request.Request(url, data=data, headers=self._headers(), method=method.upper())
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode('utf-8')
        except error.HTTPError as exc:
            body = exc.read().decode('utf-8', errors='replace')
            raise DhanClientRequestError(f'Dhan API error {exc.code}: {body}') from exc
        except error.URLError as exc:
            raise DhanClientRequestError(f'Could not reach Dhan API: {exc.reason}') from exc

        if not raw.strip():
            return {}
        try:
            payload_obj = json.loads(raw)
            return payload_obj if isinstance(payload_obj, dict) else {'raw': payload_obj}
        except json.JSONDecodeError:
            return {'raw': raw}

    def place_order(self, order_request: DhanOrderRequest) -> dict[str, object]:
        if not self.is_ready():
            raise DhanClientConfigError('Dhan credentials are missing.')
        payload = order_request.to_payload()
        response = self._request('POST', '/orders', payload)
        response.setdefault('payload', payload)
        if 'broker_reference' not in response:
            response['broker_reference'] = str(response.get('orderId') or response.get('order_id') or '')
        return response

    @classmethod
    def from_env(cls) -> 'DhanClient':
        return cls(
            os.getenv('DHAN_CLIENT_ID'),
            os.getenv('DHAN_ACCESS_TOKEN'),
            base_url=os.getenv('DHAN_BASE_URL'),
            timeout=int(os.getenv('DHAN_TIMEOUT', '30')),
        )

    def get_historical_data(
        self,
        *,
        security_id: str | int,
        exchange_segment: str,
        instrument: str,
        from_date: str,
        to_date: str,
        expiry_code: int = 0,
        oi: bool = False,
    ) -> dict[str, object]:
        if not self.is_ready():
            raise DhanClientConfigError('Dhan credentials are missing.')
        return self._request(
            'POST',
            '/charts/historical',
            {
                'securityId': str(security_id),
                'exchangeSegment': str(exchange_segment).upper(),
                'instrument': str(instrument).upper(),
                'expiryCode': int(expiry_code),
                'oi': bool(oi),
                'fromDate': str(from_date),
                'toDate': str(to_date),
            },
        )

    def get_intraday_data(
        self,
        *,
        security_id: str | int,
        exchange_segment: str,
        instrument: str,
        interval: int,
        from_date: str,
        to_date: str,
        oi: bool = False,
    ) -> dict[str, object]:
        if not self.is_ready():
            raise DhanClientConfigError('Dhan credentials are missing.')
        return self._request(
            'POST',
            '/charts/intraday',
            {
                'securityId': str(security_id),
                'exchangeSegment': str(exchange_segment).upper(),
                'instrument': str(instrument).upper(),
                'interval': int(interval),
                'oi': bool(oi),
                'fromDate': str(from_date),
                'toDate': str(to_date),
            },
        )
