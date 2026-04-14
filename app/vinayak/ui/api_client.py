from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class VinayakApiClient:
    base_url: str
    timeout_seconds: float = 10.0

    @property
    def resolved_base_url(self) -> str:
        raw = str(self.base_url or '').strip().rstrip('/')
        if not raw:
            return 'http://127.0.0.1/api'
        if raw.startswith('/'):
            return f'http://127.0.0.1{raw}'
        if '://' not in raw:
            return f'http://{raw}'
        return raw

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.resolved_base_url, timeout=self.timeout_seconds)

    def _request(self, method: str, path: str, *, json: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            with self._client() as client:
                response = client.request(method, path, json=json)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            payload: dict[str, Any] = {'error': f'{exc.response.status_code} {exc.response.reason_phrase}'}
            try:
                payload['detail'] = exc.response.json()
            except Exception:
                payload['detail'] = exc.response.text
            return payload
        except httpx.HTTPError as exc:
            return {'error': 'API_UNAVAILABLE', 'detail': str(exc)}

    def health(self) -> dict[str, Any]:
        return self._request('GET', '/health')

    def dashboard_summary(self) -> dict[str, Any]:
        return self._request('GET', '/dashboard/summary')

    def run_signals(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request('POST', '/signals/run', json=payload)

    def request_execution(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request('POST', '/execution/request', json=payload)

    def admin_validation(self) -> dict[str, Any]:
        return self._request('GET', '/admin/api/validation')

    def admin_execution(self) -> dict[str, Any]:
        return self._request('GET', '/admin/api/execution')

    def admin_logs(self) -> dict[str, Any]:
        return self._request('GET', '/admin/api/logs')
