from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class VinayakApiClient:
    base_url: str
    timeout_seconds: float = 10.0

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url.rstrip('/'), timeout=self.timeout_seconds)

    def health(self) -> dict[str, Any]:
        with self._client() as client:
            return client.get('/health').json()

    def dashboard_summary(self) -> dict[str, Any]:
        with self._client() as client:
            return client.get('/dashboard/summary').json()

    def run_signals(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._client() as client:
            response = client.post('/signals/run', json=payload)
            response.raise_for_status()
            return response.json()

    def request_execution(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._client() as client:
            response = client.post('/execution/request', json=payload)
            response.raise_for_status()
            return response.json()

    def admin_validation(self) -> dict[str, Any]:
        with self._client() as client:
            return client.get('/admin/api/validation').json()

    def admin_execution(self) -> dict[str, Any]:
        with self._client() as client:
            return client.get('/admin/api/execution').json()

    def admin_logs(self) -> dict[str, Any]:
        with self._client() as client:
            return client.get('/admin/api/logs').json()
