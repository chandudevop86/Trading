from __future__ import annotations

from typing import Protocol

from vinayak.domain.models import AuditEvent, ExecutionRequest, ExecutionResult


class ExecutionRepositoryPort(Protocol):
    def get_by_idempotency_key(self, idempotency_key: str) -> ExecutionResult | None: ...
    def save_request(self, request: ExecutionRequest) -> None: ...
    def save_result(self, result: ExecutionResult) -> ExecutionResult: ...


class AuditRepositoryPort(Protocol):
    def save_event(self, event: AuditEvent) -> None: ...
