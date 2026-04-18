from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from vinayak.domain.models import (
    AuditEvent,
    AuditEventType,
    ExecutionFailureReason as DomainExecutionFailureReason,
    ExecutionRequest as DomainExecutionRequest,
    ExecutionResult as DomainExecutionResult,
    ExecutionStatus as DomainExecutionStatus,
)
from vinayak.execution.events import (
    TRADE_EXECUTED,
    TRADE_EXECUTION_REJECTED,
    TRADE_EXECUTE_REQUESTED,
)
from vinayak.execution.guard import ExecutionGuard
from vinayak.execution.repositories import AuditRepositoryPort, ExecutionRepositoryPort


class BrokerAdapter(Protocol):
    def place_order(self, request: DomainExecutionRequest) -> DomainExecutionResult:
        ...


class PaperBrokerAdapter:
    def place_order(self, request: DomainExecutionRequest) -> DomainExecutionResult:
        return DomainExecutionResult(
            request_id=request.request_id,
            status=DomainExecutionStatus.EXECUTED,
            processed_at=datetime.now(UTC),
            order_reference=f'paper-{request.request_id.hex[:12]}',
            message='Paper execution completed.',
        )


class LiveBrokerAdapter:
    def place_order(self, request: DomainExecutionRequest) -> DomainExecutionResult:
        return DomainExecutionResult(
            request_id=request.request_id,
            status=DomainExecutionStatus.REJECTED,
            failure_reason=DomainExecutionFailureReason.LIVE_MODE_LOCKED,
            processed_at=datetime.now(UTC),
            message='Live adapter is a placeholder until broker integration is approved.',
        )


class CanonicalExecutionService:
    """Canonical execution orchestration for domain-level execution requests."""

    def __init__(
        self,
        *,
        execution_repository: ExecutionRepositoryPort,
        audit_repository: AuditRepositoryPort,
        execution_guard: ExecutionGuard,
        paper_adapter: BrokerAdapter | None = None,
        live_adapter: BrokerAdapter | None = None,
    ) -> None:
        self.execution_repository = execution_repository
        self.audit_repository = audit_repository
        self.execution_guard = execution_guard
        self.paper_adapter = paper_adapter or PaperBrokerAdapter()
        self.live_adapter = live_adapter or LiveBrokerAdapter()

    def execute(
        self,
        request: DomainExecutionRequest,
        *,
        daily_realized_pnl: Decimal = Decimal('0'),
    ) -> DomainExecutionResult:
        existing = self.execution_repository.get_by_idempotency_key(request.idempotency_key)
        if existing is not None:
            return existing

        session = self._session()
        try:
            self.execution_repository.save_request(request)
            self._emit_event(TRADE_EXECUTE_REQUESTED, request.request_id, {'idempotency_key': request.idempotency_key})

            decision = self.execution_guard.evaluate(request, daily_realized_pnl=daily_realized_pnl)
            if not decision.allowed:
                rejection = self.execution_guard.build_rejection_result(request, decision)
                stored = self.execution_repository.save_result(rejection)
                self._emit_event(
                    TRADE_EXECUTION_REJECTED,
                    request.request_id,
                    {
                        'failure_reason': rejection.failure_reason.value,
                        'message': rejection.message,
                        'signal_id': str(request.signal.signal_id),
                    },
                )
                if session is not None:
                    session.commit()
                return stored

            adapter = self.live_adapter if request.mode.value == 'LIVE' else self.paper_adapter
            result = adapter.place_order(request)
            stored = self.execution_repository.save_result(result)
            if stored.status == DomainExecutionStatus.EXECUTED:
                self.execution_guard.mark_executed(request)
            self._emit_event(
                TRADE_EXECUTED,
                request.request_id,
                {
                    'execution_id': str(stored.execution_id),
                    'status': stored.status.value,
                    'failure_reason': stored.failure_reason.value,
                    'order_reference': stored.order_reference or '',
                },
            )
            if session is not None:
                session.commit()
            return stored
        except Exception:
            if session is not None:
                session.rollback()
            raise

    def _emit_event(self, event_name: str, correlation_id: UUID, payload: dict[str, Any]) -> None:
        self.audit_repository.save_event(
            AuditEvent(
                event_type=self._audit_event_type(event_name),
                correlation_id=correlation_id,
                occurred_at=datetime.now(UTC),
                payload={'event_name': event_name, **payload},
            )
        )

    def _audit_event_type(self, event_name: str) -> AuditEventType:
        if event_name == TRADE_EXECUTE_REQUESTED:
            return AuditEventType.EXECUTION_REQUESTED
        if event_name == TRADE_EXECUTION_REJECTED:
            return AuditEventType.EXECUTION_REJECTED
        return AuditEventType.EXECUTION_COMPLETED

    def _session(self):
        return getattr(self.execution_repository, 'session', None) or getattr(self.audit_repository, 'session', None)


ProductionExecutionService = CanonicalExecutionService


__all__ = [
    'BrokerAdapter',
    'CanonicalExecutionService',
    'LiveBrokerAdapter',
    'PaperBrokerAdapter',
    'ProductionExecutionService',
]
