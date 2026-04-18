from __future__ import annotations

"""Authoritative execution facade for app runtime surfaces."""

import time
from decimal import Decimal

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.production import (
    ProductionReadRepository,
    SqlAlchemyAuditRepository,
    SqlAlchemyExecutionRepository,
)
from vinayak.domain.models import ExecutionRequest, ExecutionResult
from vinayak.execution.canonical_service import CanonicalExecutionService
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.guard import ExecutionGuard
from vinayak.execution.reviewed_trade_service import (
    ReviewedTradeCreateCommand,
    ReviewedTradeService,
    ReviewedTradeStatusUpdateCommand,
)
from vinayak.execution.reviewed_trade_execution_workflow import ReviewedTradeExecutionWorkflow
from vinayak.execution.service import ExecutionService
from vinayak.observability.observability_logger import log_event
from vinayak.observability.observability_metrics import increment_metric, set_metric


class ExecutionFacade:
    """Single execution boundary for routes and orchestration layers."""

    def __init__(
        self,
        session: Session,
        *,
        execution_guard: ExecutionGuard,
        execution_service: ExecutionService | None = None,
        reviewed_trade_service: ReviewedTradeService | None = None,
        canonical_service: CanonicalExecutionService | None = None,
        read_repository: ProductionReadRepository | None = None,
        reviewed_trade_execution_workflow: ReviewedTradeExecutionWorkflow | None = None,
    ) -> None:
        self.session = session
        self.execution_service = execution_service or ExecutionService(session)
        self.reviewed_trade_service = reviewed_trade_service or ReviewedTradeService(session)
        self.canonical_service = canonical_service or CanonicalExecutionService(
            execution_repository=SqlAlchemyExecutionRepository(session),
            audit_repository=SqlAlchemyAuditRepository(session),
            execution_guard=execution_guard,
        )
        self.read_repository = read_repository or ProductionReadRepository(session)
        self.reviewed_trade_execution_workflow = reviewed_trade_execution_workflow or ReviewedTradeExecutionWorkflow(
            session,
            execution_guard=execution_guard,
            execution_service=self.execution_service,
            canonical_service=self.canonical_service,
            read_repository=self.read_repository,
        )

    def list_executions(self) -> list[ExecutionRecord]:
        return self.execution_service.list_executions()

    def submit_reviewed_trade_execution(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        started = time.perf_counter()
        increment_metric('reviewed_trade_execution_submit_total', 1, labels={'mode': str(command.mode or '').upper()})
        record = self.reviewed_trade_execution_workflow.execute(command)
        duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
        set_metric('reviewed_trade_execution_latency_ms', duration_ms, labels={'mode': record.mode, 'status': record.status})
        increment_metric('reviewed_trade_execution_result_total', 1, labels={'mode': record.mode, 'status': record.status})
        log_event(
            component='execution_facade',
            event_name='reviewed_trade_execution_submitted',
            severity='INFO' if str(record.status or '').upper() in {'FILLED', 'EXECUTED', 'ACCEPTED', 'SENT'} else 'WARNING',
            message='Reviewed-trade execution completed via facade',
            context_json={
                'execution_id': record.id,
                'reviewed_trade_id': record.reviewed_trade_id,
                'signal_id': record.signal_id,
                'mode': record.mode,
                'status': record.status,
                'duration_ms': duration_ms,
            },
        )
        return record

    def list_reviewed_trades(self) -> list[ReviewedTradeRecord]:
        return self.reviewed_trade_service.list_reviewed_trades()

    def create_reviewed_trade(self, command: ReviewedTradeCreateCommand) -> ReviewedTradeRecord:
        return self.reviewed_trade_service.create_reviewed_trade(command)

    def update_reviewed_trade_status(self, command: ReviewedTradeStatusUpdateCommand) -> ReviewedTradeRecord:
        return self.reviewed_trade_service.update_reviewed_trade_status(command)

    def execute_request(
        self,
        request: ExecutionRequest,
        *,
        daily_realized_pnl: Decimal | None = None,
    ) -> ExecutionResult:
        started = time.perf_counter()
        increment_metric('execution_request_submit_total', 1, labels={'mode': request.mode.value, 'account_id': request.account_id})
        realized_pnl = daily_realized_pnl
        if realized_pnl is None:
            try:
                realized_pnl = self.read_repository.total_realized_pnl()
            except Exception:
                realized_pnl = Decimal('0')
        result = self.canonical_service.execute(request, daily_realized_pnl=realized_pnl)
        duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
        set_metric('execution_request_latency_ms', duration_ms, labels={'mode': request.mode.value, 'status': result.status.value})
        increment_metric('execution_request_result_total', 1, labels={'mode': request.mode.value, 'status': result.status.value, 'failure_reason': result.failure_reason.value})
        log_event(
            component='execution_facade',
            event_name='execution_request_completed',
            severity='INFO' if result.status.value == 'EXECUTED' else 'WARNING',
            message='Canonical execution request processed',
            context_json={
                'request_id': str(result.request_id),
                'execution_id': str(result.execution_id),
                'mode': request.mode.value,
                'account_id': request.account_id,
                'status': result.status.value,
                'failure_reason': result.failure_reason.value,
                'duration_ms': duration_ms,
            },
        )
        return result


__all__ = ['ExecutionFacade']
