from __future__ import annotations

"""Authoritative execution facade for app runtime surfaces."""

from decimal import Decimal

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.repositories.production import (
    ProductionReadRepository,
    SqlAlchemyAuditRepository,
    SqlAlchemyExecutionRepository,
)
from vinayak.domain.models import ExecutionRequest, ExecutionResult
from vinayak.execution.canonical_service import CanonicalExecutionService
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.guard import ExecutionGuard
from vinayak.execution.service import ExecutionService


class ExecutionFacade:
    """Single execution boundary for routes and orchestration layers."""

    def __init__(
        self,
        session: Session,
        *,
        execution_guard: ExecutionGuard,
        reviewed_trade_service: ExecutionService | None = None,
        canonical_service: CanonicalExecutionService | None = None,
        read_repository: ProductionReadRepository | None = None,
    ) -> None:
        self.session = session
        self.reviewed_trade_service = reviewed_trade_service or ExecutionService(session)
        self.canonical_service = canonical_service or CanonicalExecutionService(
            execution_repository=SqlAlchemyExecutionRepository(session),
            audit_repository=SqlAlchemyAuditRepository(session),
            execution_guard=execution_guard,
        )
        self.read_repository = read_repository or ProductionReadRepository(session)

    def list_executions(self) -> list[ExecutionRecord]:
        return self.reviewed_trade_service.list_executions()

    def create_execution(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        return self.reviewed_trade_service.create_execution(command)

    def execute_request(
        self,
        request: ExecutionRequest,
        *,
        daily_realized_pnl: Decimal | None = None,
    ) -> ExecutionResult:
        realized_pnl = daily_realized_pnl
        if realized_pnl is None:
            try:
                realized_pnl = self.read_repository.total_realized_pnl()
            except Exception:
                realized_pnl = Decimal("0")
        return self.canonical_service.execute(request, daily_realized_pnl=realized_pnl)


__all__ = ["ExecutionFacade"]
