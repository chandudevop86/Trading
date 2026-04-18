from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from vinayak.domain.models import (
    AuditEvent,
    ExecutionMode,
    ExecutionRequest,
    ExecutionSide,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)
from vinayak.execution.guard import ExecutionGuard, InMemoryGuardStateStore
from vinayak.execution.canonical_service import ProductionExecutionService


@dataclass
class _MemoryExecutionRepository:
    request: ExecutionRequest | None = None
    result: object | None = None

    def get_by_idempotency_key(self, idempotency_key: str):
        if self.result is not None and self.request is not None and self.request.idempotency_key == idempotency_key:
            return self.result
        return None

    def save_request(self, request: ExecutionRequest) -> None:
        self.request = request

    def save_result(self, result):
        self.result = result
        return result


@dataclass
class _MemoryAuditRepository:
    events: list[AuditEvent]

    def save_event(self, event: AuditEvent) -> None:
        self.events.append(event)


def _request() -> ExecutionRequest:
    signal = TradeSignal(
        idempotency_key='NIFTY-BREAKOUT-20260414T092000Z-BUY',
        strategy_name='BREAKOUT',
        symbol='NIFTY',
        timeframe=Timeframe.M5,
        signal_type=TradeSignalType.ENTRY,
        generated_at=datetime(2026, 4, 14, 9, 21, tzinfo=UTC),
        candle_timestamp=datetime(2026, 4, 14, 9, 20, tzinfo=UTC),
        side=ExecutionSide.BUY,
        entry_price=Decimal('22466.40'),
        stop_loss=Decimal('22439.20'),
        target_price=Decimal('22520.80'),
        quantity=Decimal('50'),
        confidence=Decimal('0.84'),
        rationale='valid setup',
    )
    return ExecutionRequest(
        idempotency_key='exec-NIFTY-20260414T092100Z',
        requested_at=datetime(2026, 4, 14, 10, 0, tzinfo=UTC),
        mode=ExecutionMode.PAPER,
        signal=signal,
        risk=RiskConfig(
            risk_per_trade_pct=Decimal('1'),
            max_daily_loss_pct=Decimal('3'),
            max_trades_per_day=2,
            cooldown_minutes=15,
        ),
        account_id='primary',
    )


def test_production_execution_service_is_idempotent() -> None:
    execution_repository = _MemoryExecutionRepository()
    audit_repository = _MemoryAuditRepository(events=[])
    service = ProductionExecutionService(
        execution_repository=execution_repository,
        audit_repository=audit_repository,
        execution_guard=ExecutionGuard(InMemoryGuardStateStore()),
    )
    request = _request()

    first = service.execute(request)
    second = service.execute(request)

    assert first.execution_id == second.execution_id
    assert len(audit_repository.events) >= 2
