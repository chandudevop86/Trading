from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from vinayak.domain.models import (
    AuditEvent,
    ExecutionFailureReason,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSide,
    ExecutionStatus,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)
from vinayak.execution.canonical_service import CanonicalExecutionService


@dataclass
class _StubSession:
    commits: int = 0
    rollbacks: int = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _StubExecutionRepository:
    def __init__(self, session: _StubSession) -> None:
        self.session = session
        self.saved_requests = 0
        self.saved_results = 0

    def get_by_idempotency_key(self, idempotency_key: str):
        return None

    def save_request(self, request: ExecutionRequest) -> None:
        self.saved_requests += 1

    def save_result(self, result: ExecutionResult) -> ExecutionResult:
        self.saved_results += 1
        return result


class _StubAuditRepository:
    def __init__(self, session: _StubSession) -> None:
        self.session = session
        self.saved_events: list[AuditEvent] = []

    def save_event(self, event: AuditEvent) -> None:
        self.saved_events.append(event)


class _StubGuard:
    def evaluate(self, request: ExecutionRequest, *, daily_realized_pnl: Decimal = Decimal('0')):
        return type('Decision', (), {'allowed': True})()

    def build_rejection_result(self, request: ExecutionRequest, decision):
        raise AssertionError('unexpected rejection path')

    def mark_executed(self, request: ExecutionRequest) -> None:
        return None


class _StubPaperAdapter:
    def place_order(self, request: ExecutionRequest) -> ExecutionResult:
        return ExecutionResult(
            request_id=request.request_id,
            status=ExecutionStatus.EXECUTED,
            failure_reason=ExecutionFailureReason.NONE,
            processed_at=datetime.now(UTC),
            order_reference='paper-123',
            message='ok',
        )


class _FailingAuditRepository(_StubAuditRepository):
    def save_event(self, event: AuditEvent) -> None:
        raise RuntimeError('audit boom')


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        idempotency_key='txn-test-123',
        requested_at=datetime.now(UTC),
        mode=ExecutionMode.PAPER,
        signal=TradeSignal(
            idempotency_key='txn-signal-123',
            strategy_name='BREAKOUT',
            symbol='NIFTY',
            timeframe=Timeframe.M5,
            signal_type=TradeSignalType.ENTRY,
            generated_at=datetime.now(UTC),
            candle_timestamp=datetime.now(UTC),
            side=ExecutionSide.BUY,
            entry_price=Decimal('100'),
            stop_loss=Decimal('99'),
            target_price=Decimal('102'),
            quantity=Decimal('1'),
            confidence=Decimal('0.9'),
            rationale='txn test',
        ),
        risk=RiskConfig(
            risk_per_trade_pct=Decimal('1'),
            max_daily_loss_pct=Decimal('3'),
            max_trades_per_day=5,
            cooldown_minutes=15,
        ),
        account_id='paper',
    )


def test_canonical_execution_service_commits_once_after_success() -> None:
    session = _StubSession()
    service = CanonicalExecutionService(
        execution_repository=_StubExecutionRepository(session),
        audit_repository=_StubAuditRepository(session),
        execution_guard=_StubGuard(),
        paper_adapter=_StubPaperAdapter(),
    )

    result = service.execute(_request())

    assert result.status == ExecutionStatus.EXECUTED
    assert session.commits == 1
    assert session.rollbacks == 0


def test_canonical_execution_service_rolls_back_on_audit_failure() -> None:
    session = _StubSession()
    service = CanonicalExecutionService(
        execution_repository=_StubExecutionRepository(session),
        audit_repository=_FailingAuditRepository(session),
        execution_guard=_StubGuard(),
        paper_adapter=_StubPaperAdapter(),
    )

    try:
        service.execute(_request())
        raise AssertionError('expected failure')
    except RuntimeError as exc:
        assert 'audit boom' in str(exc)

    assert session.commits == 0
    assert session.rollbacks == 1
