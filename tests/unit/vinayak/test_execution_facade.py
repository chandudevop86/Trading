from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from vinayak.domain.models import (
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
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.facade import ExecutionFacade
from vinayak.observability.observability_metrics import get_metric, reset_observability_state


@dataclass
class _StubReadRepository:
    realized_pnl: Decimal = Decimal('0')

    def total_realized_pnl(self) -> Decimal:
        return self.realized_pnl


@dataclass
class _StubCanonicalService:
    response: ExecutionResult
    captured_pnl: Decimal | None = None

    def execute(self, request: ExecutionRequest, *, daily_realized_pnl: Decimal = Decimal('0')) -> ExecutionResult:
        self.captured_pnl = daily_realized_pnl
        return self.response


@dataclass
class _StubExecutionRecord:
    id: int = 1
    reviewed_trade_id: int | None = 1
    signal_id: int | None = 2
    mode: str = 'PAPER'
    status: str = 'FILLED'


@dataclass
class _StubExecutionService:
    records: list[object]

    def list_executions(self):
        return list(self.records)


@dataclass
class _StubReviewedTradeExecutionWorkflow:
    record: _StubExecutionRecord
    created_command: ExecutionCreateCommand | None = None

    def execute(self, command):
        self.created_command = command
        return self.record


@dataclass
class _StubReviewedTradeService:
    records: list[object]

    def list_reviewed_trades(self):
        return list(self.records)


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        idempotency_key='exec-facade-test-123456',
        requested_at=datetime.now(UTC),
        mode=ExecutionMode.PAPER,
        signal=TradeSignal(
            idempotency_key='signal-facade-test-123456',
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
            confidence=Decimal('0.8'),
            rationale='test',
        ),
        risk=RiskConfig(
            risk_per_trade_pct=Decimal('1'),
            max_daily_loss_pct=Decimal('3'),
            max_trades_per_day=5,
            cooldown_minutes=15,
        ),
        account_id='paper',
    )


def test_execution_facade_routes_domain_execution_through_canonical_service(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv('VINAYAK_OBSERVABILITY_DIR', str(tmp_path / 'observability'))
    reset_observability_state()
    result = ExecutionResult(
        request_id=_request().request_id,
        status=ExecutionStatus.EXECUTED,
        failure_reason=ExecutionFailureReason.NONE,
        processed_at=datetime.now(UTC),
        order_reference='paper-123',
        message='ok',
    )
    canonical = _StubCanonicalService(response=result)
    facade = ExecutionFacade(
        session=None,  # type: ignore[arg-type]
        execution_guard=None,  # type: ignore[arg-type]
        execution_service=_StubExecutionService(records=[]),  # type: ignore[arg-type]
        reviewed_trade_service=_StubReviewedTradeService(records=[]),  # type: ignore[arg-type]
        canonical_service=canonical,  # type: ignore[arg-type]
        read_repository=_StubReadRepository(realized_pnl=Decimal('1.25')),
    )

    returned = facade.execute_request(_request())

    assert returned == result
    assert canonical.captured_pnl == Decimal('1.25')
    assert float(get_metric('execution_request_submit_total', 0)) >= 1.0
    assert float(get_metric('execution_request_latency_ms', 0)) >= 0.0
    assert float(get_metric('execution_request_result_total', 0)) >= 1.0


def test_execution_facade_exposes_explicit_reviewed_trade_execution_method(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv('VINAYAK_OBSERVABILITY_DIR', str(tmp_path / 'observability'))
    reset_observability_state()
    workflow = _StubReviewedTradeExecutionWorkflow(record=_StubExecutionRecord())
    facade = ExecutionFacade(
        session=None,  # type: ignore[arg-type]
        execution_guard=None,  # type: ignore[arg-type]
        execution_service=_StubExecutionService(records=[]),  # type: ignore[arg-type]
        reviewed_trade_service=_StubReviewedTradeService(records=[]),  # type: ignore[arg-type]
        canonical_service=_StubCanonicalService(
            response=ExecutionResult(
                request_id=_request().request_id,
                status=ExecutionStatus.EXECUTED,
                failure_reason=ExecutionFailureReason.NONE,
                processed_at=datetime.now(UTC),
                order_reference='paper-123',
                message='ok',
            )
        ),  # type: ignore[arg-type]
        read_repository=_StubReadRepository(),
        reviewed_trade_execution_workflow=workflow,  # type: ignore[arg-type]
    )
    command = ExecutionCreateCommand(mode='PAPER', broker='SIM', reviewed_trade_id=1)

    returned = facade.submit_reviewed_trade_execution(command)

    assert workflow.created_command == command
    assert returned.id == 1
    assert float(get_metric('reviewed_trade_execution_submit_total', 0)) >= 1.0
    assert float(get_metric('reviewed_trade_execution_latency_ms', 0)) >= 0.0
    assert float(get_metric('reviewed_trade_execution_result_total', 0)) >= 1.0
