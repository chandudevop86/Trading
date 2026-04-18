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
from vinayak.execution.facade import ExecutionFacade


@dataclass
class _StubReadRepository:
    realized_pnl: Decimal = Decimal("0")

    def total_realized_pnl(self) -> Decimal:
        return self.realized_pnl


@dataclass
class _StubCanonicalService:
    response: ExecutionResult
    captured_pnl: Decimal | None = None

    def execute(self, request: ExecutionRequest, *, daily_realized_pnl: Decimal = Decimal("0")) -> ExecutionResult:
        self.captured_pnl = daily_realized_pnl
        return self.response


@dataclass
class _StubReviewedTradeService:
    records: list[object]

    def list_executions(self):
        return list(self.records)

    def create_execution(self, command):
        return {"command": command}


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        idempotency_key="exec-facade-test-123456",
        requested_at=datetime.now(UTC),
        mode=ExecutionMode.PAPER,
        signal=TradeSignal(
            idempotency_key="signal-facade-test-123456",
            strategy_name="BREAKOUT",
            symbol="NIFTY",
            timeframe=Timeframe.M5,
            signal_type=TradeSignalType.ENTRY,
            generated_at=datetime.now(UTC),
            candle_timestamp=datetime.now(UTC),
            side=ExecutionSide.BUY,
            entry_price=Decimal("100"),
            stop_loss=Decimal("99"),
            target_price=Decimal("102"),
            quantity=Decimal("1"),
            confidence=Decimal("0.8"),
            rationale="test",
        ),
        risk=RiskConfig(
            risk_per_trade_pct=Decimal("1"),
            max_daily_loss_pct=Decimal("3"),
            max_trades_per_day=5,
            cooldown_minutes=15,
        ),
        account_id="paper",
    )


def test_execution_facade_routes_domain_execution_through_canonical_service() -> None:
    result = ExecutionResult(
        request_id=_request().request_id,
        status=ExecutionStatus.EXECUTED,
        failure_reason=ExecutionFailureReason.NONE,
        processed_at=datetime.now(UTC),
        order_reference="paper-123",
        message="ok",
    )
    canonical = _StubCanonicalService(response=result)
    facade = ExecutionFacade(
        session=None,  # type: ignore[arg-type]
        execution_guard=None,  # type: ignore[arg-type]
        reviewed_trade_service=_StubReviewedTradeService(records=[]),  # type: ignore[arg-type]
        canonical_service=canonical,  # type: ignore[arg-type]
        read_repository=_StubReadRepository(realized_pnl=Decimal("1.25")),
    )

    returned = facade.execute_request(_request())

    assert returned == result
    assert canonical.captured_pnl == Decimal("1.25")
