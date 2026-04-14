from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from vinayak.domain.models import (
    ExecutionMode,
    ExecutionRequest,
    ExecutionSide,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)
from vinayak.execution.guard import ExecutionGuard, InMemoryGuardStateStore


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
            max_trades_per_day=1,
            cooldown_minutes=15,
        ),
        account_id='primary',
    )


def test_execution_guard_blocks_duplicates_after_mark_executed() -> None:
    store = InMemoryGuardStateStore()
    guard = ExecutionGuard(store)
    request = _request()

    assert guard.evaluate(request).allowed is True
    guard.mark_executed(request)
    assert guard.evaluate(request).allowed is False


def test_execution_guard_blocks_when_trade_limit_reached() -> None:
    store = InMemoryGuardStateStore()
    guard = ExecutionGuard(store)
    request = _request()
    guard.mark_executed(request)

    second_request = request.model_copy(update={'idempotency_key': 'exec-NIFTY-20260414T093100Z'})
    decision = guard.evaluate(second_request)
    assert decision.allowed is False
