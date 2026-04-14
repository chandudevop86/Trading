from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from vinayak.domain.models import (
    Candle,
    ExecutionMode,
    ExecutionRequest,
    ExecutionSide,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)


def test_candle_requires_timezone_aware_timestamp() -> None:
    with pytest.raises(ValidationError):
        Candle(
            symbol='nifty',
            timeframe=Timeframe.M5,
            timestamp=datetime(2026, 1, 1, 9, 15),
            open=Decimal('100'),
            high=Decimal('101'),
            low=Decimal('99'),
            close=Decimal('100.5'),
            volume=Decimal('1000'),
        )


def test_no_trade_signal_rejects_execution_fields() -> None:
    with pytest.raises(ValidationError):
        TradeSignal(
            idempotency_key='x' * 20,
            strategy_name='breakout',
            symbol='nifty',
            timeframe=Timeframe.M5,
            signal_type=TradeSignalType.NO_TRADE,
            generated_at=datetime.now(UTC),
            candle_timestamp=datetime.now(UTC),
            side=ExecutionSide.BUY,
            entry_price=Decimal('100'),
            stop_loss=Decimal('99'),
            target_price=Decimal('102'),
            quantity=Decimal('1'),
            rationale='invalid',
        )


def test_live_execution_requires_live_enabled_risk() -> None:
    signal = TradeSignal(
        idempotency_key='s' * 20,
        strategy_name='breakout',
        symbol='nifty',
        timeframe=Timeframe.M5,
        signal_type=TradeSignalType.ENTRY,
        generated_at=datetime.now(UTC),
        candle_timestamp=datetime.now(UTC),
        side=ExecutionSide.BUY,
        entry_price=Decimal('100'),
        stop_loss=Decimal('99'),
        target_price=Decimal('102'),
        quantity=Decimal('1'),
        rationale='valid',
    )
    risk = RiskConfig(
        risk_per_trade_pct=Decimal('1'),
        max_daily_loss_pct=Decimal('3'),
        max_trades_per_day=5,
        cooldown_minutes=10,
        allow_live_trading=False,
    )

    with pytest.raises(ValidationError):
        ExecutionRequest(
            idempotency_key='e' * 20,
            requested_at=datetime.now(UTC),
            mode=ExecutionMode.LIVE,
            signal=signal,
            risk=risk,
            account_id='live-1',
        )
