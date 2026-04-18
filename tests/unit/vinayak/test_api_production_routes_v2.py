from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from fastapi.testclient import TestClient

from vinayak.api.dependencies.admin_auth import require_admin_session, require_user_session
from vinayak.api.main import app
from vinayak.api.routes import production
from vinayak.domain.models import (
    Candle,
    CandleBatch,
    ExecutionFailureReason,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSide,
    ExecutionStatus,
    RiskConfig,
    StrategySignalBatch,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)


def _build_signal() -> TradeSignal:
    return TradeSignal(
        idempotency_key='signal-key-1234567890',
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
        rationale='test signal',
    )


def test_signals_run_endpoint_returns_batch(monkeypatch) -> None:
    app.dependency_overrides[require_user_session] = lambda: {'username': 'user'}
    monkeypatch.setattr(
        production._SIGNAL_SERVICE,
        'run_signals',
        lambda **kwargs: (
            CandleBatch(
                symbol='NIFTY',
                timeframe=Timeframe.M5,
                candles=(
                    Candle(
                        symbol='NIFTY',
                        timeframe=Timeframe.M5,
                        timestamp=datetime.now(UTC),
                        open=Decimal('100'),
                        high=Decimal('101'),
                        low=Decimal('99'),
                        close=Decimal('100.5'),
                        volume=Decimal('10'),
                    ),
                ),
            ),
            StrategySignalBatch(generated_at=datetime.now(UTC), signals=(_build_signal(),)),
        ),
    )

    with TestClient(app) as client:
        response = client.post(
            '/signals/run',
            json={
                'symbol': 'NIFTY',
                'timeframe': '5m',
                'lookback': 2,
                'strategy': 'BREAKOUT',
                'risk_per_trade_pct': '1',
                'max_daily_loss_pct': '3',
                'max_trades_per_day': 5,
                'cooldown_minutes': 15,
            },
        )

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()['signals']['signals'][0]['strategy_name'] == 'BREAKOUT'


def test_execution_request_endpoint_uses_execution_service(monkeypatch) -> None:
    app.dependency_overrides[require_admin_session] = lambda: {'username': 'admin', 'role': 'ADMIN'}
    signal = _build_signal()
    result = ExecutionResult(
        request_id='00000000-0000-0000-0000-000000000001',
        status=ExecutionStatus.EXECUTED,
        failure_reason=ExecutionFailureReason.NONE,
        processed_at=datetime.now(UTC),
        order_reference='paper-123',
        message='ok',
    )
    monkeypatch.setattr(
        production,
        '_execution_facade',
        lambda db: SimpleNamespace(execute_request=lambda request: result),
    )

    payload = ExecutionRequest(
        idempotency_key='exec-key-1234567890',
        requested_at=datetime.now(UTC),
        mode=ExecutionMode.PAPER,
        signal=signal,
        risk=RiskConfig(
            risk_per_trade_pct=Decimal('1'),
            max_daily_loss_pct=Decimal('3'),
            max_trades_per_day=5,
            cooldown_minutes=15,
        ),
        account_id='paper-account',
    ).model_dump(mode='json')

    with TestClient(app) as client:
        response = client.post('/execution/request', json=payload)

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()['result']['status'] == 'EXECUTED'
