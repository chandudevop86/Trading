from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session, require_user_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.production import AdminAuditResponse, ExecutionSubmitResponse, SignalRunRequest, SignalRunResponse
from vinayak.backtest.engine import BacktestEngine
from vinayak.cache.redis_client import RedisCache
from vinayak.db.repositories.production import SqlAlchemyAuditRepository, SqlAlchemyExecutionRepository
from vinayak.domain.models import (
    AuditEvent,
    Candle,
    CandleBatch,
    ExecutionMode,
    ExecutionRequest,
    RiskConfig,
    StrategyConfig,
    Timeframe,
)
from vinayak.execution.guard import ExecutionGuard, InMemoryGuardStateStore, RedisGuardStateStore
from vinayak.execution.service import ProductionExecutionService
from vinayak.market_data.providers.legacy_live_ohlcv import LegacyLiveOhlcvProvider
from vinayak.market_data.providers.base import MarketDataRequest
from vinayak.market_data.service import InMemoryCacheStore, MarketDataService
from vinayak.observability.prometheus import snapshot_metrics
from vinayak.services.admin_views import AdminViewService
from vinayak.services.strategy_runner import StrategyRunnerService


router = APIRouter(tags=['production'])


@dataclass
class _MemoryExecutionRepository:
    requests: dict[str, ExecutionRequest] = field(default_factory=dict)
    results: dict[str, object] = field(default_factory=dict)

    def get_by_idempotency_key(self, idempotency_key: str):
        return self.results.get(idempotency_key)

    def save_request(self, request: ExecutionRequest) -> None:
        self.requests[request.idempotency_key] = request

    def save_result(self, result):
        request = self.requests.get(next(key for key, value in self.requests.items() if value.request_id == result.request_id))
        self.results[request.idempotency_key] = result
        return result


@dataclass
class _MemoryAuditRepository:
    events: list[AuditEvent] = field(default_factory=list)

    def save_event(self, event: AuditEvent) -> None:
        self.events.append(event)

    def list_events(self) -> list[AuditEvent]:
        return list(self.events)


_AUDIT_REPOSITORY = _MemoryAuditRepository()
_MARKET_DATA_SERVICE = MarketDataService(
    provider=LegacyLiveOhlcvProvider(),
    cache_store=InMemoryCacheStore(),
)
_STRATEGY_RUNNER = StrategyRunnerService()
_BACKTEST_ENGINE = BacktestEngine()


@lru_cache(maxsize=1)
def _execution_guard() -> ExecutionGuard:
    redis_cache = RedisCache.from_env()
    if redis_cache.is_configured():
        return ExecutionGuard(RedisGuardStateStore(redis_cache))
    return ExecutionGuard(InMemoryGuardStateStore())


def _execution_service(db: Session) -> ProductionExecutionService:
    return ProductionExecutionService(
        execution_repository=SqlAlchemyExecutionRepository(db),
        audit_repository=SqlAlchemyAuditRepository(db),
        execution_guard=_execution_guard(),
    )


def _audit_reader(db: Session) -> SqlAlchemyAuditRepository:
    return SqlAlchemyAuditRepository(db)


def _timeframe(value: str) -> Timeframe:
    mapping = {
        '1m': Timeframe.M1,
        '5m': Timeframe.M5,
        '15m': Timeframe.M15,
        '30m': Timeframe.M30,
        '1h': Timeframe.H1,
        '1d': Timeframe.D1,
    }
    return mapping[value]


@router.post('/signals/run', response_model=SignalRunResponse, dependencies=[Depends(require_user_session)])
def run_signals(request: SignalRunRequest) -> SignalRunResponse:
    provider_result = _MARKET_DATA_SERVICE.fetch_candles(
        MarketDataRequest(symbol=request.symbol, timeframe=request.timeframe, lookback=request.lookback)
    )
    timeframe = _timeframe(request.timeframe)
    candles = tuple(
        Candle(
            symbol=request.symbol,
            timeframe=timeframe,
            timestamp=row['timestamp'].to_pydatetime(),
            open=Decimal(str(row['open'])),
            high=Decimal(str(row['high'])),
            low=Decimal(str(row['low'])),
            close=Decimal(str(row['close'])),
            volume=Decimal(str(row['volume'])),
        )
        for _, row in provider_result.frame.tail(request.lookback).iterrows()
    )
    candle_batch = CandleBatch(symbol=request.symbol, timeframe=timeframe, candles=candles)
    strategy_config = StrategyConfig(
        strategy_name=request.strategy,
        symbol=request.symbol,
        timeframe=timeframe,
        risk=RiskConfig(
            risk_per_trade_pct=request.risk_per_trade_pct,
            max_daily_loss_pct=request.max_daily_loss_pct,
            max_trades_per_day=request.max_trades_per_day,
            cooldown_minutes=request.cooldown_minutes,
        ),
    )
    signal_batch = _STRATEGY_RUNNER.run(candle_batch, strategy_config)
    return SignalRunResponse(candles=candle_batch, signals=signal_batch)


@router.post('/execution/request', response_model=ExecutionSubmitResponse, dependencies=[Depends(require_admin_session)])
def request_execution(request: ExecutionRequest, db: Session = Depends(get_db)) -> ExecutionSubmitResponse:
    result = _execution_service(db).execute(request)
    return ExecutionSubmitResponse(result=result)


@router.get('/admin/api/validation', response_model=AdminAuditResponse, dependencies=[Depends(require_admin_session)])
def admin_validation_view(db: Session = Depends(get_db)) -> AdminAuditResponse:
    service = AdminViewService(audit_reader=_audit_reader(db))
    return AdminAuditResponse(payload=service.validation_view())


@router.get('/admin/api/execution', response_model=AdminAuditResponse, dependencies=[Depends(require_admin_session)])
def admin_execution_view(db: Session = Depends(get_db)) -> AdminAuditResponse:
    service = AdminViewService(audit_reader=_audit_reader(db))
    return AdminAuditResponse(payload=service.execution_view())


@router.get('/admin/api/logs', response_model=AdminAuditResponse, dependencies=[Depends(require_admin_session)])
def admin_logs_view(db: Session = Depends(get_db)) -> AdminAuditResponse:
    service = AdminViewService(audit_reader=_audit_reader(db))
    payload = service.logs_view()
    payload['metrics'] = snapshot_metrics()
    return AdminAuditResponse(payload=payload)
