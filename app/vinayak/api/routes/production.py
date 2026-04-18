from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session, require_user_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.production import AdminAuditResponse, ExecutionSubmitResponse, SignalRunRequest, SignalRunResponse
from vinayak.backtest.engine import BacktestEngine
from vinayak.db.repositories.production import SqlAlchemyAuditRepository
from vinayak.domain.models import ExecutionRequest
from vinayak.execution.runtime import build_execution_facade
from vinayak.market_data.providers.runtime_live_ohlcv import RuntimeLiveOhlcvProvider
from vinayak.market_data.service import InMemoryCacheStore, MarketDataService
from vinayak.observability.prometheus import snapshot_metrics
from vinayak.services.admin_views import AdminViewService
from vinayak.services.production_runtime import ProductionSignalService
from vinayak.services.strategy_runner import StrategyRunnerService


router = APIRouter(tags=['production'])

_MARKET_DATA_SERVICE = MarketDataService(
    provider=RuntimeLiveOhlcvProvider(),
    cache_store=InMemoryCacheStore(),
)
_STRATEGY_RUNNER = StrategyRunnerService()
_SIGNAL_SERVICE = ProductionSignalService(
    market_data_service=_MARKET_DATA_SERVICE,
    strategy_runner=_STRATEGY_RUNNER,
)
_BACKTEST_ENGINE = BacktestEngine()
def _execution_facade(db: Session):
    return build_execution_facade(db)


def _audit_reader(db: Session) -> SqlAlchemyAuditRepository:
    return SqlAlchemyAuditRepository(db)

@router.post('/signals/run', response_model=SignalRunResponse, dependencies=[Depends(require_user_session)])
def run_signals(request: SignalRunRequest) -> SignalRunResponse:
    candle_batch, signal_batch = _SIGNAL_SERVICE.run_signals(
        symbol=request.symbol,
        timeframe=request.timeframe,
        lookback=request.lookback,
        strategy=request.strategy,
        risk_per_trade_pct=request.risk_per_trade_pct,
        max_daily_loss_pct=request.max_daily_loss_pct,
        max_trades_per_day=request.max_trades_per_day,
        cooldown_minutes=request.cooldown_minutes,
    )
    return SignalRunResponse(candles=candle_batch, signals=signal_batch)


@router.post('/execution/request', response_model=ExecutionSubmitResponse, dependencies=[Depends(require_admin_session)])
def request_execution(request: ExecutionRequest, db: Session = Depends(get_db)) -> ExecutionSubmitResponse:
    result = _execution_facade(db).execute_request(request)
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
