from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import (
    DashboardSummaryResponse,
    LiveAnalysisExecutionRow,
    LiveAnalysisExecutionSummary,
    LiveAnalysisReportArtifacts,
    LiveAnalysisResponse,
    LiveAnalysisSignalRow,
    LiveOhlcvResponse,
    LiveOhlcvRowResponse,
    ReportArtifactLocation,
)
from vinayak.api.schemas.strategy import LiveAnalysisRequest
from vinayak.api.services.dashboard_summary import DashboardSummaryService
from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.api.services.trading_workspace import run_live_trading_analysis
from vinayak.observability.observability_health import build_observability_dashboard_payload


router = APIRouter(prefix='/dashboard', tags=['dashboard'], dependencies=[Depends(require_admin_session)])


@router.get('/summary', response_model=DashboardSummaryResponse)
def get_dashboard_summary(db: Session = Depends(get_db)) -> DashboardSummaryResponse:
    service = DashboardSummaryService(db)
    return DashboardSummaryResponse(**service.build_summary())


@router.get('/observability')
def get_observability_dashboard() -> dict:
    return build_observability_dashboard_payload()


@router.get('/candles', response_model=LiveOhlcvResponse)
def get_dashboard_candles(
    symbol: str = Query(default='^NSEI', min_length=1),
    interval: str = Query(default='1m', min_length=1),
    period: str = Query(default='1d', min_length=1),
) -> LiveOhlcvResponse:
    rows = fetch_live_ohlcv(symbol=symbol, interval=interval, period=period)
    return LiveOhlcvResponse(
        symbol=symbol,
        interval=interval,
        period=period,
        total=len(rows),
        candles=[LiveOhlcvRowResponse(**row) for row in rows],
    )


@router.post('/live-analysis', response_model=LiveAnalysisResponse)
def post_live_analysis(request: LiveAnalysisRequest, db: Session = Depends(get_db)) -> LiveAnalysisResponse:
    result = run_live_trading_analysis(
        symbol=request.symbol,
        interval=request.interval,
        period=request.period,
        strategy=request.strategy,
        capital=request.capital,
        risk_pct=request.risk_pct,
        rr_ratio=request.rr_ratio,
        trailing_sl_pct=request.trailing_sl_pct,
        strike_step=request.strike_step,
        moneyness=request.moneyness,
        strike_steps=request.strike_steps,
        fetch_option_metrics=request.fetch_option_metrics,
        send_telegram=request.send_telegram,
        telegram_token=request.telegram_token,
        telegram_chat_id=request.telegram_chat_id,
        auto_execute=request.auto_execute,
        execution_type=request.execution_type,
        lot_size=request.lot_size,
        lots=request.lots,
        security_map_path=request.security_map_path,
        paper_log_path=request.paper_log_path,
        live_log_path=request.live_log_path,
        mtf_ema_period=request.mtf_ema_period,
        mtf_setup_mode=request.mtf_setup_mode,
        mtf_retest_strength=request.mtf_retest_strength,
        mtf_max_trades_per_day=request.mtf_max_trades_per_day,
        entry_cutoff_hhmm=request.entry_cutoff_hhmm,
        cost_bps=request.cost_bps,
        fixed_cost_per_trade=request.fixed_cost_per_trade,
        max_daily_loss=request.max_daily_loss,
        max_trades_per_day=request.max_trades_per_day,
        db_session=db,
    )
    return LiveAnalysisResponse(
        symbol=result['symbol'],
        interval=result['interval'],
        period=result['period'],
        strategy=result['strategy'],
        generated_at=result['generated_at'],
        candle_count=result['candle_count'],
        signal_count=result['signal_count'],
        side_counts=result['side_counts'],
        candles=[LiveOhlcvRowResponse(**row) for row in result['candles']],
        signals=[LiveAnalysisSignalRow(**row) for row in result['signals']],
        telegram_sent=result['telegram_sent'],
        telegram_error=result['telegram_error'],
        telegram_payload=result['telegram_payload'],
        execution_summary=LiveAnalysisExecutionSummary(**result['execution_summary']),
        execution_rows=[LiveAnalysisExecutionRow(**row) for row in result['execution_rows']],
        validation_summary=result.get('validation_summary', {}),
        data_status=result.get('data_status', {}),
        system_status=result.get('system_status', 'NOT_READY'),
        report_artifacts=LiveAnalysisReportArtifacts(
            json_report=ReportArtifactLocation(**result['report_artifacts']['json_report']),
            summary_report=ReportArtifactLocation(**result['report_artifacts']['summary_report']),
        ),
    )





