from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import (
    DashboardSummaryResponse,
    LiveAnalysisJobAcceptedResponse,
    LiveAnalysisJobActionResponse,
    LiveAnalysisJobListResponse,
    LiveAnalysisJobResponse,
    LiveAnalysisJobStatusResponse,
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
from vinayak.api.services.live_analysis_jobs import get_live_analysis_job_service
from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.api.services.trading_workspace import refresh_market_data_snapshot, run_live_trading_analysis
from vinayak.core.config import get_settings
from vinayak.observability.observability_health import build_observability_dashboard_payload
from vinayak.web.services.role_view_service import RoleViewService


router = APIRouter(prefix='/dashboard', tags=['dashboard'], dependencies=[Depends(require_admin_session)])


def _to_live_analysis_response(result: dict) -> LiveAnalysisResponse:
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
        execution_note=result.get('execution_note', ''),
        execution_summary=LiveAnalysisExecutionSummary(**result['execution_summary']),
        execution_rows=[LiveAnalysisExecutionRow(**row) for row in result['execution_rows']],
        validation_summary=result.get('validation_summary', {}),
        data_status=result.get('data_status', {}),
        system_status=result.get('system_status', 'NOT_READY'),
        report_artifacts=LiveAnalysisReportArtifacts(
            json_report=ReportArtifactLocation(**result['report_artifacts']['json_report']),
            summary_report=ReportArtifactLocation(**result['report_artifacts']['summary_report']),
        ),
        alert_notifications_sent=result.get('alert_notifications_sent', 0),
    )


@router.get('/summary', response_model=DashboardSummaryResponse)
def get_dashboard_summary(db: Session = Depends(get_db)) -> DashboardSummaryResponse:
    service = DashboardSummaryService(db)
    return DashboardSummaryResponse(**service.build_summary())


@router.get('/observability')
def get_observability_dashboard() -> dict:
    return build_observability_dashboard_payload()


@router.get('/market-heartbeat')
def get_market_heartbeat(
    symbol: str = Query(default='^NSEI', min_length=1),
    interval: str = Query(default='5m', min_length=1),
    period: str = Query(default='1d', min_length=1),
) -> dict:
    return refresh_market_data_snapshot(symbol=symbol, interval=interval, period=period)


@router.get('/candles', response_model=LiveOhlcvResponse)
def get_dashboard_candles(
    symbol: str = Query(default='^NSEI', min_length=1),
    interval: str = Query(default='1m', min_length=1),
    period: str = Query(default='1d', min_length=1),
    refresh: bool = Query(default=False),
) -> LiveOhlcvResponse:
    rows = fetch_live_ohlcv(
        symbol=symbol,
        interval=interval,
        period=period,
        provider='DHAN',
        force_refresh=refresh,
    )
    return LiveOhlcvResponse(
        symbol=symbol,
        interval=interval,
        period=period,
        total=len(rows),
        candles=[LiveOhlcvRowResponse(**row) for row in rows],
    )


def _to_live_analysis_job_response(job: dict) -> LiveAnalysisJobResponse:
    return LiveAnalysisJobResponse(
        job_id=job['job_id'],
        status=job['status'],
        symbol=job['symbol'],
        interval=job['interval'],
        period=job['period'],
        strategy=job['strategy'],
        requested_at=job['requested_at'],
        started_at=job.get('started_at'),
        finished_at=job.get('finished_at'),
        error=job.get('error'),
        deduplicated=bool(job.get('deduplicated', False)),
        signal_count=int(job.get('signal_count', 0) or 0),
        candle_count=int(job.get('candle_count', 0) or 0),
    )


def _legacy_sync_live_analysis_enabled() -> bool:
    return bool(get_settings().legacy_sync_live_analysis_enabled)


@router.get('/live-analysis/latest', response_model=LiveAnalysisResponse | None)
def get_latest_live_analysis(db: Session = Depends(get_db)) -> LiveAnalysisResponse | None:
    result = RoleViewService(db).load_latest_analysis()
    if not result:
        return None
    return _to_live_analysis_response(result)


@router.post('/live-analysis/jobs', response_model=LiveAnalysisJobAcceptedResponse)
def create_live_analysis_job(request: LiveAnalysisRequest) -> LiveAnalysisJobAcceptedResponse:
    job = get_live_analysis_job_service().submit(request)
    poll_url = f"/dashboard/live-analysis/jobs/{job['job_id']}"
    return LiveAnalysisJobAcceptedResponse(
        job=_to_live_analysis_job_response(job),
        poll_url=poll_url,
        latest_result_url='/dashboard/live-analysis/latest',
    )


@router.get('/live-analysis/jobs', response_model=LiveAnalysisJobListResponse)
def list_live_analysis_jobs(
    limit: int = Query(default=25, ge=1, le=200),
    status: str | None = Query(default=None),
) -> LiveAnalysisJobListResponse:
    jobs = get_live_analysis_job_service().list_jobs(limit=limit, status=status)
    return LiveAnalysisJobListResponse(
        total=len(jobs),
        jobs=[_to_live_analysis_job_response(job) for job in jobs],
    )


@router.get('/live-analysis/jobs/{job_id}', response_model=LiveAnalysisJobStatusResponse)
def get_live_analysis_job(job_id: str) -> LiveAnalysisJobStatusResponse:
    job = get_live_analysis_job_service().get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail='Live analysis job not found.')
    result = job.get('result')
    return LiveAnalysisJobStatusResponse(
        job=_to_live_analysis_job_response(job),
        result=_to_live_analysis_response(result) if isinstance(result, dict) and result else None,
    )


@router.post('/live-analysis/jobs/{job_id}/retry', response_model=LiveAnalysisJobActionResponse)
def retry_live_analysis_job(job_id: str) -> LiveAnalysisJobActionResponse:
    try:
        job = get_live_analysis_job_service().retry_job(job_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if 'not found' in message.lower() else 422
        raise HTTPException(status_code=status_code, detail=message) from exc
    return LiveAnalysisJobActionResponse(
        job=_to_live_analysis_job_response(job),
        message=f'Live analysis job {job_id} queued for retry.',
    )


@router.post('/live-analysis/jobs/{job_id}/cancel', response_model=LiveAnalysisJobActionResponse)
def cancel_live_analysis_job(job_id: str) -> LiveAnalysisJobActionResponse:
    try:
        job = get_live_analysis_job_service().cancel_job(job_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if 'not found' in message.lower() else 422
        raise HTTPException(status_code=status_code, detail=message) from exc
    return LiveAnalysisJobActionResponse(
        job=_to_live_analysis_job_response(job),
        message=f'Live analysis job {job_id} cancelled.',
    )


@router.post('/live-analysis', response_model=LiveAnalysisResponse | LiveAnalysisJobAcceptedResponse)
def post_live_analysis(
    request: LiveAnalysisRequest,
    db: Session = Depends(get_db),
) -> LiveAnalysisResponse | LiveAnalysisJobAcceptedResponse:
    if not _legacy_sync_live_analysis_enabled():
        job = get_live_analysis_job_service().submit(request)
        poll_url = f"/dashboard/live-analysis/jobs/{job['job_id']}"
        return LiveAnalysisJobAcceptedResponse(
            job=_to_live_analysis_job_response(job),
            poll_url=poll_url,
            latest_result_url='/dashboard/live-analysis/latest',
        )
    result = run_live_trading_analysis(
        symbol=request.symbol,
        interval=request.interval,
        period=request.period,
        strategy=request.strategy,
        force_market_refresh=request.force_market_refresh,
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
        max_position_value=request.max_position_value,
        max_open_positions=request.max_open_positions,
        max_symbol_exposure_pct=request.max_symbol_exposure_pct,
        max_portfolio_exposure_pct=request.max_portfolio_exposure_pct,
        max_open_risk_pct=request.max_open_risk_pct,
        kill_switch_enabled=request.kill_switch_enabled,
        db_session=db,
    )
    return _to_live_analysis_response(result)
