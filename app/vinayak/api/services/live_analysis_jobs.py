from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from vinayak.api.schemas.strategy import LiveAnalysisRequest
from vinayak.api.services.trading_workspace import run_live_trading_analysis
from vinayak.db.repositories.live_analysis_job_repository import LiveAnalysisJobRepository
from vinayak.db.session import build_session_factory
from vinayak.observability.observability_logger import log_exception
from vinayak.observability.observability_metrics import increment_metric, set_metric


def _utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def _job_key(payload: dict[str, Any]) -> str:
    return '|'.join([
        str(payload.get('symbol', '') or '').strip().upper(),
        str(payload.get('interval', '') or '').strip().lower(),
        str(payload.get('period', '') or '').strip().lower(),
        str(payload.get('strategy', '') or '').strip(),
    ])


def _serialize_job(record, repo: LiveAnalysisJobRepository, *, deduplicated: bool = False) -> dict[str, Any]:
    result = repo.parse_result_payload(record) or {}
    return {
        'job_id': record.id,
        'status': str(record.status or ''),
        'symbol': str(record.symbol or ''),
        'interval': str(record.interval or ''),
        'period': str(record.period or ''),
        'strategy': str(record.strategy or ''),
        'requested_at': _utc_text(record.requested_at) or '',
        'started_at': _utc_text(record.started_at),
        'finished_at': _utc_text(record.finished_at),
        'error': record.error,
        'deduplicated': deduplicated,
        'result': dict(result) if result else None,
        'signal_count': int(result.get('signal_count', 0) or 0),
        'candle_count': int(result.get('candle_count', 0) or 0),
    }


class LiveAnalysisJobService:
    def __init__(self, session_factory=None) -> None:
        self._session_factory = session_factory or build_session_factory()

    def _refresh_queue_metrics(self, repo: LiveAnalysisJobRepository) -> None:
        metrics = repo.queue_metrics()
        set_metric('live_analysis_jobs_pending', int(metrics['pending_count']))
        set_metric('live_analysis_jobs_running', int(metrics['running_count']))
        set_metric('live_analysis_jobs_oldest_pending_age_seconds', float(metrics['oldest_pending_age_seconds']))

    def submit(self, request: LiveAnalysisRequest) -> dict[str, Any]:
        payload = request.model_dump()
        dedup_key = _job_key(payload)
        session = self._session_factory()
        try:
            repo = LiveAnalysisJobRepository(session)
            existing = repo.find_active_job_by_key(dedup_key)
            if existing is not None:
                increment_metric('live_analysis_job_deduplicated_total', 1)
                self._refresh_queue_metrics(repo)
                return _serialize_job(existing, repo, deduplicated=True)

            record = repo.create_job(
                job_id=str(uuid4()),
                dedup_key=dedup_key,
                symbol=str(payload.get('symbol', '') or ''),
                interval=str(payload.get('interval', '') or ''),
                period=str(payload.get('period', '') or ''),
                strategy=str(payload.get('strategy', '') or ''),
                request_payload=payload,
            )
            session.commit()
            session.refresh(record)
            increment_metric('live_analysis_job_enqueued_total', 1)
            self._refresh_queue_metrics(repo)
            return _serialize_job(record, repo)
        finally:
            session.close()

    def get(self, job_id: str) -> dict[str, Any] | None:
        session = self._session_factory()
        try:
            repo = LiveAnalysisJobRepository(session)
            record = repo.get_job(job_id)
            return _serialize_job(record, repo) if record is not None else None
        finally:
            session.close()

    def list_jobs(self, *, limit: int = 50, status: str | None = None) -> list[dict[str, Any]]:
        session = self._session_factory()
        try:
            repo = LiveAnalysisJobRepository(session)
            return [_serialize_job(record, repo) for record in repo.list_jobs(limit=limit, status=status)]
        finally:
            session.close()

    def retry_job(self, job_id: str) -> dict[str, Any]:
        session = self._session_factory()
        try:
            repo = LiveAnalysisJobRepository(session)
            record = repo.get_job(job_id)
            if record is None:
                raise ValueError('Live analysis job was not found.')
            if str(record.status or '').upper() not in {'FAILED', 'CANCELLED'}:
                raise ValueError('Only failed or cancelled live analysis jobs can be retried.')
            repo.retry_job(record)
            session.commit()
            increment_metric('live_analysis_job_retried_total', 1)
            self._refresh_queue_metrics(repo)
            return _serialize_job(record, repo)
        finally:
            session.close()

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        session = self._session_factory()
        try:
            repo = LiveAnalysisJobRepository(session)
            record = repo.get_job(job_id)
            if record is None:
                raise ValueError('Live analysis job was not found.')
            if str(record.status or '').upper() not in {'PENDING', 'RUNNING'}:
                raise ValueError('Only pending or running live analysis jobs can be cancelled.')
            repo.cancel_job(record)
            session.commit()
            increment_metric('live_analysis_job_cancelled_total', 1)
            self._refresh_queue_metrics(repo)
            return _serialize_job(record, repo)
        finally:
            session.close()

    def process_next_pending_job(self) -> bool:
        session = self._session_factory()
        repo = LiveAnalysisJobRepository(session)
        recovered = repo.requeue_stale_running_jobs()
        if recovered:
            increment_metric('live_analysis_job_recovered_total', recovered)
        record = repo.claim_next_pending_job()
        if record is None:
            self._refresh_queue_metrics(repo)
            session.close()
            return False
        session.commit()
        increment_metric('live_analysis_job_claimed_total', 1)
        self._refresh_queue_metrics(repo)
        payload = repo.parse_request_payload(record)
        try:
            result = run_live_trading_analysis(
                symbol=str(payload.get('symbol', '^NSEI') or '^NSEI'),
                interval=str(payload.get('interval', '5m') or '5m'),
                period=str(payload.get('period', '1d') or '1d'),
                strategy=str(payload.get('strategy', 'Breakout') or 'Breakout'),
                capital=float(payload.get('capital', 100000.0) or 100000.0),
                risk_pct=float(payload.get('risk_pct', 1.0) or 1.0),
                rr_ratio=float(payload.get('rr_ratio', 2.0) or 2.0),
                trailing_sl_pct=float(payload.get('trailing_sl_pct', 0.5) or 0.5),
                strike_step=int(payload.get('strike_step', 50) or 50),
                moneyness=str(payload.get('moneyness', 'ATM') or 'ATM'),
                strike_steps=int(payload.get('strike_steps', 0) or 0),
                fetch_option_metrics=bool(payload.get('fetch_option_metrics', False)),
                send_telegram=bool(payload.get('send_telegram', False)),
                telegram_token=str(payload.get('telegram_token', '') or ''),
                telegram_chat_id=str(payload.get('telegram_chat_id', '') or ''),
                auto_execute=bool(payload.get('auto_execute', False)),
                execution_type=str(payload.get('execution_type', 'NONE') or 'NONE'),
                lot_size=int(payload.get('lot_size', 0) or 0),
                lots=int(payload.get('lots', 0) or 0),
                security_map_path=str(payload.get('security_map_path', 'data/dhan_security_map.csv') or 'data/dhan_security_map.csv'),
                paper_log_path=str(payload.get('paper_log_path', 'app/vinayak/data/paper_trading_logs_all.csv') or 'app/vinayak/data/paper_trading_logs_all.csv'),
                live_log_path=str(payload.get('live_log_path', 'app/vinayak/data/live_trading_logs_all.csv') or 'app/vinayak/data/live_trading_logs_all.csv'),
                mtf_ema_period=int(payload.get('mtf_ema_period', 3) or 3),
                mtf_setup_mode=str(payload.get('mtf_setup_mode', 'either') or 'either'),
                mtf_retest_strength=bool(payload.get('mtf_retest_strength', True)),
                mtf_max_trades_per_day=int(payload.get('mtf_max_trades_per_day', 3) or 3),
                entry_cutoff_hhmm=str(payload.get('entry_cutoff_hhmm', '') or ''),
                cost_bps=float(payload.get('cost_bps', 0.0) or 0.0),
                fixed_cost_per_trade=float(payload.get('fixed_cost_per_trade', 0.0) or 0.0),
                max_daily_loss=payload.get('max_daily_loss'),
                max_trades_per_day=payload.get('max_trades_per_day'),
                max_position_value=payload.get('max_position_value'),
                max_open_positions=payload.get('max_open_positions'),
                max_symbol_exposure_pct=payload.get('max_symbol_exposure_pct'),
                max_portfolio_exposure_pct=payload.get('max_portfolio_exposure_pct'),
                max_open_risk_pct=payload.get('max_open_risk_pct'),
                kill_switch_enabled=bool(payload.get('kill_switch_enabled', False)),
                db_session=session,
            )
        except Exception as exc:
            log_exception(
                component='live_analysis_jobs',
                event_name='live_analysis_job_failed',
                exc=exc,
                symbol=str(payload.get('symbol', '') or ''),
                strategy=str(payload.get('strategy', '') or ''),
                message='Persistent live-analysis job failed',
                context_json={'job_id': record.id},
            )
            repo.mark_failed(record, str(exc))
            session.commit()
            increment_metric('live_analysis_job_failed_total', 1)
            self._refresh_queue_metrics(repo)
            session.close()
            return True
        repo.mark_succeeded(record, result)
        session.commit()
        increment_metric('live_analysis_job_succeeded_total', 1)
        self._refresh_queue_metrics(repo)
        session.close()
        return True


_JOB_SERVICE = LiveAnalysisJobService()


def get_live_analysis_job_service() -> LiveAnalysisJobService:
    return _JOB_SERVICE


def process_next_live_analysis_job() -> bool:
    return _JOB_SERVICE.process_next_pending_job()
