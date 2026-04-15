from __future__ import annotations

import os
from typing import Any

import pandas as pd

from vinayak.db.repositories.deferred_execution_job_repository import DeferredExecutionJobRepository
from vinayak.db.session import build_session_factory
from vinayak.execution.gateway import execute_workspace_candidates
from vinayak.messaging.bus import EventEnvelope, build_message_bus
from vinayak.messaging.events import EVENT_DEFERRED_EXECUTION_REQUESTED, EVENT_NOTIFICATION_REQUESTED
from vinayak.notifications.telegram.notifier import send_text_notification
from vinayak.observability.observability_logger import log_exception
from vinayak.observability.observability_metrics import increment_metric, set_metric


def _env_telegram_target() -> tuple[str, str]:
    token = str(os.getenv('TELEGRAM_BOT_TOKEN', '') or '').strip()
    chat_id = str(os.getenv('TELEGRAM_CHAT_ID', '') or '').strip()
    return token, chat_id


def _handle_deferred_execution(payload: dict[str, Any]) -> None:
    candles = list(payload.get('candles', []) or [])
    signals = [dict(item) for item in list(payload.get('signals', []) or []) if isinstance(item, dict)]
    if not candles or not signals:
        return
    increment_metric('deferred_execution_attempt_total', 1)
    session_factory = build_session_factory()
    session = session_factory()
    deferred_job_repo = DeferredExecutionJobRepository(session)
    deferred_job_id = str(payload.get('deferred_execution_job_id', '') or '').strip()
    deferred_job = deferred_job_repo.get_job(deferred_job_id) if deferred_job_id else None
    try:
        if deferred_job is not None:
            deferred_job_repo.mark_running(deferred_job)
        execute_workspace_candidates(
            str(payload.get('strategy', '') or ''),
            str(payload.get('symbol', '') or ''),
            pd.DataFrame(candles),
            signals,
            execution_mode=str(payload.get('execution_mode', 'NONE') or 'NONE'),
            paper_log_path=str(payload.get('paper_log_path', 'app/vinayak/data/paper_trading_logs_all.csv') or 'app/vinayak/data/paper_trading_logs_all.csv'),
            live_log_path=str(payload.get('live_log_path', 'app/vinayak/data/live_trading_logs_all.csv') or 'app/vinayak/data/live_trading_logs_all.csv'),
            capital=payload.get('capital'),
            per_trade_risk_pct=payload.get('per_trade_risk_pct'),
            max_trades_per_day=payload.get('max_trades_per_day'),
            max_daily_loss=payload.get('max_daily_loss'),
            max_position_value=payload.get('max_position_value'),
            max_open_positions=payload.get('max_open_positions'),
            max_symbol_exposure_pct=payload.get('max_symbol_exposure_pct'),
            max_portfolio_exposure_pct=payload.get('max_portfolio_exposure_pct'),
            max_open_risk_pct=payload.get('max_open_risk_pct'),
            kill_switch_enabled=bool(payload.get('kill_switch_enabled', False)),
            security_map_path=str(payload.get('security_map_path', 'data/dhan_security_map.csv') or 'data/dhan_security_map.csv'),
            db_session=session,
        )
        if deferred_job is not None:
            deferred_job_repo.mark_succeeded(
                deferred_job,
                {
                    'execution_mode': str(payload.get('execution_mode', 'NONE') or 'NONE'),
                    'symbol': str(payload.get('symbol', '') or ''),
                    'strategy': str(payload.get('strategy', '') or ''),
                },
            )
        session.commit()
        increment_metric('deferred_execution_success_total', 1)
        set_metric('deferred_execution_last_status', 'SUCCESS')
    except Exception as exc:
        session.rollback()
        if deferred_job is not None:
            deferred_job_repo = DeferredExecutionJobRepository(session)
            failed_job = deferred_job_repo.get_job(deferred_job.id)
            if failed_job is not None:
                deferred_job_repo.mark_failed(failed_job, str(exc))
                session.commit()
        increment_metric('deferred_execution_failed_total', 1)
        set_metric('deferred_execution_last_status', 'FAIL')
        log_exception(
            component='event_worker',
            event_name='deferred_execution_failed',
            exc=exc,
            symbol=str(payload.get('symbol', '') or ''),
            strategy=str(payload.get('strategy', '') or ''),
            message='Event worker failed to process deferred execution request',
            context_json={'execution_mode': str(payload.get('execution_mode', '') or '')},
        )
        raise
    finally:
        session.close()


def _handle_event(event: EventEnvelope) -> None:
    if event.name == EVENT_DEFERRED_EXECUTION_REQUESTED:
        _handle_deferred_execution(event.payload)
        return
    if event.name != EVENT_NOTIFICATION_REQUESTED:
        return
    payload = event.payload
    token, chat_id = _env_telegram_target()
    message = str(payload.get('message', '') or '')
    if token and chat_id and message:
        try:
            send_text_notification(token=token, chat_id=chat_id, message=message)
        except Exception as exc:
            log_exception(
                component='event_worker',
                event_name='notification_delivery_failed',
                exc=exc,
                symbol=str(payload.get('symbol', '') or ''),
                strategy=str(payload.get('strategy', '') or ''),
                message='Event worker failed to deliver Telegram notification',
                context_json={'message_preview': message[:200], 'channel': 'telegram'},
            )


def main() -> None:
    bus = build_message_bus()
    bus.consume(_handle_event)


if __name__ == '__main__':
    main()
