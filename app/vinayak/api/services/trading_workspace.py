from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from vinayak.api.services.analysis_service import (
    attach_indicator_trade_levels as analysis_attach_indicator_trade_levels,
    attach_lots as analysis_attach_lots,
    attach_option_metrics as analysis_attach_option_metrics,
    build_analysis,
    normalize_rows as analysis_normalize_rows,
    resolve_workspace_auto_execution_mode as analysis_resolve_workspace_auto_execution_mode,
    validation_summary_from_rows as analysis_validation_summary_from_rows,
)
from vinayak.api.services.data_preparation import prepare_trading_data as canonical_prepare_trading_data
from vinayak.api.services.execution_coordinator import coordinate_workspace_execution
from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.api.services.market_data_service import (
    data_status as market_data_status,
    df_to_candles as market_df_to_candles,
    prepare_trading_data as market_prepare_trading_data,
    recent_market_snapshot as market_recent_market_snapshot,
    refresh_market_data_snapshot as market_refresh_market_data_snapshot,
    update_observability_metrics_from_run as market_update_observability_metrics_from_run,
)
from vinayak.api.services.notification_service import dispatch_signal_summary
from vinayak.api.services.report_service import (
    build_report_artifacts as report_build_artifacts,
    empty_report_artifacts as report_empty_artifacts,
)
from vinayak.api.services.report_storage import cache_json_artifact, store_json_report, store_text_report
from vinayak.api.services.strategy_workflow import Candle, StrategyContext, run_strategy_workflow
from vinayak.api.services.strike_selector import attach_option_strikes
from vinayak.analytics.readiness import evaluate_readiness
from vinayak.execution.gateway import execute_workspace_candidates
from vinayak.legacy.market_data import load_legacy_security_map
from vinayak.legacy.options import build_legacy_option_metrics_map, extract_legacy_option_records, fetch_legacy_option_chain, normalize_legacy_index_symbol
from vinayak.messaging.bus import build_message_bus
from vinayak.messaging.events import EVENT_ANALYSIS_COMPLETED
from vinayak.metrics import run_full_metrics_engine
from vinayak.notifications.telegram.service import build_trade_summary, send_telegram_message
from vinayak.observability.alerting import publish_active_alerts
from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import get_observability_snapshot, record_stage, set_metric
from vinayak.validation.trade_evaluation import build_trade_evaluation_summary

if pd is None:  # pragma: no cover
    raise ModuleNotFoundError('pandas is required for trading workspace integration')


DEFAULT_PAPER_LOG_PATH = Path('app/vinayak/data/paper_trading_logs_all.csv')
DEFAULT_LIVE_LOG_PATH = Path('app/vinayak/data/live_trading_logs_all.csv')
MARKET_HEARTBEAT_MIN_REFRESH_SECONDS = 300
MARKET_HEARTBEAT_MAX_ROWS = 120

def _resolve_workspace_auto_execution_mode(requested_execution_type: str, auto_execute: bool) -> tuple[str, str]:
    return analysis_resolve_workspace_auto_execution_mode(requested_execution_type, auto_execute)


def prepare_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    return market_prepare_trading_data(df, canonical_prepare_trading_data_fn=canonical_prepare_trading_data)

def df_to_candles(df: pd.DataFrame) -> list[Candle]:
    return market_df_to_candles(df, candle_cls=Candle)


def _format_expiry(expiry: object) -> str:
    if expiry is None:
        return ''
    text = str(expiry).strip()
    if not text or text in {'-', 'N/A'}:
        return ''
    for fmt in ('%Y-%m-%d', '%d-%b-%Y'):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


def _estimate_weekly_expiry(symbol: str, now: datetime | None = None) -> str:
    s = (symbol or '').strip().upper()
    if s in {'^NSEI', 'NIFTY', 'NIFTY 50', 'NIFTY50'}:
        dt = now or datetime.now(UTC) + timedelta(hours=5, minutes=30)
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ''


def attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    return analysis_attach_indicator_trade_levels(rows, rr_ratio, trailing_sl_pct)


def attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    return analysis_attach_option_metrics(
        rows,
        symbol=symbol,
        fetch_option_metrics=fetch_option_metrics,
        fetch_legacy_option_chain_fn=fetch_legacy_option_chain,
        extract_legacy_option_records_fn=extract_legacy_option_records,
        build_legacy_option_metrics_map_fn=build_legacy_option_metrics_map,
        normalize_legacy_index_symbol_fn=normalize_legacy_index_symbol,
    )


def attach_lots(rows: list[dict[str, object]], lot_size: int, lots: int) -> list[dict[str, object]]:
    return analysis_attach_lots(rows, lot_size=lot_size, lots=lots)


def _normalize_rows(rows: list[dict[str, object]]) -> list[dict[str, Any]]:
    return analysis_normalize_rows(rows)


def _resolve_live_execution_kwargs(security_map_path: str) -> dict[str, object]:
    security_map: dict[str, dict[str, str]] = {}
    if load_legacy_security_map is not None:
        try:
            security_map = load_legacy_security_map(Path(str(security_map_path)))
        except Exception:
            security_map = {}
    return {'broker_name': 'DHAN', 'security_map': security_map}


def _build_report_artifacts(result: dict[str, Any], *, summary_text: str | None = None) -> dict[str, dict[str, str]]:
    return report_build_artifacts(
        result,
        summary_text=summary_text,
        build_trade_summary_fn=build_trade_summary,
        store_json_report_fn=store_json_report,
        store_text_report_fn=store_text_report,
        cache_json_artifact_fn=cache_json_artifact,
    )


def _empty_report_artifacts() -> dict[str, dict[str, str]]:
    return report_empty_artifacts()


def _validation_summary_from_rows(rows: list[dict[str, Any]], strategy: str) -> dict[str, Any]:
    return analysis_validation_summary_from_rows(rows, strategy=strategy)


def _data_status(candles: pd.DataFrame) -> dict[str, Any]:
    return market_data_status(candles)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = pd.to_datetime(text, errors='coerce', utc=True)
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _recent_market_snapshot(interval: str, *, max_age_seconds: int = MARKET_HEARTBEAT_MIN_REFRESH_SECONDS) -> dict[str, Any] | None:
    return market_recent_market_snapshot(
        interval,
        get_observability_snapshot_fn=get_observability_snapshot,
        max_age_seconds=max_age_seconds,
    )

def _update_observability_metrics_from_run(rows: list[dict[str, Any]], candles: pd.DataFrame) -> None:
    market_update_observability_metrics_from_run(
        rows,
        candles,
        run_full_metrics_engine_fn=run_full_metrics_engine,
        set_metric_fn=set_metric,
    )

def refresh_market_data_snapshot(
    *,
    symbol: str,
    interval: str,
    period: str,
    security_map_path: str = 'data/dhan_security_map.csv',
) -> dict[str, Any]:
    return market_refresh_market_data_snapshot(
        symbol=symbol,
        interval=interval,
        period=period,
        security_map_path=security_map_path,
        fetch_live_ohlcv_fn=fetch_live_ohlcv,
        prepare_trading_data_fn=prepare_trading_data,
        normalize_rows_fn=_normalize_rows,
        recent_market_snapshot_fn=_recent_market_snapshot,
        update_observability_metrics_from_run_fn=_update_observability_metrics_from_run,
        data_status_fn=_data_status,
    )

def run_live_trading_analysis(
    *,
    symbol: str,
    interval: str,
    period: str,
    strategy: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    mtf_ema_period: int,
    mtf_setup_mode: str,
    mtf_retest_strength: bool,
    mtf_max_trades_per_day: int,
    entry_cutoff_hhmm: str = '',
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = None,
    max_position_value: float | None = None,
    max_open_positions: int | None = None,
    max_symbol_exposure_pct: float | None = None,
    max_portfolio_exposure_pct: float | None = None,
    max_open_risk_pct: float | None = None,
    kill_switch_enabled: bool = False,
    fetch_option_metrics: bool = False,
    send_telegram: bool = False,
    telegram_token: str = '',
    telegram_chat_id: str = '',
    auto_execute: bool = False,
    execution_type: str = 'NONE',
    lot_size: int = 0,
    lots: int = 0,
    force_market_refresh: bool = False,
    security_map_path: str = 'data/dhan_security_map.csv',
    paper_log_path: str = str(DEFAULT_PAPER_LOG_PATH),
    live_log_path: str = str(DEFAULT_LIVE_LOG_PATH),
    db_session: Session | None = None,
    persist_reports: bool = True,
    publish_completion_event: bool = True,
    deliver_telegram_inline: bool = True,
    publish_alert_notifications: bool = True,
    execute_inline: bool = True,
) -> dict[str, Any]:
    import time
    trace_id = f"{symbol}_{strategy}_{int(time.time())}"
    started = time.perf_counter()
    fetch_started = time.perf_counter()
    live_rows = fetch_live_ohlcv(
        symbol=symbol,
        interval=interval,
        period=period,
        provider='DHAN',
        security_map_path=security_map_path,
        force_refresh=bool(force_market_refresh),
    )
    record_stage('market_fetch', status='SUCCESS', duration_seconds=round(time.perf_counter() - fetch_started, 4), symbol=symbol, strategy=strategy, message='Live market rows fetched', trace_id=trace_id)
    prep_started = time.perf_counter()
    candles_df = prepare_trading_data(pd.DataFrame(live_rows))
    candle_rows = df_to_candles(candles_df)
    record_stage('indicator_calc', status='SUCCESS', duration_seconds=round(time.perf_counter() - prep_started, 4), symbol=symbol, strategy=strategy, message='Data prepared for indicators', trace_id=trace_id)

    context = StrategyContext(
        strategy=strategy,
        candles=candles_df,
        candle_rows=candle_rows,
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=float(trailing_sl_pct),
        symbol=str(symbol),
        strike_step=int(strike_step),
        moneyness=str(moneyness),
        strike_steps=int(strike_steps),
        fetch_option_metrics=bool(fetch_option_metrics),
        mtf_ema_period=int(mtf_ema_period),
        mtf_setup_mode=str(mtf_setup_mode),
        mtf_retest_strength=bool(mtf_retest_strength),
        mtf_max_trades_per_day=int(mtf_max_trades_per_day),
        entry_cutoff=str(entry_cutoff_hhmm),
        cost_bps=float(cost_bps),
        fixed_cost_per_trade=float(fixed_cost_per_trade),
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
        max_position_value=max_position_value,
        max_open_positions=max_open_positions,
        max_symbol_exposure_pct=max_symbol_exposure_pct,
        max_portfolio_exposure_pct=max_portfolio_exposure_pct,
        max_open_risk_pct=max_open_risk_pct,
        kill_switch_enabled=kill_switch_enabled,
    )
    strategy_started = time.perf_counter()
    analysis_result = build_analysis(
        context=context,
        run_strategy_workflow_fn=run_strategy_workflow,
        attach_levels_fn=attach_indicator_trade_levels,
        attach_option_strikes_fn=attach_option_strikes,
        attach_option_metrics_fn=attach_option_metrics,
        attach_lots_fn=attach_lots,
        lot_size=lot_size,
        lots=lots,
    )
    signal_rows = list(analysis_result['signals'])
    record_stage('zone_detection', status='SUCCESS', duration_seconds=round(time.perf_counter() - strategy_started, 4), symbol=symbol, strategy=strategy, message='Strategy workflow completed', trace_id=trace_id)

    message_bus = build_message_bus()
    notification_result = dispatch_signal_summary(
        send_telegram=send_telegram,
        signal_rows=signal_rows,
        symbol=symbol,
        strategy=strategy,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        deliver_telegram_inline=deliver_telegram_inline,
        build_trade_summary_fn=build_trade_summary,
        send_telegram_message_fn=send_telegram_message,
        log_exception_fn=log_exception,
        message_bus=message_bus,
    )

    requested_execution_mode = str(execution_type or 'NONE').upper()
    execution_mode, execution_note = _resolve_workspace_auto_execution_mode(requested_execution_mode, auto_execute)
    execution_result = coordinate_workspace_execution(
        auto_execute=auto_execute,
        execution_mode=execution_mode,
        execution_note=execution_note,
        signal_rows=signal_rows,
        execute_inline=execute_inline,
        strategy=strategy,
        symbol=symbol,
        candles_df=candles_df,
        paper_log_path=paper_log_path,
        live_log_path=live_log_path,
        capital=capital,
        risk_pct=risk_pct,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_position_value=max_position_value,
        max_open_positions=max_open_positions,
        max_symbol_exposure_pct=max_symbol_exposure_pct,
        max_portfolio_exposure_pct=max_portfolio_exposure_pct,
        max_open_risk_pct=max_open_risk_pct,
        kill_switch_enabled=kill_switch_enabled,
        security_map_path=security_map_path,
        db_session=db_session,
        execute_workspace_candidates_fn=execute_workspace_candidates,
        normalize_rows_fn=_normalize_rows,
        resolve_live_kwargs_fn=_resolve_live_execution_kwargs,
    )

    record_stage('trade_build', status='SUCCESS', symbol=symbol, strategy=strategy, message='Trade rows built', trace_id=trace_id)
    validation_rows = execution_result['execution_rows'] if execution_result['execution_rows'] else signal_rows
    validation_summary = _validation_summary_from_rows(validation_rows, strategy)
    _update_observability_metrics_from_run(validation_rows, candles_df)
    alert_notifications_sent = 0
    if publish_alert_notifications:
        alert_notifications_sent = publish_active_alerts(
            message_bus=message_bus,
            telegram_token=telegram_token,
            telegram_chat_id=telegram_chat_id,
            source='live_analysis_alerting',
        )

    total_duration = round(time.perf_counter() - started, 4)
    set_metric('trading_cycle_duration_seconds', total_duration)
    log_event(component='trading_workspace', event_name='live_analysis_run', symbol=symbol, strategy=strategy, severity='INFO', message='Live analysis run completed', context_json={'signal_count': len(signal_rows), 'execution_rows': len(execution_result['execution_rows']), 'duration_seconds': total_duration, 'trace_id': trace_id})
    response: dict[str, Any] = {
        'symbol': symbol,
        'interval': interval,
        'period': period,
        'strategy': strategy,
        'candle_count': len(live_rows),
        'signal_count': len(signal_rows),
        'side_counts': analysis_result['side_counts'],
        'candles': _normalize_rows(live_rows),
        'signals': signal_rows,
        'generated_at': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'telegram_sent': notification_result['telegram_sent'],
        'telegram_error': notification_result['telegram_error'],
        'telegram_payload': notification_result['telegram_payload'],
        'execution_note': execution_result['execution_note'],
        'execution_summary': execution_result['execution_summary'],
        'execution_rows': execution_result['execution_rows'],
        'validation_summary': validation_summary,
        'data_status': {**_data_status(candles_df), 'provider': str((live_rows[-1] if live_rows else {}).get('provider', '') or ''), 'source': str((live_rows[-1] if live_rows else {}).get('source', '') or ''), 'latest_interval': str((live_rows[-1] if live_rows else {}).get('interval', interval) or interval)},
        'system_status': validation_summary.get('system_status', 'NOT_READY'),
        'alert_notifications_sent': alert_notifications_sent,
    }
    if persist_reports:
        response['report_artifacts'] = _build_report_artifacts(response)
    else:
        cache_json_artifact('latest_live_analysis', response)
        response['report_artifacts'] = _empty_report_artifacts()
    if publish_completion_event:
        message_bus.publish(
            EVENT_ANALYSIS_COMPLETED,
            {
                'symbol': symbol,
                'strategy': strategy,
                'interval': interval,
                'period': period,
                'signal_count': len(signal_rows),
                'execution_mode': execution_mode,
                'requested_execution_mode': requested_execution_mode,
                'report_artifacts': response['report_artifacts'],
            },
            source='live_analysis',
        )
    return response







































