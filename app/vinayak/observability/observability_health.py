from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json
import os
import time

from vinayak.cache.redis_client import RedisCache
from vinayak.observability.alerting import build_active_alerts
from vinayak.observability.observability_logger import tail_events
from vinayak.observability.observability_metrics import get_observability_snapshot


_REDIS_CACHE = RedisCache.from_env()
_LATEST_ANALYSIS_TTL_SECONDS = 2.0
_LATEST_ANALYSIS_CACHE: dict[str, Any] = {
    'signature': None,
    'loaded_at': 0.0,
    'value': {},
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except Exception:
        return default


def _parse_ts(value: Any) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace('Z', '+00:00')):
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            continue
    return None


def _status_color(status: str) -> str:
    normalized = str(status or '').upper()
    if normalized in {'UP', 'HEALTHY', 'OK', 'SUCCESS', 'VALID', 'FRESH', 'PASS'}:
        return 'green'
    if normalized in {'WARN', 'WARNING', 'STALE', 'DEGRADED', 'PAPER_ONLY'}:
        return 'yellow'
    if normalized in {'BLUE', 'INFO'}:
        return 'blue'
    return 'red'


def _metric(snapshot: dict[str, Any], name: str, default: Any = 0) -> Any:
    return snapshot.get('metrics', {}).get(name, {}).get('value', default)


def _reports_dir() -> Path:
    return Path(os.getenv('REPORTS_DIR', 'app/vinayak/data/reports'))


def _load_latest_analysis() -> dict[str, Any]:
    cached = _REDIS_CACHE.get_json('vinayak:artifact:latest_live_analysis') if _REDIS_CACHE.is_configured() else None
    if isinstance(cached, dict) and cached:
        return cached
    report_files = sorted(_reports_dir().glob('*live_analysis_result.json'), key=lambda path: path.stat().st_mtime, reverse=True)
    latest_path = report_files[0] if report_files else None
    if latest_path is None or not latest_path.exists() or latest_path.stat().st_size == 0:
        _LATEST_ANALYSIS_CACHE['signature'] = None
        _LATEST_ANALYSIS_CACHE['loaded_at'] = time.monotonic()
        _LATEST_ANALYSIS_CACHE['value'] = {}
        return {}
    stat = latest_path.stat()
    signature = (str(latest_path.resolve()), stat.st_mtime_ns, stat.st_size)
    now = time.monotonic()
    if _LATEST_ANALYSIS_CACHE.get('signature') == signature and (now - float(_LATEST_ANALYSIS_CACHE.get('loaded_at', 0.0))) <= _LATEST_ANALYSIS_TTL_SECONDS:
        return dict(_LATEST_ANALYSIS_CACHE.get('value', {}))
    try:
        payload = json.loads(latest_path.read_text(encoding='utf-8'))
    except Exception:
        payload = {}
    value = payload if isinstance(payload, dict) else {}
    _LATEST_ANALYSIS_CACHE['signature'] = signature
    _LATEST_ANALYSIS_CACHE['loaded_at'] = now
    _LATEST_ANALYSIS_CACHE['value'] = value
    return dict(value)


def _build_detail_cards(row: dict[str, Any], fields: list[tuple[str, str]]) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for label, key in fields:
        value = row.get(key, '-')
        if value in (None, ''):
            value = '-'
        cards.append({'label': label, 'value': value})
    return cards


def build_observability_dashboard_payload() -> dict[str, Any]:
    snapshot = get_observability_snapshot()
    metrics = snapshot.get('metrics', {})
    now = datetime.now(UTC)

    latest_ts = _metric(snapshot, 'latest_data_timestamp', '')
    latest_dt = _parse_ts(latest_ts)
    data_delay = _safe_float(_metric(snapshot, 'market_data_delay_seconds', 0.0))
    trading_app_up = bool(_metric(snapshot, 'trading_app_up', 0))
    cycle_failures = int(_safe_float(_metric(snapshot, 'trading_cycle_failures_total', 0)))
    total_signals = int(_safe_float(_metric(snapshot, 'total_signals_today', _metric(snapshot, 'trade_candidates_total', 0))))
    valid_signals = int(_safe_float(_metric(snapshot, 'valid_signals_today', _metric(snapshot, 'zones_accepted_total', 0))))
    executed_trades = int(_safe_float(_metric(snapshot, 'executed_paper_trades_today', _metric(snapshot, 'paper_trades_executed_total', 0))))
    telegram_failures = int(_safe_float(_metric(snapshot, 'telegram_send_failures_total', 0)))
    telegram_last = _metric(snapshot, 'telegram_last_delivery_timestamp', '')
    pnl_today = round(_safe_float(_metric(snapshot, 'pnl_today', 0.0)), 2)
    win_rate = round(_safe_float(_metric(snapshot, 'rolling_win_rate', 0.0)), 2)
    expectancy = round(_safe_float(_metric(snapshot, 'rolling_expectancy', 0.0)), 2)
    execution_attempts = int(_safe_float(_metric(snapshot, 'execution_attempt_total', 0)))
    execution_success = int(_safe_float(_metric(snapshot, 'execution_success_total', 0)))
    execution_failed = int(_safe_float(_metric(snapshot, 'execution_failed_total', 0)))
    execution_blocked = int(_safe_float(_metric(snapshot, 'execution_blocked_total', 0)))
    duplicate_execution_blocks = int(_safe_float(_metric(snapshot, 'duplicate_execution_block_total', 0)))
    kill_switch_active = bool(_safe_float(_metric(snapshot, 'portfolio_kill_switch_active', 0)))
    daily_loss_limit = round(abs(_safe_float(_metric(snapshot, 'portfolio_daily_loss_limit', 0.0))), 2)
    per_trade_risk_pct = round(_safe_float(_metric(snapshot, 'portfolio_per_trade_risk_pct', 0.0)), 4)
    max_trades_per_day = int(_safe_float(_metric(snapshot, 'portfolio_max_trades_per_day', 0)))
    alert_notifications = int(_safe_float(_metric(snapshot, 'observability_alert_notifications_total', 0)))
    active_alerts = build_active_alerts(snapshot)

    app_status = 'UP' if trading_app_up and cycle_failures == 0 else 'DEGRADED' if trading_app_up else 'DOWN'
    market_status = 'FRESH' if latest_dt is not None and data_delay <= 180 else 'STALE'
    signal_status = 'PASS' if total_signals == 0 or valid_signals >= max(1, total_signals // 2) else 'WARN'
    execution_status = 'OK' if execution_failed == 0 and execution_blocked == 0 else 'WARN' if execution_success > 0 else 'FAIL'
    telegram_status = 'OK' if telegram_last and telegram_failures == 0 else 'WARN' if telegram_failures > 0 else 'IDLE'
    risk_status = 'FAIL' if kill_switch_active or (daily_loss_limit > 0 and pnl_today <= -daily_loss_limit) else 'WARN' if per_trade_risk_pct > 0 or max_trades_per_day > 0 else 'OK'

    recent_failures = tail_events(12, severities={'WARNING', 'ERROR', 'CRITICAL'})
    stages = snapshot.get('stages', {})
    latest_analysis = _load_latest_analysis()
    latest_signal = dict((latest_analysis.get('signals') or [{}])[-1] if latest_analysis.get('signals') else {})
    latest_execution = dict((latest_analysis.get('execution_rows') or [{}])[-1] if latest_analysis.get('execution_rows') else {})
    latest_candle = dict((latest_analysis.get('candles') or [{}])[-1] if latest_analysis.get('candles') else {})
    latest_data_status = dict(latest_analysis.get('data_status', {}) or {})

    if latest_signal and not latest_execution:
        latest_execution = {
            'trade_id': latest_signal.get('trade_id', '-'),
            'side': latest_signal.get('side', '-'),
            'execution_status': latest_analysis.get('execution_summary', {}).get('mode', '-'),
            'option_strike': latest_signal.get('option_strike', '-'),
            'strike_price': latest_signal.get('strike_price', '-'),
            'price': latest_signal.get('entry_price', latest_signal.get('entry', '-')),
            'reason': latest_analysis.get('execution_note', '-'),
        }

    kpis = [
        {'name': 'app_status', 'value': app_status, 'color': _status_color(app_status)},
        {'name': 'market_data_status', 'value': market_status, 'color': _status_color(market_status)},
        {'name': 'latest_data_timestamp', 'value': latest_ts or '-', 'color': _status_color(market_status)},
        {'name': 'total_signals_today', 'value': total_signals, 'color': 'green' if total_signals else 'yellow'},
        {'name': 'valid_signals_today', 'value': valid_signals, 'color': _status_color(signal_status)},
        {'name': 'executed_paper_trades_today', 'value': executed_trades, 'color': _status_color(execution_status)},
        {'name': 'execution_failed_total', 'value': execution_failed, 'color': 'red' if execution_failed else 'green'},
        {'name': 'execution_blocked_total', 'value': execution_blocked, 'color': 'yellow' if execution_blocked else 'green'},
        {'name': 'telegram_status', 'value': telegram_status, 'color': _status_color(telegram_status)},
        {'name': 'pnl_today', 'value': pnl_today, 'color': 'green' if pnl_today >= 0 else 'red'},
        {'name': 'kill_switch_active', 'value': kill_switch_active, 'color': 'red' if kill_switch_active else 'green'},
        {'name': 'active_alerts_total', 'value': len(active_alerts), 'color': 'red' if active_alerts else 'green'},
    ]

    return {
        'generated_at': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'kpis': kpis,
        'system_health': {
            'status': app_status,
            'color': _status_color(app_status),
            'cards': [
                {'label': 'trading_app_up', 'value': trading_app_up},
                {'label': 'trading_cycle_duration_seconds', 'value': _metric(snapshot, 'trading_cycle_duration_seconds', 0.0)},
                {'label': 'trading_cycle_failures_total', 'value': cycle_failures},
                {'label': 'active_alerts_total', 'value': len(active_alerts)},
            ],
        },
        'data_health': {
            'status': market_status,
            'color': _status_color(market_status),
            'cards': [
                {'label': 'market_data_delay_seconds', 'value': data_delay},
                {'label': 'market_data_rows_loaded_total', 'value': _metric(snapshot, 'market_data_rows_loaded_total', 0)},
                {'label': 'market_data_duplicates_total', 'value': _metric(snapshot, 'market_data_duplicates_total', 0)},
                {'label': 'market_data_nulls_total', 'value': _metric(snapshot, 'market_data_nulls_total', 0)},
                {'label': 'schema_validation_failures_total', 'value': _metric(snapshot, 'schema_validation_failures_total', 0)},
            ],
        },
        'strategy_health': {
            'status': signal_status,
            'color': _status_color(signal_status),
            'cards': [
                {'label': 'zones_detected_total', 'value': _metric(snapshot, 'zones_detected_total', 0)},
                {'label': 'zones_accepted_total', 'value': _metric(snapshot, 'zones_accepted_total', 0)},
                {'label': 'zones_rejected_total', 'value': _metric(snapshot, 'zones_rejected_total', 0)},
                {'label': 'zone_score_avg', 'value': _metric(snapshot, 'zone_score_avg', 0.0)},
                {'label': 'trade_candidates_total', 'value': _metric(snapshot, 'trade_candidates_total', 0)},
            ],
        },
        'execution_health': {
            'status': execution_status,
            'color': _status_color(execution_status),
            'cards': [
                {'label': 'execution_attempt_total', 'value': execution_attempts},
                {'label': 'execution_success_total', 'value': execution_success},
                {'label': 'execution_failed_total', 'value': execution_failed},
                {'label': 'execution_blocked_total', 'value': execution_blocked},
                {'label': 'duplicate_execution_block_total', 'value': duplicate_execution_blocks},
                {'label': 'paper_trades_executed_total', 'value': _metric(snapshot, 'paper_trades_executed_total', 0)},
                {'label': 'paper_trade_rejections_total', 'value': _metric(snapshot, 'paper_trade_rejections_total', 0)},
                {'label': 'duplicate_trade_blocks_total', 'value': _metric(snapshot, 'duplicate_trade_blocks_total', 0)},
            ],
        },
        'latest_market_data': _build_detail_cards(
            {
                'provider': latest_data_status.get('provider') or latest_candle.get('provider', '-'),
                'source': latest_data_status.get('source') or latest_candle.get('source', '-'),
                'latest_timestamp': latest_data_status.get('latest_timestamp') or latest_candle.get('timestamp', '-'),
                'interval': latest_data_status.get('latest_interval') or latest_candle.get('interval', '-'),
                'symbol': latest_candle.get('symbol', latest_analysis.get('symbol', '-')),
                'close': latest_candle.get('close', '-'),
            },
            [
                ('provider', 'provider'),
                ('source', 'source'),
                ('latest_timestamp', 'latest_timestamp'),
                ('interval', 'interval'),
                ('symbol', 'symbol'),
                ('close', 'close'),
            ],
        ),
        'validation_risk_health': {
            'status': risk_status,
            'color': _status_color(risk_status),
            'cards': [
                {'label': 'trade_validation_failures_total', 'value': _metric(snapshot, 'trade_validation_failures_total', 0)},
                {'label': 'rolling_win_rate', 'value': win_rate},
                {'label': 'rolling_expectancy', 'value': expectancy},
                {'label': 'portfolio_kill_switch_active', 'value': kill_switch_active},
                {'label': 'portfolio_daily_loss_limit', 'value': daily_loss_limit},
                {'label': 'portfolio_per_trade_risk_pct', 'value': per_trade_risk_pct},
                {'label': 'portfolio_max_trades_per_day', 'value': max_trades_per_day},
            ],
        },
        'latest_signal': _build_detail_cards(
            latest_signal,
            [
                ('symbol', 'symbol'),
                ('side', 'side'),
                ('option_strike', 'option_strike'),
                ('strike_price', 'strike_price'),
                ('option_type', 'option_type'),
                ('entry_price', 'entry_price'),
                ('stop_loss', 'stop_loss'),
                ('target_price', 'target_price'),
            ],
        ),
        'latest_execution': _build_detail_cards(
            latest_execution,
            [
                ('trade_id', 'trade_id'),
                ('side', 'side'),
                ('execution_status', 'execution_status'),
                ('broker_name', 'broker_name'),
                ('option_strike', 'option_strike'),
                ('strike_price', 'strike_price'),
                ('price', 'price'),
                ('reason', 'reason'),
            ],
        ),
        'alerts_and_recent_failures': {
            'alerts': active_alerts[:12],
            'recent_failures': recent_failures,
            'notification_count': alert_notifications,
        },
        'stages': stages,
        'metrics': metrics,
    }


__all__ = ['build_observability_dashboard_payload']





