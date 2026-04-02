from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from vinayak.observability.observability_logger import tail_events
from vinayak.observability.observability_metrics import get_observability_snapshot


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
    return 'red'


def _metric(snapshot: dict[str, Any], name: str, default: Any = 0) -> Any:
    return snapshot.get('metrics', {}).get(name, {}).get('value', default)


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
    rejected_trades = int(_safe_float(_metric(snapshot, 'rejected_trades_today', _metric(snapshot, 'paper_trade_rejections_total', 0))))
    telegram_failures = int(_safe_float(_metric(snapshot, 'telegram_send_failures_total', 0)))
    telegram_last = _metric(snapshot, 'telegram_last_delivery_timestamp', '')
    pnl_today = round(_safe_float(_metric(snapshot, 'pnl_today', 0.0)), 2)
    win_rate = round(_safe_float(_metric(snapshot, 'rolling_win_rate', 0.0)), 2)
    expectancy = round(_safe_float(_metric(snapshot, 'rolling_expectancy', 0.0)), 2)

    app_status = 'UP' if trading_app_up and cycle_failures == 0 else 'DEGRADED' if trading_app_up else 'DOWN'
    market_status = 'FRESH' if latest_dt is not None and data_delay <= 180 else 'STALE'
    signal_status = 'PASS' if total_signals == 0 or valid_signals >= max(1, total_signals // 2) else 'WARN'
    execution_status = 'OK' if rejected_trades == 0 else 'WARN' if executed_trades >= rejected_trades else 'FAIL'
    telegram_status = 'OK' if telegram_last and telegram_failures == 0 else 'WARN' if telegram_failures > 0 else 'IDLE'

    recent_failures = tail_events(12, severities={'WARNING', 'ERROR', 'CRITICAL'})
    stages = snapshot.get('stages', {})

    alerts: list[dict[str, Any]] = []
    if data_delay > 180:
        alerts.append({'name': 'market_data_delay_seconds too high', 'severity': 'red', 'value': data_delay})
    if int(_safe_float(_metric(snapshot, 'schema_validation_failures_total', 0))) > 0:
        alerts.append({'name': 'schema_validation_failures_total increased', 'severity': 'red', 'value': _metric(snapshot, 'schema_validation_failures_total', 0)})
    if cycle_failures > 0:
        alerts.append({'name': 'trading_cycle_failures_total increased', 'severity': 'red', 'value': cycle_failures})
    if int(_safe_float(_metric(snapshot, 'paper_trade_rejections_total', 0))) > 0:
        alerts.append({'name': 'paper_trade_rejections_total spike', 'severity': 'yellow', 'value': _metric(snapshot, 'paper_trade_rejections_total', 0)})
    if int(_safe_float(_metric(snapshot, 'duplicate_trade_blocks_total', 0))) > 0:
        alerts.append({'name': 'duplicate_trade_blocks_total sudden increase', 'severity': 'yellow', 'value': _metric(snapshot, 'duplicate_trade_blocks_total', 0)})
    if telegram_failures > 0:
        alerts.append({'name': 'telegram_send_failures_total increased', 'severity': 'yellow', 'value': telegram_failures})
    if expectancy < 0:
        alerts.append({'name': 'rolling_expectancy below threshold', 'severity': 'red', 'value': expectancy})
    if win_rate < 45 and total_signals > 0:
        alerts.append({'name': 'rolling_win_rate below threshold', 'severity': 'yellow', 'value': win_rate})
    if latest_dt is None:
        alerts.append({'name': 'latest_data_timestamp stale', 'severity': 'red', 'value': latest_ts})
    if int(_safe_float(_metric(snapshot, 'csv_write_failures_total', 0))) > 0:
        alerts.append({'name': 'csv save failure or no recent execution log', 'severity': 'red', 'value': _metric(snapshot, 'csv_write_failures_total', 0)})

    kpis = [
        {'name': 'app_status', 'value': app_status, 'color': _status_color(app_status)},
        {'name': 'market_data_status', 'value': market_status, 'color': _status_color(market_status)},
        {'name': 'latest_data_timestamp', 'value': latest_ts or '-','color': _status_color(market_status)},
        {'name': 'total_signals_today', 'value': total_signals, 'color': 'green' if total_signals else 'yellow'},
        {'name': 'valid_signals_today', 'value': valid_signals, 'color': _status_color(signal_status)},
        {'name': 'executed_paper_trades_today', 'value': executed_trades, 'color': _status_color(execution_status)},
        {'name': 'rejected_trades_today', 'value': rejected_trades, 'color': 'red' if rejected_trades else 'green'},
        {'name': 'telegram_status', 'value': telegram_status, 'color': _status_color(telegram_status)},
        {'name': 'pnl_today', 'value': pnl_today, 'color': 'green' if pnl_today >= 0 else 'red'},
        {'name': 'win_rate_rolling', 'value': win_rate, 'color': 'green' if win_rate >= 55 else 'yellow' if win_rate >= 45 else 'red'},
        {'name': 'expectancy_rolling', 'value': expectancy, 'color': 'green' if expectancy > 0 else 'red'},
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
                {'label': 'paper_trades_executed_total', 'value': _metric(snapshot, 'paper_trades_executed_total', 0)},
                {'label': 'paper_trade_rejections_total', 'value': _metric(snapshot, 'paper_trade_rejections_total', 0)},
                {'label': 'duplicate_trade_blocks_total', 'value': _metric(snapshot, 'duplicate_trade_blocks_total', 0)},
                {'label': 'pnl_today', 'value': pnl_today},
            ],
        },
        'validation_risk_health': {
            'status': 'OK' if int(_safe_float(_metric(snapshot, 'trade_validation_failures_total', 0))) == 0 else 'WARN',
            'color': _status_color('OK' if int(_safe_float(_metric(snapshot, 'trade_validation_failures_total', 0))) == 0 else 'WARN'),
            'cards': [
                {'label': 'trade_validation_failures_total', 'value': _metric(snapshot, 'trade_validation_failures_total', 0)},
                {'label': 'rolling_win_rate', 'value': win_rate},
                {'label': 'rolling_expectancy', 'value': expectancy},
            ],
        },
        'alerts_and_recent_failures': {
            'alerts': alerts[:10],
            'recent_failures': recent_failures,
        },
        'stages': stages,
        'metrics': metrics,
    }


__all__ = ['build_observability_dashboard_payload']

