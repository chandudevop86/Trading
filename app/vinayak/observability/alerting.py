from __future__ import annotations

from typing import Any

from vinayak.messaging.events import EVENT_NOTIFICATION_REQUESTED
from vinayak.observability.observability_metrics import get_observability_snapshot, increment_metric, set_metric


AlertRecord = dict[str, Any]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except Exception:
        return default


def _metric(snapshot: dict[str, Any], name: str, default: Any = 0) -> Any:
    return snapshot.get('metrics', {}).get(name, {}).get('value', default)


def build_active_alerts(snapshot: dict[str, Any] | None = None) -> list[AlertRecord]:
    snapshot = snapshot or get_observability_snapshot()

    data_delay = _safe_float(_metric(snapshot, 'market_data_delay_seconds', 0.0))
    schema_failures = int(_safe_float(_metric(snapshot, 'schema_validation_failures_total', 0)))
    cycle_failures = int(_safe_float(_metric(snapshot, 'trading_cycle_failures_total', 0)))
    paper_rejections = int(_safe_float(_metric(snapshot, 'paper_trade_rejections_total', 0)))
    duplicate_trade_blocks = int(_safe_float(_metric(snapshot, 'duplicate_trade_blocks_total', 0)))
    telegram_failures = int(_safe_float(_metric(snapshot, 'telegram_send_failures_total', 0)))
    expectancy = round(_safe_float(_metric(snapshot, 'rolling_expectancy', 0.0)), 2)
    win_rate = round(_safe_float(_metric(snapshot, 'rolling_win_rate', 0.0)), 2)
    latest_ts = str(_metric(snapshot, 'latest_data_timestamp', '') or '')
    csv_failures = int(_safe_float(_metric(snapshot, 'csv_write_failures_total', 0)))

    execution_attempts = int(_safe_float(_metric(snapshot, 'execution_attempt_total', 0)))
    execution_success = int(_safe_float(_metric(snapshot, 'execution_success_total', 0)))
    execution_failed = int(_safe_float(_metric(snapshot, 'execution_failed_total', 0)))
    execution_blocked = int(_safe_float(_metric(snapshot, 'execution_blocked_total', 0)))
    duplicate_execution_blocks = int(_safe_float(_metric(snapshot, 'duplicate_execution_block_total', 0)))
    kill_switch_active = bool(_safe_float(_metric(snapshot, 'portfolio_kill_switch_active', 0)))
    pnl_today = round(_safe_float(_metric(snapshot, 'pnl_today', 0.0)), 2)
    daily_loss_limit = abs(_safe_float(_metric(snapshot, 'portfolio_daily_loss_limit', 0.0)))
    per_trade_risk_pct = round(_safe_float(_metric(snapshot, 'portfolio_per_trade_risk_pct', 0.0)), 4)
    max_trades_per_day = int(_safe_float(_metric(snapshot, 'portfolio_max_trades_per_day', 0)))

    alerts: list[AlertRecord] = []
    if data_delay > 180:
        alerts.append({'name': 'market_data_delay_seconds too high', 'severity': 'red', 'value': data_delay, 'metric': 'market_data_delay_seconds'})
    if schema_failures > 0:
        alerts.append({'name': 'schema_validation_failures_total increased', 'severity': 'red', 'value': schema_failures, 'metric': 'schema_validation_failures_total'})
    if cycle_failures > 0:
        alerts.append({'name': 'trading_cycle_failures_total increased', 'severity': 'red', 'value': cycle_failures, 'metric': 'trading_cycle_failures_total'})
    if paper_rejections > 0:
        alerts.append({'name': 'paper_trade_rejections_total spike', 'severity': 'yellow', 'value': paper_rejections, 'metric': 'paper_trade_rejections_total'})
    if duplicate_trade_blocks > 0:
        alerts.append({'name': 'duplicate_trade_blocks_total sudden increase', 'severity': 'yellow', 'value': duplicate_trade_blocks, 'metric': 'duplicate_trade_blocks_total'})
    if telegram_failures > 0:
        alerts.append({'name': 'telegram_send_failures_total increased', 'severity': 'yellow', 'value': telegram_failures, 'metric': 'telegram_send_failures_total'})
    if expectancy < 0:
        alerts.append({'name': 'rolling_expectancy below threshold', 'severity': 'red', 'value': expectancy, 'metric': 'rolling_expectancy'})
    if win_rate < 45 and int(_safe_float(_metric(snapshot, 'total_signals_today', 0))) > 0:
        alerts.append({'name': 'rolling_win_rate below threshold', 'severity': 'yellow', 'value': win_rate, 'metric': 'rolling_win_rate'})
    if not latest_ts:
        alerts.append({'name': 'latest_data_timestamp stale', 'severity': 'red', 'value': latest_ts, 'metric': 'latest_data_timestamp'})
    if csv_failures > 0:
        alerts.append({'name': 'csv save failure or no recent execution log', 'severity': 'red', 'value': csv_failures, 'metric': 'csv_write_failures_total'})
    if execution_failed > 0:
        alerts.append({'name': 'execution_failed_total increased', 'severity': 'red', 'value': execution_failed, 'metric': 'execution_failed_total'})
    if execution_blocked > 0:
        alerts.append({'name': 'execution_blocked_total increased', 'severity': 'yellow', 'value': execution_blocked, 'metric': 'execution_blocked_total'})
    if duplicate_execution_blocks > 0:
        alerts.append({'name': 'duplicate_execution_block_total increased', 'severity': 'yellow', 'value': duplicate_execution_blocks, 'metric': 'duplicate_execution_block_total'})
    if kill_switch_active:
        alerts.append({'name': 'portfolio_kill_switch_active is enabled', 'severity': 'red', 'value': 1, 'metric': 'portfolio_kill_switch_active'})
    if daily_loss_limit > 0 and pnl_today <= -daily_loss_limit:
        alerts.append({'name': 'pnl_today breached portfolio_daily_loss_limit', 'severity': 'red', 'value': pnl_today, 'metric': 'pnl_today'})
    if execution_attempts > 0 and execution_success == 0 and (execution_failed > 0 or execution_blocked > 0):
        alerts.append({'name': 'execution pipeline has no successful fills', 'severity': 'red', 'value': execution_attempts, 'metric': 'execution_attempt_total'})
    if per_trade_risk_pct > 0:
        alerts.append({'name': 'portfolio_per_trade_risk_pct active', 'severity': 'blue', 'value': per_trade_risk_pct, 'metric': 'portfolio_per_trade_risk_pct'})
    if max_trades_per_day > 0:
        alerts.append({'name': 'portfolio_max_trades_per_day active', 'severity': 'blue', 'value': max_trades_per_day, 'metric': 'portfolio_max_trades_per_day'})

    severity_rank = {'red': 0, 'yellow': 1, 'blue': 2}
    deduped: list[AlertRecord] = []
    seen: set[tuple[str, str]] = set()
    for item in sorted(alerts, key=lambda row: (severity_rank.get(str(row.get('severity', 'yellow')), 9), str(row.get('name', '')))):
        key = (str(item.get('metric', '')), str(item.get('name', '')))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    set_metric('active_alerts_total', len(deduped))
    return deduped


def build_alert_notification_message(alerts: list[AlertRecord]) -> str:
    if not alerts:
        return 'Observability alert: no active alerts.'
    lines = ['Observability alert summary']
    for item in alerts[:8]:
        severity = str(item.get('severity', 'yellow')).upper()
        lines.append(f"{severity}: {item.get('name')} = {item.get('value')}")
    if len(alerts) > 8:
        lines.append(f"Additional alerts: {len(alerts) - 8}")
    return '\n'.join(lines)


def publish_active_alerts(
    *,
    message_bus: Any,
    telegram_token: str = '',
    telegram_chat_id: str = '',
    source: str = 'observability_alerting',
    snapshot: dict[str, Any] | None = None,
) -> int:
    token = str(telegram_token or '').strip()
    chat_id = str(telegram_chat_id or '').strip()
    if not token or not chat_id:
        return 0
    alerts = build_active_alerts(snapshot)
    actionable = [item for item in alerts if str(item.get('severity', '')).lower() in {'red', 'yellow'}]
    if not actionable:
        return 0
    published = bool(message_bus.publish(
        EVENT_NOTIFICATION_REQUESTED,
        {
            'channel': 'telegram',
            'message': build_alert_notification_message(actionable),
            'alerts': actionable,
        },
        source=source,
    ))
    if published:
        increment_metric('observability_alert_notifications_total', 1)
        return len(actionable)
    return 0


__all__ = [
    'build_active_alerts',
    'build_alert_notification_message',
    'publish_active_alerts',
]


