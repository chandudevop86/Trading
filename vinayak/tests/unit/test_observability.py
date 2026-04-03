from __future__ import annotations

import os

from vinayak.observability.alerting import build_active_alerts, publish_active_alerts
from vinayak.observability.observability_dashboard_spec import (
    build_grafana_dashboard_spec,
    build_observability_dashboard_html,
    build_text_wireframe,
)
from vinayak.observability.observability_health import build_observability_dashboard_payload
from vinayak.observability.observability_logger import log_event, tail_events
from vinayak.observability.observability_metrics import (
    get_observability_snapshot,
    increment_metric,
    record_stage,
    reset_observability_state,
    set_metric,
)


class _StubBus:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict, str]] = []

    def publish(self, name: str, payload: dict, *, source: str) -> bool:
        self.messages.append((name, payload, source))
        return True


def test_observability_metrics_and_stage_snapshot(tmp_path) -> None:
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path)
    reset_observability_state()

    increment_metric('paper_trades_executed_total', 2)
    set_metric('trading_app_up', 1)
    record_stage('market_fetch', status='SUCCESS', duration_seconds=1.25, symbol='^NSEI', strategy='Breakout', message='Fetched rows')

    snapshot = get_observability_snapshot()
    assert snapshot['metrics']['paper_trades_executed_total']['value'] == 2.0
    assert snapshot['metrics']['trading_app_up']['value'] == 1
    assert snapshot['stages']['market_fetch']['status'] == 'SUCCESS'
    assert snapshot['stages']['market_fetch']['duration_seconds'] == 1.25


def test_observability_logger_and_health_payload(tmp_path) -> None:
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path)
    reset_observability_state()

    set_metric('trading_app_up', 1)
    set_metric('latest_data_timestamp', '2026-04-02T09:20:00Z')
    set_metric('market_data_delay_seconds', 60)
    set_metric('total_signals_today', 5)
    set_metric('valid_signals_today', 4)
    set_metric('executed_paper_trades_today', 3)
    set_metric('rejected_trades_today', 1)
    set_metric('telegram_status', 'OK')
    set_metric('pnl_today', 1250.5)
    set_metric('rolling_win_rate', 62.5)
    set_metric('rolling_expectancy', 1.15)
    increment_metric('zones_detected_total', 5)
    increment_metric('zones_accepted_total', 4)
    increment_metric('zones_rejected_total', 1)
    set_metric('zone_score_avg', 7.8)
    record_stage('validation', status='SUCCESS', duration_seconds=0.42, symbol='^NSEI', strategy='Breakout', message='Validation passed')
    log_event(component='validation_engine', event_name='trade_validation', symbol='^NSEI', strategy='Breakout', severity='WARNING', message='One trade rejected', context_json={'reason': 'bad_rr'})

    payload = build_observability_dashboard_payload()
    assert payload['system_health']['status'] in {'UP', 'DEGRADED'}
    assert any(kpi['name'] == 'pnl_today' for kpi in payload['kpis'])
    assert payload['strategy_health']['cards'][0]['label'] == 'zones_detected_total'
    assert len(tail_events(5)) >= 1


def test_observability_alerts_cover_execution_and_risk(tmp_path) -> None:
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path)
    reset_observability_state()

    set_metric('trading_app_up', 1)
    set_metric('latest_data_timestamp', '2026-04-02T09:20:00Z')
    set_metric('execution_attempt_total', 3)
    set_metric('execution_success_total', 0)
    set_metric('execution_failed_total', 1)
    set_metric('execution_blocked_total', 2)
    set_metric('duplicate_execution_block_total', 1)
    set_metric('portfolio_kill_switch_active', 1)
    set_metric('portfolio_daily_loss_limit', 500.0)
    set_metric('pnl_today', -650.0)

    alerts = build_active_alerts()
    names = {item['name'] for item in alerts}
    assert 'execution_failed_total increased' in names
    assert 'execution_blocked_total increased' in names
    assert 'duplicate_execution_block_total increased' in names
    assert 'portfolio_kill_switch_active is enabled' in names
    assert 'pnl_today breached portfolio_daily_loss_limit' in names

    payload = build_observability_dashboard_payload()
    assert any(kpi['name'] == 'execution_failed_total' for kpi in payload['kpis'])
    assert payload['validation_risk_health']['status'] == 'FAIL'


def test_observability_alert_publishing_routes_notification_event(tmp_path) -> None:
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path)
    reset_observability_state()
    set_metric('execution_failed_total', 2)

    bus = _StubBus()
    sent = publish_active_alerts(
        message_bus=bus,
        telegram_token='token',
        telegram_chat_id='chat',
    )

    assert sent >= 1
    assert len(bus.messages) == 1
    assert bus.messages[0][0] == 'notification.requested'
    assert 'Observability alert summary' in bus.messages[0][1]['message']



def test_observability_payload_includes_latest_signal_and_execution_details(tmp_path) -> None:
    os.environ['VINAYAK_OBSERVABILITY_DIR'] = str(tmp_path / 'observability')
    os.environ['REPORTS_DIR'] = str(tmp_path / 'reports')
    reset_observability_state()

    reports_dir = tmp_path / 'reports'
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / '20260403_210000_live_analysis_result.json').write_text(
        """
{
  "signals": [
    {
      "symbol": "NIFTY",
      "side": "BUY",
      "option_strike": "24500CE",
      "strike_price": 24500,
      "option_type": "CE",
      "entry_price": 101.5,
      "stop_loss": 99.5,
      "target_price": 105.5
    }
  ],
  "execution_rows": [
    {
      "trade_id": "TRADE-1",
      "side": "BUY",
      "execution_status": "FILLED",
      "broker_name": "PAPER",
      "option_strike": "24500CE",
      "strike_price": 24500,
      "price": 101.5,
      "reason": "-"
    }
  ]
}
""".strip(),
        encoding='utf-8',
    )

    payload = build_observability_dashboard_payload()
    latest_signal = {item['label']: item['value'] for item in payload['latest_signal']}
    latest_execution = {item['label']: item['value'] for item in payload['latest_execution']}

    assert latest_signal['option_strike'] == '24500CE'
    assert latest_signal['strike_price'] == 24500
    assert latest_execution['trade_id'] == 'TRADE-1'
    assert latest_execution['option_strike'] == '24500CE'

def test_dashboard_spec_outputs_are_readable() -> None:
    wireframe = build_text_wireframe()
    grafana = build_grafana_dashboard_spec()
    html = build_observability_dashboard_html()

    assert 'Top Row:' in wireframe
    assert grafana['title'] == 'Vinayak Paper Trading Observability'
    assert any(panel['title'] == 'Recent Failures' for panel in grafana['panels'])
    assert '/dashboard/observability' in html

