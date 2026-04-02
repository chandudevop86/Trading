from __future__ import annotations

import os

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


def test_dashboard_spec_outputs_are_readable() -> None:
    wireframe = build_text_wireframe()
    grafana = build_grafana_dashboard_spec()
    html = build_observability_dashboard_html()

    assert 'Top Row:' in wireframe
    assert grafana['title'] == 'Vinayak Paper Trading Observability'
    assert any(panel['title'] == 'Recent Failures' for panel in grafana['panels'])
    assert '/dashboard/observability' in html

