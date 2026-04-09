from __future__ import annotations

import json


def build_text_wireframe() -> str:
    return """
Top Row: app_status | market_data_status | latest_data_timestamp | total_signals_today | valid_signals_today | executed_paper_trades_today | rejected_trades_today | telegram_status | pnl_today | win_rate_rolling | expectancy_rolling

Left Column:
- System Health card
- Data Health card
- Strategy Health card

Center Column:
- Execution Health card
- Validation and Risk Health card
- Trace Stages table

Right Column:
- Alerts card
- Recent Failures table
- Metric snapshot table
""".strip()


def build_grafana_dashboard_spec() -> dict:
    return {
        'title': 'Vinayak Paper Trading Observability',
        'timezone': 'browser',
        'schemaVersion': 39,
        'version': 1,
        'refresh': '30s',
        'tags': ['vinayak', 'paper-trading', 'observability'],
        'templating': {'list': []},
        'panels': [
            {'title': 'App Status', 'type': 'stat', 'gridPos': {'x': 0, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'trading_app_up'}]},
            {'title': 'Market Data Delay', 'type': 'stat', 'gridPos': {'x': 3, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'market_data_delay_seconds'}]},
            {'title': 'Signals Today', 'type': 'stat', 'gridPos': {'x': 6, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'total_signals_today'}]},
            {'title': 'Paper Trades Executed', 'type': 'stat', 'gridPos': {'x': 9, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'paper_trades_executed_total'}]},
            {'title': 'PnL Today', 'type': 'stat', 'gridPos': {'x': 12, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'pnl_today'}]},
            {'title': 'Rolling Win Rate', 'type': 'stat', 'gridPos': {'x': 15, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'rolling_win_rate'}]},
            {'title': 'Rolling Expectancy', 'type': 'stat', 'gridPos': {'x': 18, 'y': 0, 'w': 3, 'h': 4}, 'targets': [{'expr': 'rolling_expectancy'}]},
            {'title': 'Pipeline Stages', 'type': 'table', 'gridPos': {'x': 0, 'y': 4, 'w': 10, 'h': 8}, 'targets': [{'expr': 'stage_visibility'}]},
            {'title': 'Recent Failures', 'type': 'logs', 'gridPos': {'x': 10, 'y': 4, 'w': 14, 'h': 8}, 'targets': [{'expr': '{component=~".*"}'}]},
            {'title': 'Execution and Validation', 'type': 'timeseries', 'gridPos': {'x': 0, 'y': 12, 'w': 12, 'h': 8}, 'targets': [
                {'expr': 'paper_trades_executed_total'},
                {'expr': 'paper_trade_rejections_total'},
                {'expr': 'trade_validation_failures_total'},
            ]},
            {'title': 'Data Health', 'type': 'timeseries', 'gridPos': {'x': 12, 'y': 12, 'w': 12, 'h': 8}, 'targets': [
                {'expr': 'market_data_rows_loaded_total'},
                {'expr': 'market_data_duplicates_total'},
                {'expr': 'market_data_nulls_total'},
                {'expr': 'schema_validation_failures_total'},
            ]},
        ],
    }


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Vinayak Observability</title>
  <style>
    :root { --bg:#07111d; --panel:#102338; --line:#28435f; --text:#eef4ff; --muted:#8ea6c7; --good:#22c55e; --warn:#f59e0b; --bad:#ef4444; }
    * { box-sizing:border-box; }
    body { margin:0; font-family:Segoe UI, Arial, sans-serif; color:var(--text); background:linear-gradient(180deg,#0d2035 0%, #07111d 65%); }
    .shell { max-width:1500px; margin:0 auto; padding:22px; }
    .hero, .grid, .section-grid { display:grid; gap:14px; }
    .hero { grid-template-columns: 1.2fr .8fr; margin-bottom:14px; }
    .grid { grid-template-columns: repeat(6, minmax(0, 1fr)); margin-bottom:14px; }
    .section-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    .card { background:linear-gradient(180deg,#102338,#0b1d30); border:1px solid var(--line); border-radius:18px; padding:16px; }
    h1,h2 { margin:0 0 10px; }
    .muted { color:var(--muted); }
    .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .value { font-size:26px; font-weight:800; margin-top:8px; }
    .pill { display:inline-flex; padding:5px 10px; border-radius:999px; font-size:12px; font-weight:800; }
    .green { background:rgba(34,197,94,.15); color:#bbf7d0; }
    .yellow { background:rgba(245,158,11,.15); color:#fde68a; }
    .red { background:rgba(239,68,68,.15); color:#fecaca; }
    table { width:100%; border-collapse:collapse; }
    th, td { padding:9px 8px; border-bottom:1px solid rgba(255,255,255,.08); font-size:13px; text-align:left; vertical-align:top; }
    th { color:var(--muted); text-transform:uppercase; font-size:11px; }
    pre { margin:0; white-space:pre-wrap; font-size:12px; color:var(--text); background:rgba(255,255,255,.03); border:1px solid var(--line); border-radius:12px; padding:12px; overflow:auto; max-height:220px; }
    @media (max-width: 1100px) { .hero, .grid, .section-grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"hero\">
      <section class=\"card\">
        <h1>Trading Observability</h1>
        <div class=\"muted\">One-screen paper-trading visibility for app health, data freshness, signal quality, execution reliability, and active failures.</div>
      </section>
      <section class=\"card\">
        <div class=\"label\">Grafana Spec</div>
        <pre id=\"grafanaSpec\">__GRAFANA_JSON__</pre>
      </section>
    </div>
    <div id=\"kpis\" class=\"grid\"></div>
    <div class=\"section-grid\">
      <section class=\"card\"><h2>System Health</h2><div id=\"systemHealth\"></div></section>
      <section class=\"card\"><h2>Data Health</h2><div id=\"dataHealth\"></div></section>
      <section class=\"card\"><h2>Strategy Health</h2><div id=\"strategyHealth\"></div></section>
      <section class=\"card\"><h2>Latest Market Data</h2><div id=\"latestMarketData\"></div></section>
      <section class=\"card\"><h2>Execution Health</h2><div id=\"executionHealth\"></div></section>
      <section class=\"card\"><h2>Validation and Risk Health</h2><div id=\"validationRiskHealth\"></div></section>
      <section class=\"card\"><h2>Alerts and Recent Failures</h2><div id=\"alerts\"></div></section>
    </div>
    <div class=\"section-grid\" style=\"margin-top:14px;\">
      <section class=\"card\" style=\"grid-column: span 2;\"><h2>Trace-Like Stage Visibility</h2><div id=\"stages\"></div></section>
      <section class=\"card\"><h2>Recent Failures</h2><div id=\"recentFailures\"></div></section>
    </div>
  </div>
  <script>
    function renderCards(nodeId, items) {
      const node = document.getElementById(nodeId);
      node.innerHTML = (items || []).map((item) => `<div style=\"padding:10px 0; border-bottom:1px solid rgba(255,255,255,.08);\"><div class=\"label\">${item.label}</div><div class=\"value\" style=\"font-size:18px;\">${item.value}</div></div>`).join('') || '<div class=\"muted\">No data.</div>';
    }
    function renderKpis(items) {
      const node = document.getElementById('kpis');
      node.innerHTML = (items || []).map((item) => `<div class=\"card\"><div class=\"label\">${item.name}</div><div class=\"value\">${item.value}</div><div class=\"pill ${item.color}\">${String(item.color || '').toUpperCase()}</div></div>`).join('');
    }
    function renderAlerts(alerts, failures) {
      const node = document.getElementById('alerts');
      const alertHtml = (alerts || []).map((item) => `<div style=\"margin-bottom:8px;\"><span class=\"pill ${item.severity}\">${String(item.severity || '').toUpperCase()}</span> ${item.name} <span class=\"muted\">${item.value}</span></div>`).join('');
      node.innerHTML = alertHtml || '<div class=\"muted\">No active alerts.</div>';
      const failureNode = document.getElementById('recentFailures');
      failureNode.innerHTML = `<table><thead><tr><th>Time</th><th>Component</th><th>Event</th><th>Message</th></tr></thead><tbody>${(failures || []).map((row) => `<tr><td>${row.timestamp || '-'}</td><td>${row.component || '-'}</td><td>${row.event_name || '-'}</td><td>${row.message || '-'}</td></tr>`).join('')}</tbody></table>`;
    }
    function renderStages(stages) {
      const rows = Object.entries(stages || {});
      document.getElementById('stages').innerHTML = `<table><thead><tr><th>Stage</th><th>Status</th><th>Duration</th><th>Last Success</th><th>Last Failure</th></tr></thead><tbody>${rows.map(([name, value]) => {
        const status = String(value.status || 'UNKNOWN');
        const color = status.toLowerCase().includes('success') ? 'green' : status.toLowerCase().includes('fail') ? 'red' : 'yellow';
        return `<tr><td>${name}</td><td><span class=\"pill ${color}\">${status}</span></td><td>${value.duration_seconds || 0}</td><td>${value.last_success || '-'}</td><td>${value.last_failure || '-'}</td></tr>`;
      }).join('')}</tbody></table>`;
    }
    fetch('/dashboard/observability').then((r) => r.json()).then((payload) => {
      renderKpis(payload.kpis);
      renderCards('systemHealth', payload.system_health.cards);
      renderCards('dataHealth', payload.data_health.cards);
      renderCards('strategyHealth', payload.strategy_health.cards);
      renderCards('latestMarketData', payload.latest_market_data);
      renderCards('executionHealth', payload.execution_health.cards);
      renderCards('validationRiskHealth', payload.validation_risk_health.cards);
      renderAlerts(payload.alerts_and_recent_failures.alerts, payload.alerts_and_recent_failures.recent_failures);
      renderStages(payload.stages);
    }).catch((error) => {
      document.getElementById('kpis').innerHTML = `<div class=\"card\">Failed to load observability payload: ${error.message}</div>`;
    });
  </script>
</body>
</html>
"""


def build_observability_dashboard_html() -> str:
    grafana = json.dumps(build_grafana_dashboard_spec(), indent=2)
    return _HTML_TEMPLATE.replace('__GRAFANA_JSON__', grafana)


__all__ = ['build_grafana_dashboard_spec', 'build_observability_dashboard_html', 'build_text_wireframe']



