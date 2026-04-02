from __future__ import annotations

import html
import json
from typing import Any


ROLE_PAGE_CSS = """
<style>
  :root {
    --bg: #07111d;
    --panel: #102338;
    --panel-2: #0c1b2d;
    --text: #eef4ff;
    --muted: #8ea6c7;
    --line: #28435f;
    --accent: #ff9f3f;
    --accent-2: #5da9ff;
    --good: #2ecc71;
    --warn: #f59e0b;
    --bad: #ff6b6b;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    font-family: Segoe UI, Arial, sans-serif;
    color: var(--text);
    background:
      radial-gradient(circle at top left, rgba(255, 159, 63, 0.16), transparent 28%),
      radial-gradient(circle at top right, rgba(93, 169, 255, 0.12), transparent 30%),
      linear-gradient(180deg, #0d2035 0%, var(--bg) 62%);
  }
  .wrap { max-width: 1380px; margin: 0 auto; padding: 28px 22px 44px; }
  .nav, .subnav { display:flex; gap:12px; flex-wrap:wrap; align-items:center; }
  .nav { justify-content:space-between; margin-bottom: 18px; }
  .brand { font-size: 28px; font-weight: 800; }
  .muted { color: var(--muted); }
  .button, .tab {
    display:inline-flex; align-items:center; justify-content:center; min-height:42px; padding:10px 14px;
    border-radius: 12px; border:1px solid var(--line); text-decoration:none; color:var(--text);
    background: rgba(255,255,255,0.03); font-weight: 700;
  }
  .button.primary, .tab.active { background: linear-gradient(135deg, #ffb45f, var(--accent)); color:#111; border-color: transparent; }
  .hero, .grid, .split { display:grid; gap:16px; }
  .hero { grid-template-columns: 1.3fr 0.7fr; margin-bottom: 16px; }
  .grid { grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 16px; }
  .split { grid-template-columns: 1fr 1fr; margin-bottom: 16px; }
  .card {
    background: linear-gradient(180deg, var(--panel), var(--panel-2));
    border: 1px solid var(--line);
    border-radius: 18px;
    padding: 18px;
    box-shadow: 0 18px 42px rgba(0,0,0,0.22);
  }
  h1, h2, h3 { margin: 0 0 10px; }
  .eyebrow {
    display:inline-flex; padding:6px 10px; border-radius:999px; background:rgba(255,255,255,0.04);
    border:1px solid rgba(255,255,255,0.08); color:#ffd18d; font-size:12px; font-weight:800; letter-spacing:.08em; text-transform:uppercase;
  }
  .metric-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
  .metric-value { margin-top: 8px; font-size: 28px; font-weight: 800; }
  .pill { display:inline-flex; padding:5px 10px; border-radius:999px; font-size:12px; font-weight:800; }
  .good { background: rgba(46,204,113,.14); color: #d7ffe7; }
  .warn { background: rgba(245,158,11,.14); color: #ffe0a6; }
  .bad { background: rgba(255,107,107,.14); color: #ffd6d6; }
  table { width:100%; border-collapse:collapse; }
  th, td { padding:10px 8px; border-bottom:1px solid rgba(255,255,255,0.08); text-align:left; vertical-align:top; }
  th { color: var(--muted); font-size:12px; text-transform: uppercase; }
  pre {
    margin: 0; padding: 12px; white-space: pre-wrap; font-size: 12px; line-height: 1.5;
    border-radius: 12px; border:1px solid var(--line); background: rgba(255,255,255,0.03); overflow:auto; max-height: 320px;
  }
  ul { margin: 0; padding-left: 18px; }
  li { margin-bottom: 8px; }
  @media (max-width: 980px) { .hero, .grid, .split { grid-template-columns: 1fr; } .nav { flex-direction: column; align-items: start; } }
</style>
"""


def _tab(href: str, label: str, active: bool) -> str:
    cls = 'tab active' if active else 'tab'
    return f'<a class="{cls}" href="{href}">{html.escape(label)}</a>'


def _metric(label: str, value: object, tone: str = 'good') -> str:
    return (
        '<div class="card">'
        f'<div class="metric-label">{html.escape(label)}</div>'
        f'<div class="metric-value">{html.escape(str(value))}</div>'
        f'<div class="pill {tone}">{html.escape(tone.upper())}</div>'
        '</div>'
    )


def render_role_page(*, title: str, role: str, active_path: str, body: str, top_actions: str = '') -> str:
    if role == 'Admin':
        tabs = ''.join([
            _tab('/admin/dashboard', 'Dashboard', active_path == 'dashboard'),
            _tab('/admin/validation', 'Validation', active_path == 'validation'),
            _tab('/admin/execution', 'Execution', active_path == 'execution'),
            _tab('/admin/logs', 'Logs', active_path == 'logs'),
            _tab('/admin/settings', 'Settings', active_path == 'settings'),
        ])
    else:
        tabs = ''.join([
            _tab('/app', 'Home', active_path == 'home'),
            _tab('/app/live-signal', 'Live Signal', active_path == 'live-signal'),
            _tab('/app/trade-history', 'Trade History', active_path == 'trade-history'),
        ])
    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{html.escape(title)}</title>
  {ROLE_PAGE_CSS}
</head>
<body>
  <div class=\"wrap\">
    <div class=\"nav\">
      <div>
        <div class=\"brand\">Vinayak</div>
        <div class=\"muted\">Role-based operations surface</div>
      </div>
      <div class=\"subnav\">{top_actions}</div>
    </div>
    <div class=\"subnav\" style=\"margin-bottom:18px;\">{tabs}</div>
    {body}
  </div>
</body>
</html>"""


def render_user_home_page(payload: dict[str, Any]) -> str:
    signal = dict(payload.get('latest_signal', {}) or {})
    body = f"""
    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">User View</div>
        <h1>Final trading output only.</h1>
        <p class=\"muted\">This page hides logs, validation internals, and execution controls. It only shows the current tradable outcome.</p>
      </section>
      <aside class=\"card\">
        <div class=\"metric-label\">Latest Message</div>
        <div style=\"margin-top:10px; line-height:1.6;\">{html.escape(str(signal.get('message', '-')))}</div>
      </aside>
    </div>
    <div class=\"grid\">
      {_metric('Symbol', signal.get('symbol', '-'))}
      {_metric('Status', signal.get('status', 'NO TRADE'), 'good' if str(signal.get('status', '')).upper() in {'BUY', 'SELL'} else 'warn')}
      {_metric('Confidence', signal.get('confidence', 0.0), 'good')}
      {_metric('Last Updated', signal.get('last_updated', '-'), 'good')}
    </div>
    <div class=\"split\">
      <section class=\"card\"><h2>Trade Plan</h2><table><tbody>
        <tr><th>Entry Price</th><td>{html.escape(str(signal.get('entry_price', 0.0)))}</td></tr>
        <tr><th>Stop Loss</th><td>{html.escape(str(signal.get('stop_loss', 0.0)))}</td></tr>
        <tr><th>Target Price</th><td>{html.escape(str(signal.get('target_price', 0.0)))}</td></tr>
        <tr><th>RR Ratio</th><td>{html.escape(str(signal.get('rr_ratio', 0.0)))}</td></tr>
      </tbody></table></section>
      <section class=\"card\"><h2>Live Summary</h2><table><tbody>
        <tr><th>Trade History Rows</th><td>{html.escape(str(payload.get('history_count', 0)))}</td></tr>
        <tr><th>Last Trade Time</th><td>{html.escape(str(payload.get('last_trade_time', '-')))}</td></tr>
      </tbody></table></section>
    </div>
    """
    return render_role_page(title='Vinayak User Home', role='User', active_path='home', body=body, top_actions='<a class="button" href="/admin">Admin Login</a>')


def render_user_signal_page(signal: dict[str, Any]) -> str:
    body = f"""
    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Live Signal</div>
        <h1>{html.escape(str(signal.get('status', 'NO TRADE')))}</h1>
        <p class=\"muted\">Only the required user output fields are shown on this page.</p>
      </section>
      <aside class=\"card\"><div class=\"metric-label\">Message</div><div style=\"margin-top:10px; line-height:1.6;\">{html.escape(str(signal.get('message', '-')))}</div></aside>
    </div>
    <section class=\"card\"><h2>Required Output</h2><table><tbody>
      <tr><th>symbol</th><td>{html.escape(str(signal.get('symbol', '-')))}</td></tr>
      <tr><th>status</th><td>{html.escape(str(signal.get('status', 'NO TRADE')))}</td></tr>
      <tr><th>entry_price</th><td>{html.escape(str(signal.get('entry_price', 0.0)))}</td></tr>
      <tr><th>stop_loss</th><td>{html.escape(str(signal.get('stop_loss', 0.0)))}</td></tr>
      <tr><th>target_price</th><td>{html.escape(str(signal.get('target_price', 0.0)))}</td></tr>
      <tr><th>rr_ratio</th><td>{html.escape(str(signal.get('rr_ratio', 0.0)))}</td></tr>
      <tr><th>confidence</th><td>{html.escape(str(signal.get('confidence', 0.0)))}</td></tr>
      <tr><th>last_updated</th><td>{html.escape(str(signal.get('last_updated', '-')))}</td></tr>
      <tr><th>message</th><td>{html.escape(str(signal.get('message', '-')))}</td></tr>
    </tbody></table></section>
    """
    return render_role_page(title='Vinayak Live Signal', role='User', active_path='live-signal', body=body, top_actions='<a class="button" href="/admin">Admin Login</a>')


def render_trade_history_page(history_payload: dict[str, Any]) -> str:
    rows = list(history_payload.get('history', []) or [])
    if rows:
        row_html = ''.join(
            f"<tr><td>{html.escape(str(row.get('symbol', '-')))}</td><td>{html.escape(str(row.get('side', '-')))}</td><td>{html.escape(str(row.get('entry_price', row.get('price', '-'))))}</td><td>{html.escape(str(row.get('stop_loss', '-')))}</td><td>{html.escape(str(row.get('target', row.get('target_price', '-'))))}</td><td>{html.escape(str(row.get('execution_status', row.get('status', '-'))))}</td><td>{html.escape(str(row.get('executed_at_utc', row.get('signal_time', '-'))))}</td></tr>"
            for row in rows[:25]
        )
    else:
        row_html = '<tr><td colspan="7">No paper trade history yet.</td></tr>'
    body = f"""
    <section class=\"card\">
      <div class=\"eyebrow\">Trade History</div>
      <h1>Recent paper trading outcomes</h1>
      <p class=\"muted\">This view remains user-safe: no validation internals, debug state, or control buttons.</p>
    </section>
    <section class=\"card\" style=\"margin-top:16px;\"><h2>Recent Trades</h2>
      <table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Stop</th><th>Target</th><th>Status</th><th>Time</th></tr></thead><tbody>{row_html}</tbody></table>
    </section>
    """
    return render_role_page(title='Vinayak Trade History', role='User', active_path='trade-history', body=body, top_actions='<a class="button" href="/admin">Admin Login</a>')


def render_admin_dashboard_page(payload: dict[str, Any]) -> str:
    summary = dict(payload.get('summary', {}) or {})
    signal = dict(payload.get('latest_signal', {}) or {})
    debug = dict(payload.get('admin_debug', {}) or {})
    body = f"""
    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Admin View</div>
        <h1>Admin Dashboard</h1>
        <p class=\"muted\">Full control surface for reviewed trades, validation quality, execution visibility, and operator workflows.</p>
      </section>
      <aside class=\"card\">
        <div class=\"metric-label\">Broker</div>
        <div class=\"metric-value\">{html.escape(str(summary.get('broker_name', 'DHAN')))}</div>
        <div class=\"pill {'good' if summary.get('broker_ready') else 'bad'}\">{'READY' if summary.get('broker_ready') else 'MISSING CREDENTIALS'}</div>
      </aside>
    </div>
    <div class=\"grid\">
      {_metric('Reviewed Trades', sum(dict(summary.get('reviewed_trade_counts', {}) or {}).values()), 'good')}
      {_metric('Executions', sum(dict(summary.get('execution_status_counts', {}) or {}).values()), 'good')}
      {_metric('Audit Failures', summary.get('recent_audit_failures', 0), 'bad' if int(summary.get('recent_audit_failures', 0) or 0) else 'good')}
      {_metric('Signal Status', signal.get('status', 'NO TRADE'), 'good' if str(signal.get('status', '')).upper() in {'BUY', 'SELL'} else 'warn')}
    </div>
    <div class=\"split\">
      <section class=\"card\"><h2>Validation Snapshot</h2><pre>{html.escape(json.dumps(debug.get('validation_checks', {}), indent=2))}</pre></section>
      <section class=\"card\"><h2>Latest Signal</h2><pre>{html.escape(json.dumps(signal, indent=2))}</pre></section>
    </div>
    """
    actions = '<a class="button" href="/workspace">Workspace</a><a class="button" href="/workspace/observability">Observability</a><form method="post" action="/admin/logout" style="display:inline;"><button class="button primary" type="submit">Logout</button></form>'
    return render_role_page(title='Vinayak Admin Dashboard', role='Admin', active_path='dashboard', body=body, top_actions=actions)


def render_admin_validation_page(payload: dict[str, Any]) -> str:
    debug = dict(payload.get('admin_debug', {}) or {})
    validation_summary = dict(payload.get('validation_summary', {}) or {})
    empty_state = payload.get('empty_state') or {}
    rejection_items = ''.join(f'<li><strong>{html.escape(str(key))}</strong>: {html.escape(str(value))}</li>' for key, value in dict(debug.get('rejection_reasons', {}) or {}).items()) or '<li>No rejection reasons recorded.</li>'
    latest_errors = ''.join(f'<li>{html.escape(str(item))}</li>' for item in list(debug.get('latest_errors', []) or [])) or '<li>No latest errors.</li>'
    empty_html = ''
    if empty_state:
        empty_html = f"""
        <section class=\"card\" style=\"margin-top:16px;\">
          <h2>{html.escape(str(empty_state.get('title', 'No analysis data yet')))}</h2>
          <p class=\"muted\">{html.escape(str(empty_state.get('message', 'Run a fresh analysis from the workspace.')))}</p>
          <table><tbody>
            <tr><th>Last Analysis Time</th><td>{html.escape(str(empty_state.get('last_analysis_time', '-')))}</td></tr>
            <tr><th>Last Signal Count</th><td>{html.escape(str(empty_state.get('last_signal_count', 0)))}</td></tr>
            <tr><th>Last Signal Time</th><td>{html.escape(str(empty_state.get('last_signal_time', '-')))}</td></tr>
            <tr><th>Why Not Ready</th><td>{html.escape(str(empty_state.get('why_not_ready', '-')))}</td></tr>
          </tbody></table>
        </section>
        """
    body = f"""
    <section class=\"card\"><div class=\"eyebrow\">Validation</div><h1>Validation Health</h1><p class=\"muted\">Admin-only debug fields and trade validation quality gates.</p></section>
    {empty_html}
    <div class=\"grid\" style=\"margin-top:16px;\">
      {_metric('zones_detected', debug.get('zones_detected', 0), 'good' if int(debug.get('zones_detected', 0) or 0) else 'warn')}
      {_metric('accepted_zones', debug.get('accepted_zones', 0), 'good' if int(debug.get('accepted_zones', 0) or 0) else 'warn')}
      {_metric('rejected_zones', debug.get('rejected_zones', 0), 'warn' if int(debug.get('rejected_zones', 0) or 0) else 'good')}
      {_metric('system_status', validation_summary.get('system_status', 'NOT_READY'), 'warn')}
    </div>
    <div class=\"split\">
      <section class=\"card\"><h2>Rejection Reasons</h2><ul>{rejection_items}</ul></section>
      <section class=\"card\"><h2>Latest Errors</h2><ul>{latest_errors}</ul></section>
    </div>
    <section class=\"card\"><h2>Validation Checks</h2><pre>{html.escape(json.dumps(validation_summary, indent=2))}</pre></section>
    """
    actions = '<a class=\"button\" href=\"/workspace\">Workspace</a><form method=\"post\" action=\"/admin/logout\" style=\"display:inline;\"><button class=\"button primary\" type=\"submit\">Logout</button></form>'
    return render_role_page(title='Vinayak Admin Validation', role='Admin', active_path='validation', body=body, top_actions=actions)


def render_admin_execution_page(payload: dict[str, Any]) -> str:
    summary = dict(payload.get('paper_summary', {}) or {})
    signal = dict(payload.get('latest_signal', {}) or {})
    rows = list(payload.get('history', []) or [])
    row_html = ''.join(
        f"<tr><td>{html.escape(str(row.get('symbol', '-')))}</td><td>{html.escape(str(row.get('side', '-')))}</td><td>{html.escape(str(row.get('entry_price', row.get('price', '-'))))}</td><td>{html.escape(str(row.get('execution_status', row.get('status', '-'))))}</td><td>{html.escape(str(row.get('pnl', '-')))}</td><td>{html.escape(str(row.get('executed_at_utc', row.get('signal_time', '-'))))}</td></tr>"
        for row in rows[:25]
    ) or '<tr><td colspan="6">No paper executions yet.</td></tr>'
    body = f"""
    <section class=\"card\"><div class=\"eyebrow\">Execution</div><h1>Execution Health</h1><p class=\"muted\">Admin-only execution tracking and paper trade verification.</p></section>
    <div class=\"grid\" style=\"margin-top:16px;\">
      {_metric('Mode', summary.get('mode', 'PAPER'), 'good')}
      {_metric('Executed Count', summary.get('executed_count', 0), 'good')}
      {_metric('Blocked Count', summary.get('blocked_count', 0), 'warn' if int(summary.get('blocked_count', 0) or 0) else 'good')}
      {_metric('Duplicate Count', summary.get('duplicate_count', 0), 'warn' if int(summary.get('duplicate_count', 0) or 0) else 'good')}
    </div>
    <div class=\"split\">
      <section class=\"card\"><h2>Execution Summary</h2><pre>{html.escape(json.dumps(summary, indent=2))}</pre></section>
      <section class=\"card\"><h2>Latest User Signal</h2><pre>{html.escape(json.dumps(signal, indent=2))}</pre></section>
    </div>
    <section class=\"card\"><h2>Recent Paper Trades</h2><table><thead><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Status</th><th>PnL</th><th>Time</th></tr></thead><tbody>{row_html}</tbody></table></section>
    """
    actions = '<a class="button" href="/workspace">Workspace</a><form method="post" action="/admin/logout" style="display:inline;"><button class="button primary" type="submit">Logout</button></form>'
    return render_role_page(title='Vinayak Admin Execution', role='Admin', active_path='execution', body=body, top_actions=actions)


def render_admin_logs_page(payload: dict[str, Any]) -> str:
    logs = dict(payload.get('logs', {}) or {})
    body = f"""
    <section class=\"card\"><div class=\"eyebrow\">Logs</div><h1>Runtime Logs</h1><p class=\"muted\">Admin-only access to app, execution, rejection, and error logs.</p></section>
    <div class=\"split\" style=\"margin-top:16px;\">
      <section class=\"card\"><h2>App Log</h2><pre>{html.escape(logs.get('app_log', 'No log entries yet.'))}</pre></section>
      <section class=\"card\"><h2>Execution Log</h2><pre>{html.escape(logs.get('execution_log', 'No log entries yet.'))}</pre></section>
      <section class=\"card\"><h2>Rejections Log</h2><pre>{html.escape(logs.get('rejections_log', 'No log entries yet.'))}</pre></section>
      <section class=\"card\"><h2>Errors Log</h2><pre>{html.escape(logs.get('errors_log', 'No log entries yet.'))}</pre></section>
    </div>
    """
    actions = '<a class="button" href="/workspace">Workspace</a><form method="post" action="/admin/logout" style="display:inline;"><button class="button primary" type="submit">Logout</button></form>'
    return render_role_page(title='Vinayak Admin Logs', role='Admin', active_path='logs', body=body, top_actions=actions)


def render_admin_settings_page(payload: dict[str, Any]) -> str:
    settings = dict(payload.get('settings', {}) or {})
    users = list(settings.get('users', []) or [])
    flash_message = str(payload.get('flash_message', '') or '')
    flash_tone = str(payload.get('flash_tone', 'good') or 'good')
    user_rows = ''.join(
        f"<tr><td>{html.escape(str(row.get('username', '-')))}</td><td>{html.escape(str(row.get('role', '-')))}</td><td>{html.escape('ACTIVE' if row.get('is_active') else 'DISABLED')}</td><td>{html.escape(str(row.get('created_at', '-')))}</td></tr>"
        for row in users
    ) or '<tr><td colspan="4">No users found.</td></tr>'
    flash_html = f'<div class="pill {flash_tone}" style="margin-bottom:12px;">{html.escape(flash_message)}</div>' if flash_message else ''
    body = f"""
    <section class="card"><div class="eyebrow">Settings</div><h1>Role-Based Access Model</h1><p class="muted">Shared configuration, user administration, and separation-of-concerns view for the web app.</p></section>
    <div class="split" style="margin-top:16px;">
      <section class="card"><h2>Runtime Paths</h2><table><tbody>
        <tr><th>Paper Log</th><td>{html.escape(str(settings.get('paper_log_path', '-')))}</td></tr>
        <tr><th>Reports Dir</th><td>{html.escape(str(settings.get('reports_dir', '-')))}</td></tr>
        <tr><th>Cache Configured</th><td>{html.escape(str(settings.get('cache_configured', False)))}</td></tr>
      </tbody></table></section>
      <section class="card"><h2>Role Model</h2><pre>{html.escape(json.dumps(settings.get('role_model', {}), indent=2))}</pre></section>
    </div>
    <div class="split" style="margin-top:16px;">
      <section class="card">
        <h2>Create User</h2>
        {flash_html}
        <form method="post" action="/admin/users/create">
          <label class="metric-label" for="username">Username</label>
          <input id="username" name="username" type="text" required style="width:100%; padding:10px 12px; margin:8px 0 14px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.03); color:var(--text);" />
          <label class="metric-label" for="password">Password</label>
          <input id="password" name="password" type="password" required style="width:100%; padding:10px 12px; margin:8px 0 14px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.03); color:var(--text);" />
          <label class="metric-label" for="role">Role</label>
          <select id="role" name="role" style="width:100%; padding:10px 12px; margin:8px 0 14px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.03); color:var(--text);">
            <option value="USER">USER</option>
            <option value="ADMIN">ADMIN</option>
          </select>
          <button class="button primary" type="submit">Create User</button>
        </form>
      </section>
      <section class="card"><h2>Current Users</h2><table><thead><tr><th>Username</th><th>Role</th><th>Status</th><th>Created</th></tr></thead><tbody>{user_rows}</tbody></table></section>
    </div>
    <section class="card"><h2>UI Separation</h2><ul>
      <li>Admin pages expose control, validation, execution, logs, settings, and user creation.</li>
      <li>User pages expose only final signal output and trade history after login.</li>
      <li>Business logic stays behind services and API layers, not inside templates.</li>
    </ul></section>
    """
    actions = '<a class="button" href="/workspace">Workspace</a><form method="post" action="/logout" style="display:inline;"><button class="button primary" type="submit">Logout</button></form>'
    return render_role_page(title='Vinayak Admin Settings', role='Admin', active_path='settings', body=body, top_actions=actions)


__all__ = [
    'render_admin_dashboard_page',
    'render_admin_execution_page',
    'render_admin_logs_page',
    'render_admin_settings_page',
    'render_admin_validation_page',
    'render_role_page',
    'render_trade_history_page',
    'render_user_home_page',
    'render_user_signal_page',
]



