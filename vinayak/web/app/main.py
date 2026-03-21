from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from vinayak.api.dependencies.admin_auth import COOKIE_NAME, admin_password, admin_username, is_authenticated, session_token


router = APIRouter(tags=['web'])


LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vinayak Admin Login</title>
  <style>
    :root {
      --bg: #081421;
      --panel: #102338;
      --panel-2: #0d1d2f;
      --text: #eef4ff;
      --muted: #8ea6c7;
      --accent: #ff9f3f;
      --line: #28435f;
      --bad: #ff6b6b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: Segoe UI, Arial, sans-serif;
      background: radial-gradient(circle at top, #133052 0%, var(--bg) 55%);
      color: var(--text);
      padding: 24px;
    }
    .panel {
      width: min(460px, 100%);
      background: linear-gradient(180deg, var(--panel), var(--panel-2));
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 28px;
      box-shadow: 0 24px 60px rgba(0,0,0,0.28);
    }
    h1 { margin: 0 0 10px; font-size: 30px; }
    p { margin: 0 0 20px; color: var(--muted); line-height: 1.5; }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
    }
    input {
      width: 100%;
      padding: 12px 14px;
      margin-bottom: 16px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.03);
      color: var(--text);
    }
    button {
      width: 100%;
      border: 1px solid var(--line);
      background: linear-gradient(135deg, #ffb25f, var(--accent));
      color: #111;
      border-radius: 12px;
      padding: 12px 16px;
      font-weight: 700;
      cursor: pointer;
    }
    .error {
      margin-bottom: 16px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid rgba(255,107,107,0.4);
      color: #ffd6d6;
      background: rgba(255,107,107,0.08);
    }
    code {
      color: #ffd18d;
      background: rgba(255,255,255,0.04);
      padding: 2px 6px;
      border-radius: 6px;
    }
  </style>
</head>
<body>
  <div class="panel">
    <h1>Vinayak Admin Login</h1>
    <p>Sign in to access the operations console for reviewed trades, executions, and audit logs.</p>
    __ERROR_BLOCK__
    <form method="post" action="/admin/login">
      <label for="username">Username</label>
      <input id="username" name="username" type="text" autocomplete="username" required />
      <label for="password">Password</label>
      <input id="password" name="password" type="password" autocomplete="current-password" required />
      <button type="submit">Sign In</button>
    </form>
  </div>
</body>
</html>
"""


ADMIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vinayak Admin</title>
  <style>
    :root {
      --bg: #081421;
      --panel: #102338;
      --panel-2: #0d1d2f;
      --text: #eef4ff;
      --muted: #8ea6c7;
      --accent: #ff9f3f;
      --line: #28435f;
      --good: #2ecc71;
      --bad: #ff6b6b;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Segoe UI, Arial, sans-serif; background: radial-gradient(circle at top, #133052 0%, var(--bg) 55%); color: var(--text); }
    .wrap { max-width: 1460px; margin: 0 auto; padding: 24px; }
    .hero { display: flex; justify-content: space-between; align-items: end; gap: 16px; margin-bottom: 22px; }
    .hero h1 { margin: 0; font-size: 34px; }
    .hero p { margin: 8px 0 0; color: var(--muted); font-size: 15px; max-width: 760px; }
    .toolbar, .filterbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .button { border: 1px solid var(--line); background: linear-gradient(135deg, #ffb25f, var(--accent)); color: #111; border-radius: 12px; padding: 11px 16px; font-weight: 700; cursor: pointer; }
    .button.secondary, .button.audit, .button.logout { background: rgba(255,255,255,0.03); color: var(--text); }
    .button.reject { background: linear-gradient(135deg, #ff8a8a, #ff6b6b); color: #111; }
    .button.approve { background: linear-gradient(135deg, #6fe6a1, #2ecc71); color: #111; }
    .button.execute { background: linear-gradient(135deg, #8ec8ff, #5da9ff); color: #111; }
    .button:disabled { opacity: 0.5; cursor: default; }
    .grid { display: grid; gap: 16px; }
    .metrics { grid-template-columns: repeat(5, minmax(0, 1fr)); margin-bottom: 16px; }
    .cols { grid-template-columns: 1.1fr 1.2fr; margin-bottom: 16px; }
    .card { background: linear-gradient(180deg, var(--panel), var(--panel-2)); border: 1px solid var(--line); border-radius: 16px; padding: 18px; box-shadow: 0 18px 42px rgba(0,0,0,0.22); }
    .card h2, .card h3 { margin: 0 0 10px; font-size: 18px; }
    .metric-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }
    .metric-value { margin-top: 10px; font-size: 30px; font-weight: 800; }
    .badge { display: inline-flex; align-items: center; gap: 8px; border-radius: 999px; padding: 6px 12px; font-size: 12px; font-weight: 700; }
    .good { background: rgba(46, 204, 113, 0.12); color: var(--good); }
    .bad { background: rgba(255, 107, 107, 0.12); color: var(--bad); }
    .lists { display: grid; gap: 14px; grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .count-list { display: grid; gap: 10px; }
    .count-row { display: flex; justify-content: space-between; gap: 12px; padding: 10px 12px; border: 1px solid var(--line); border-radius: 12px; background: rgba(255,255,255,0.02); }
    .input, .select { width: 170px; border: 1px solid var(--line); background: rgba(255,255,255,0.03); color: var(--text); border-radius: 10px; padding: 10px 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
    .status-pill { display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: 4px 9px; font-size: 11px; font-weight: 700; white-space: nowrap; }
    .action-bar { display: flex; gap: 8px; flex-wrap: wrap; }
    .muted { color: var(--muted); }
    .empty { color: var(--muted); padding: 12px 0 0; }
    .flash { margin-bottom: 14px; padding: 12px 14px; border-radius: 12px; border: 1px solid var(--line); background: rgba(255,255,255,0.03); color: var(--text); }
    .flash.error { border-color: rgba(255,107,107,0.5); color: #ffd6d6; }
    .flash.success { border-color: rgba(46,204,113,0.5); color: #d8ffe7; }
    @media (max-width: 1100px) { .metrics, .cols, .lists { grid-template-columns: 1fr; } .hero { flex-direction: column; align-items: start; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div id="flash" hidden></div>
    <div class="hero">
      <div>
        <h1>Vinayak Admin Console</h1>
        <p>Operations view for broker readiness, reviewed trade workflow, execution flow, and live-route audit history.</p>
      </div>
      <div class="toolbar">
        <form method="post" action="/admin/logout" style="margin:0;">
          <button class="button logout" type="submit">Logout</button>
        </form>
        <select id="executionModeSelect" class="select">
          <option value="PAPER">PAPER</option>
          <option value="LIVE">LIVE</option>
        </select>
        <input id="executionIdInput" class="input" type="number" min="1" placeholder="Execution ID" />
        <button class="button secondary" id="refreshExecutionAuditBtn" type="button">Load Execution Audit</button>
        <button class="button" id="refreshBtn" type="button">Refresh Data</button>
      </div>
    </div>

    <div class="grid metrics">
      <div class="card"><div class="metric-label">Broker</div><div class="metric-value" id="brokerName">-</div></div>
      <div class="card"><div class="metric-label">Broker Ready</div><div class="metric-value" id="brokerReady">-</div></div>
      <div class="card"><div class="metric-label">Reviewed Trades</div><div class="metric-value" id="reviewedTotal">0</div></div>
      <div class="card"><div class="metric-label">Executions</div><div class="metric-value" id="executionTotal">0</div></div>
      <div class="card"><div class="metric-label">Audit Failures</div><div class="metric-value" id="auditFailures">0</div></div>
    </div>

    <div class="grid cols">
      <div class="card">
        <h2>Workflow Summary</h2>
        <div class="lists">
          <div><h3>Reviewed Trades</h3><div class="count-list" id="reviewedCounts"></div></div>
          <div><h3>Execution Status</h3><div class="count-list" id="executionStatusCounts"></div></div>
          <div><h3>Execution Modes</h3><div class="count-list" id="executionModeCounts"></div></div>
          <div><h3>Audit Status</h3><div class="count-list" id="auditStatusCounts"></div></div>
        </div>
      </div>
      <div class="card">
        <h2>Reviewed Trades</h2>
        <div class="filterbar">
          <select id="reviewedTradeStatusFilter" class="select">
            <option value="ALL">All Statuses</option>
            <option value="REVIEWED">Reviewed</option>
            <option value="APPROVED">Approved</option>
            <option value="REJECTED">Rejected</option>
            <option value="EXECUTED">Executed</option>
          </select>
        </div>
        <table>
          <thead><tr><th>ID</th><th>Strategy</th><th>Symbol</th><th>Side</th><th>Status</th><th>Qty</th><th>Actions</th></tr></thead>
          <tbody id="reviewedTradesTable"></tbody>
        </table>
        <div class="empty" id="reviewedEmpty" hidden>No reviewed trades yet.</div>
      </div>
    </div>

    <div class="grid cols">
      <div class="card">
        <h2>Recent Executions</h2>
        <div class="filterbar">
          <select id="executionModeFilter" class="select">
            <option value="ALL">All Modes</option>
            <option value="PAPER">PAPER</option>
            <option value="LIVE">LIVE</option>
          </select>
          <select id="executionStatusFilter" class="select">
            <option value="ALL">All Statuses</option>
            <option value="FILLED">Filled</option>
            <option value="ACCEPTED">Accepted</option>
            <option value="BLOCKED">Blocked</option>
            <option value="PENDING_LIVE_ROUTE">Pending Live Route</option>
          </select>
        </div>
        <table>
          <thead><tr><th>ID</th><th>Mode</th><th>Broker</th><th>Status</th><th>Reviewed Trade</th><th>Actions</th></tr></thead>
          <tbody id="recentExecutionsTable"></tbody>
        </table>
        <div class="empty" id="recentExecutionsEmpty" hidden>No executions yet.</div>
      </div>
      <div class="card">
        <h2>Execution Audit Logs</h2>
        <div class="filterbar">
          <select id="auditBrokerFilter" class="select">
            <option value="ALL">All Brokers</option>
            <option value="SIM">SIM</option>
            <option value="DHAN">DHAN</option>
          </select>
          <select id="auditStatusFilter" class="select">
            <option value="ALL">All Statuses</option>
            <option value="FILLED">Filled</option>
            <option value="ACCEPTED">Accepted</option>
            <option value="BLOCKED">Blocked</option>
            <option value="REJECTED">Rejected</option>
          </select>
        </div>
        <table>
          <thead><tr><th>Execution</th><th>Broker</th><th>Status</th><th>Request</th></tr></thead>
          <tbody id="auditTable"></tbody>
        </table>
        <div class="empty" id="auditEmpty" hidden>No audit logs yet.</div>
      </div>
    </div>

    <div class="grid cols">
      <div class="card"><h2>Latest Payload Snapshot</h2><pre id="payloadPreview" class="muted" style="white-space: pre-wrap; margin: 0; font-size: 12px; line-height: 1.5;">Waiting for audit data...</pre></div>
      <div class="card"><h2>Admin Notes</h2><p class="muted">Use filters to narrow reviewed trades, executions, and audit logs. Approved trades can be executed in either PAPER or LIVE mode. Recent executions can open their audit trail without typing an ID.</p></div>
    </div>
  </div>

  <script>
    const state = { reviewedTrades: [], executions: [], auditLogs: [] };

    function showFlash(message, tone='success') {
      const node = document.getElementById('flash');
      node.hidden = false;
      node.className = `flash ${tone}`;
      node.textContent = message;
      window.setTimeout(() => { node.hidden = true; }, 3000);
    }

    async function getJson(path) {
      const response = await fetch(path);
      if (!response.ok) {
        let detail = `${path} failed with ${response.status}`;
        try { const payload = await response.json(); detail = payload.detail || detail; } catch (error) {}
        throw new Error(detail);
      }
      return await response.json();
    }

    async function patchJson(path, body) {
      const response = await fetch(path, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (!response.ok) {
        let detail = `${path} failed with ${response.status}`;
        try { const payload = await response.json(); detail = payload.detail || detail; } catch (error) {}
        throw new Error(detail);
      }
      return await response.json();
    }

    async function postJson(path, body) {
      const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (!response.ok) {
        let detail = `${path} failed with ${response.status}`;
        try { const payload = await response.json(); detail = payload.detail || detail; } catch (error) {}
        throw new Error(detail);
      }
      return await response.json();
    }

    function renderCountList(targetId, counts) {
      const node = document.getElementById(targetId);
      const entries = Object.entries(counts || {});
      if (!entries.length) { node.innerHTML = '<div class="empty">No data yet.</div>'; return; }
      node.innerHTML = entries.map(([label, value]) => `<div class="count-row"><span>${label}</span><strong>${value}</strong></div>`).join('');
    }

    async function updateReviewedTradeStatus(id, status) {
      try {
        await patchJson(`/reviewed-trades/${id}`, { status, notes: `Status changed from admin console to ${status}.` });
        showFlash(`Reviewed trade ${id} updated to ${status}.`);
        await loadDashboard();
      } catch (error) { showFlash(error.message, 'error'); }
    }

    async function executeReviewedTrade(id) {
      const mode = document.getElementById('executionModeSelect').value;
      const broker = mode === 'LIVE' ? 'DHAN' : 'SIM';
      try {
        const execution = await postJson('/executions', { reviewed_trade_id: id, mode, broker });
        document.getElementById('executionIdInput').value = execution.id;
        showFlash(`Execution ${execution.id} created for reviewed trade ${id}.`);
        await loadDashboard();
        await loadExecutionAuditById();
      } catch (error) { showFlash(error.message, 'error'); }
    }

    async function openExecutionAudit(id) {
      document.getElementById('executionIdInput').value = id;
      await loadExecutionAuditById();
    }

    function renderReviewedTrades() {
      const filter = document.getElementById('reviewedTradeStatusFilter').value;
      const rows = state.reviewedTrades.filter((row) => filter === 'ALL' || row.status === filter);
      const body = document.getElementById('reviewedTradesTable');
      const empty = document.getElementById('reviewedEmpty');
      if (!rows.length) { body.innerHTML = ''; empty.hidden = false; return; }
      empty.hidden = true;
      body.innerHTML = rows.map((row) => {
        const approveDisabled = row.status === 'APPROVED' || row.status === 'EXECUTED';
        const rejectDisabled = row.status === 'REJECTED' || row.status === 'EXECUTED';
        const executeDisabled = row.status !== 'APPROVED';
        return `<tr><td>${row.id}</td><td>${row.strategy_name}</td><td>${row.symbol}</td><td>${row.side}</td><td><span class="status-pill">${row.status}</span></td><td>${row.quantity}</td><td><div class="action-bar"><button class="button approve" ${approveDisabled ? 'disabled' : ''} onclick="updateReviewedTradeStatus(${row.id}, 'APPROVED')">Approve</button><button class="button reject" ${rejectDisabled ? 'disabled' : ''} onclick="updateReviewedTradeStatus(${row.id}, 'REJECTED')">Reject</button><button class="button execute" ${executeDisabled ? 'disabled' : ''} onclick="executeReviewedTrade(${row.id})">Execute</button></div></td></tr>`;
      }).join('');
    }

    function renderRecentExecutions() {
      const modeFilter = document.getElementById('executionModeFilter').value;
      const statusFilter = document.getElementById('executionStatusFilter').value;
      const rows = state.executions.filter((row) => (modeFilter === 'ALL' || row.mode === modeFilter) && (statusFilter === 'ALL' || row.status === statusFilter));
      const body = document.getElementById('recentExecutionsTable');
      const empty = document.getElementById('recentExecutionsEmpty');
      if (!rows.length) { body.innerHTML = ''; empty.hidden = false; return; }
      empty.hidden = true;
      body.innerHTML = rows.slice(0, 12).map((row) => `<tr><td>${row.id}</td><td>${row.mode}</td><td>${row.broker}</td><td><span class="status-pill">${row.status}</span></td><td>${row.reviewed_trade_id ?? '-'}</td><td><button class="button audit" onclick="openExecutionAudit(${row.id})">Open Audit</button></td></tr>`).join('');
    }

    function renderAuditLogs(rowsOverride = null) {
      const brokerFilter = document.getElementById('auditBrokerFilter').value;
      const statusFilter = document.getElementById('auditStatusFilter').value;
      const rows = (rowsOverride || state.auditLogs).filter((row) => (brokerFilter === 'ALL' || row.broker === brokerFilter) && (statusFilter === 'ALL' || row.status === statusFilter));
      const body = document.getElementById('auditTable');
      const empty = document.getElementById('auditEmpty');
      const preview = document.getElementById('payloadPreview');
      if (!rows.length) { body.innerHTML = ''; empty.hidden = false; preview.textContent = 'Waiting for audit data...'; return; }
      empty.hidden = true;
      body.innerHTML = rows.map((row) => {
        let payload = row.request_payload;
        try { payload = JSON.stringify(JSON.parse(row.request_payload), null, 2); } catch (error) {}
        return `<tr><td>${row.execution_id}</td><td>${row.broker}</td><td><span class="status-pill">${row.status}</span></td><td><code>${payload.slice(0, 120)}</code></td></tr>`;
      }).join('');
      try { preview.textContent = JSON.stringify(JSON.parse(rows[0].request_payload), null, 2); } catch (error) { preview.textContent = rows[0].request_payload; }
    }

    async function loadExecutionAuditById() {
      const id = document.getElementById('executionIdInput').value;
      if (!id) { showFlash('Enter an execution ID first.', 'error'); return; }
      try {
        const audit = await getJson(`/executions/${id}/audit`);
        renderAuditLogs(audit.audit_logs || []);
        showFlash(`Loaded audit logs for execution ${id}.`);
      } catch (error) { showFlash(error.message, 'error'); }
    }

    async function loadDashboard() {
      const [summary, reviewedTrades, auditLogs, executions] = await Promise.all([
        getJson('/dashboard/summary'),
        getJson('/reviewed-trades'),
        getJson('/executions/audit-logs'),
        getJson('/executions'),
      ]);
      state.reviewedTrades = reviewedTrades.reviewed_trades || [];
      state.executions = executions.executions || [];
      state.auditLogs = auditLogs.audit_logs || [];
      document.getElementById('brokerName').textContent = summary.broker_name || '-';
      const readyNode = document.getElementById('brokerReady');
      readyNode.innerHTML = summary.broker_ready ? '<span class="badge good">Ready</span>' : '<span class="badge bad">Missing Credentials</span>';
      const reviewedTotal = Object.values(summary.reviewed_trade_counts || {}).reduce((a, b) => a + b, 0);
      const executionTotal = Object.values(summary.execution_status_counts || {}).reduce((a, b) => a + b, 0);
      document.getElementById('reviewedTotal').textContent = String(reviewedTotal);
      document.getElementById('executionTotal').textContent = String(executionTotal);
      document.getElementById('auditFailures').textContent = String(summary.recent_audit_failures || 0);
      renderCountList('reviewedCounts', summary.reviewed_trade_counts);
      renderCountList('executionStatusCounts', summary.execution_status_counts);
      renderCountList('executionModeCounts', summary.execution_mode_counts);
      renderCountList('auditStatusCounts', summary.audit_status_counts);
      renderReviewedTrades();
      renderRecentExecutions();
      renderAuditLogs();
    }

    document.getElementById('refreshBtn').addEventListener('click', () => loadDashboard().catch((error) => showFlash(error.message, 'error')));
    document.getElementById('refreshExecutionAuditBtn').addEventListener('click', () => loadExecutionAuditById());
    document.getElementById('reviewedTradeStatusFilter').addEventListener('change', renderReviewedTrades);
    document.getElementById('executionModeFilter').addEventListener('change', renderRecentExecutions);
    document.getElementById('executionStatusFilter').addEventListener('change', renderRecentExecutions);
    document.getElementById('auditBrokerFilter').addEventListener('change', () => renderAuditLogs());
    document.getElementById('auditStatusFilter').addEventListener('change', () => renderAuditLogs());

    loadDashboard().catch((error) => { document.getElementById('payloadPreview').textContent = error.message; });
  </script>
</body>
</html>
"""


def _render_login(error_message: str | None = None) -> HTMLResponse:
    error_block = f'<div class="error">{error_message}</div>' if error_message else ''
    return HTMLResponse(LOGIN_HTML.replace('__ERROR_BLOCK__', error_block))


@router.get('/admin', response_class=HTMLResponse)
def admin_console(request: Request) -> HTMLResponse:
    if not is_authenticated(request):
        return _render_login()
    return HTMLResponse(ADMIN_HTML)


@router.post('/admin/login', response_model=None)
def admin_login(username: str = Form(...), password: str = Form(...)):
    if username != admin_username() or password != admin_password():
        return _render_login('Invalid admin username or password.')
    response = RedirectResponse(url='/admin', status_code=303)
    response.set_cookie(COOKIE_NAME, session_token(), httponly=True, samesite='lax')
    return response


@router.post('/admin/logout', response_model=None)
def admin_logout():
    response = RedirectResponse(url='/admin', status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response




