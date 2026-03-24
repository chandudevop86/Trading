WORKSPACE_HTML = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Vinayak Live Workspace</title>
  <style>
    :root {
      --bg: #07111d;
      --panel: rgba(15, 28, 45, 0.92);
      --panel-2: rgba(10, 20, 34, 0.96);
      --text: #eef4ff;
      --muted: #8ea6c7;
      --line: rgba(138, 176, 224, 0.16);
      --accent: #f59e0b;
      --accent-2: #22c55e;
      --accent-3: #38bdf8;
      --danger: #fb7185;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Segoe UI, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(245, 158, 11, 0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.14), transparent 32%),
        linear-gradient(180deg, #0a1422 0%, #07111d 60%, #050b15 100%);
    }
    .shell { max-width: 1400px; margin: 0 auto; padding: 28px 22px 40px; }
    .nav { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:22px; }
    .brand { font-size: 24px; font-weight: 800; letter-spacing: .03em; }
    .nav-actions { display:flex; gap:10px; flex-wrap:wrap; }
    .button, button {
      border: 0;
      border-radius: 14px;
      min-height: 44px;
      padding: 11px 16px;
      font-weight: 800;
      cursor: pointer;
      text-decoration: none;
    }
    .button.primary, button.primary { background: linear-gradient(135deg, var(--accent), #fb923c); color:#111; }
    .button.secondary, button.secondary { background: rgba(255,255,255,0.04); color: var(--text); border:1px solid var(--line); }
    .hero {
      display:grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap:18px;
      margin-bottom:18px;
    }
    .card {
      background: linear-gradient(180deg, var(--panel), var(--panel-2));
      border: 1px solid var(--line);
      border-radius: 24px;
      padding: 22px;
      box-shadow: 0 24px 60px rgba(0,0,0,0.24);
    }
    .eyebrow {
      display:inline-flex;
      padding:7px 11px;
      border-radius:999px;
      background: rgba(255,255,255,0.05);
      border:1px solid rgba(255,255,255,0.08);
      color:#ffd089;
      font-size:11px;
      font-weight:800;
      letter-spacing:.14em;
      text-transform:uppercase;
    }
    h1 { margin: 14px 0 10px; font-size: clamp(32px, 5vw, 54px); line-height:1.02; }
    .lead { margin:0; color:var(--muted); font-size:17px; line-height:1.65; max-width:760px; }
    .stats { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    .stat { padding:16px; border:1px solid var(--line); border-radius:18px; background: rgba(255,255,255,0.03); }
    .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .value { margin-top:8px; font-size:24px; font-weight:800; }
    .layout { display:grid; grid-template-columns: 360px minmax(0, 1fr); gap:18px; }
    .stack { display:grid; gap:18px; }
    .section-title { margin:0 0 14px; font-size:18px; }
    .grid2 { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    .grid3 { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; }
    label { display:block; margin-bottom:6px; color:var(--muted); font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }
    input, select {
      width:100%; min-height:44px; border-radius:14px; border:1px solid var(--line);
      background: rgba(255,255,255,0.04); color:var(--text); padding:10px 12px;
    }
    input::placeholder { color:#7890b1; }
    .toggle { display:flex; align-items:center; gap:10px; padding:11px 12px; border:1px solid var(--line); border-radius:14px; background:rgba(255,255,255,0.03); }
    .toggle input { width:auto; min-height:auto; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }
    .flash { margin-bottom:12px; padding:12px 14px; border-radius:14px; border:1px solid var(--line); display:none; }
    .flash.good { display:block; background: rgba(34,197,94,0.12); color:#bbf7d0; }
    .flash.bad { display:block; background: rgba(251,113,133,0.12); color:#fecdd3; }
    .ribbon { display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }
    .pill { padding:8px 12px; border-radius:999px; border:1px solid var(--line); background: rgba(255,255,255,0.04); color:#d6e5ff; font-size:13px; font-weight:700; }
    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding:11px 10px; border-bottom:1px solid rgba(138,176,224,0.10); font-size:14px; vertical-align:top; }
    th { color:#b9cdea; font-size:12px; text-transform:uppercase; letter-spacing:.06em; }
    .table-shell { overflow:auto; }
    code, pre { color:#d8e8ff; background: rgba(255,255,255,0.04); border-radius:12px; }
    pre { margin:0; padding:14px; white-space:pre-wrap; font-size:12px; line-height:1.5; }
    .muted { color:var(--muted); }
    @media (max-width: 1100px) {
      .hero, .layout { grid-template-columns: 1fr; }
      .grid3 { grid-template-columns: 1fr; }
    }
    @media (max-width: 720px) {
      .grid2 { grid-template-columns: 1fr; }
      .nav { flex-direction:column; align-items:flex-start; }
    }
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"nav\">
      <div class=\"brand\">Vinayak Workspace</div>
      <div class=\"nav-actions\">
        <a class=\"button secondary\" href=\"/admin\">Admin</a>
        <a class=\"button secondary\" href=\"/health\">Health</a>
        <form method=\"post\" action=\"/admin/logout\"><button class=\"button primary\" type=\"submit\">Logout</button></form>
      </div>
    </div>

    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Integrated Trading Workspace</div>
        <h1>Run live analysis, option enrichment, Telegram alerts, and execution from Vinayak.</h1>
        <p class=\"lead\">This workspace now reuses the Trading project workflow inside Vinayak. Fetch candles, generate strategy rows, enrich options, send alerts, and optionally push PAPER or LIVE execution in one flow.</p>
        <div class=\"ribbon\">
          <div class=\"pill\">Endpoint: <code>/dashboard/live-analysis</code></div>
          <div class=\"pill\">Live candles: <code>/dashboard/candles</code></div>
          <div class=\"pill\">Modes: NONE / PAPER / LIVE</div>
        </div>
      </section>
      <aside class=\"card\">
        <div class=\"stats\">
          <div class=\"stat\"><div class=\"label\">Last Strategy</div><div id=\"statStrategy\" class=\"value\">-</div></div>
          <div class=\"stat\"><div class=\"label\">Candles</div><div id=\"statCandles\" class=\"value\">0</div></div>
          <div class=\"stat\"><div class=\"label\">Signals</div><div id=\"statSignals\" class=\"value\">0</div></div>
          <div class=\"stat\"><div class=\"label\">Execution</div><div id=\"statExecution\" class=\"value\">NONE</div></div>
        </div>
      </aside>
    </div>

    <div class=\"layout\">
      <section class=\"card\">
        <h2 class=\"section-title\">Control Panel</h2>
        <div id=\"flash\" class=\"flash\"></div>
        <div class=\"grid2\">
          <div><label for=\"symbol\">Symbol</label><input id=\"symbol\" value=\"^NSEI\" /></div>
          <div><label for=\"strategy\">Strategy</label><select id=\"strategy\"><option>Breakout</option><option>Demand Supply</option><option>Indicator</option><option>One Trade/Day</option><option>MTF 5m</option><option>BTST</option></select></div>
          <div><label for=\"interval\">Interval</label><select id=\"interval\"><option>1m</option><option selected>5m</option><option>15m</option><option>1h</option><option>1d</option></select></div>
          <div><label for=\"period\">Period</label><select id=\"period\"><option selected>1d</option><option>5d</option><option>1mo</option><option>3mo</option></select></div>
          <div><label for=\"capital\">Capital</label><input id=\"capital\" type=\"number\" value=\"100000\" /></div>
          <div><label for=\"riskPct\">Risk %</label><input id=\"riskPct\" type=\"number\" step=\"0.1\" value=\"1\" /></div>
          <div><label for=\"rrRatio\">RR Ratio</label><input id=\"rrRatio\" type=\"number\" step=\"0.1\" value=\"2\" /></div>
          <div><label for=\"trailingSlPct\">Trailing SL %</label><input id=\"trailingSlPct\" type=\"number\" step=\"0.1\" value=\"0.5\" /></div>
          <div><label for=\"strikeStep\">Strike Step</label><input id=\"strikeStep\" type=\"number\" value=\"50\" /></div>
          <div><label for=\"moneyness\">Moneyness</label><select id=\"moneyness\"><option selected>ATM</option><option>ITM</option><option>OTM</option></select></div>
          <div><label for=\"strikeSteps\">Strike Steps</label><input id=\"strikeSteps\" type=\"number\" value=\"0\" /></div>
          <div><label for=\"executionType\">Execution Type</label><select id=\"executionType\"><option selected>NONE</option><option>PAPER</option><option>LIVE</option></select></div>
        </div>
        <div class=\"grid3\" style=\"margin-top:12px;\">
          <div class=\"toggle\"><input id=\"fetchOptionMetrics\" type=\"checkbox\" /><label for=\"fetchOptionMetrics\" style=\"margin:0;\">Fetch Option Metrics</label></div>
          <div class=\"toggle\"><input id=\"sendTelegram\" type=\"checkbox\" /><label for=\"sendTelegram\" style=\"margin:0;\">Send Telegram</label></div>
          <div class=\"toggle\"><input id=\"autoExecute\" type=\"checkbox\" /><label for=\"autoExecute\" style=\"margin:0;\">Auto Execute</label></div>
        </div>
        <div class=\"grid2\" style=\"margin-top:12px;\">
          <div><label for=\"telegramToken\">Telegram Token</label><input id=\"telegramToken\" placeholder=\"Bot token\" /></div>
          <div><label for=\"telegramChatId\">Telegram Chat ID</label><input id=\"telegramChatId\" placeholder=\"Chat id\" /></div>
          <div><label for=\"lotSize\">Lot Size</label><input id=\"lotSize\" type=\"number\" value=\"65\" /></div>
          <div><label for=\"lots\">Lots</label><input id=\"lots\" type=\"number\" value=\"1\" /></div>
        </div>
        <div class=\"actions\">
          <button id=\"runAnalysisBtn\" class=\"primary\" type=\"button\">Run Live Analysis</button>
          <button id=\"loadCandlesBtn\" class=\"secondary\" type=\"button\">Preview Candles</button>
        </div>
      </section>

      <div class=\"stack\">
        <section class=\"card\">
          <h2 class=\"section-title\">Run Summary</h2>
          <div class=\"grid3\">
            <div class=\"stat\"><div class=\"label\">Side Counts</div><div id=\"sideCounts\" class=\"muted\">No run yet.</div></div>
            <div class=\"stat\"><div class=\"label\">Telegram</div><div id=\"telegramStatus\" class=\"muted\">Not sent.</div></div>
            <div class=\"stat\"><div class=\"label\">Generated At</div><div id=\"generatedAt\" class=\"muted\">-</div></div>
          </div>
        </section>
        <section class=\"card\">
          <h2 class=\"section-title\">Signal Rows</h2>
          <div class=\"table-shell\"><table><thead><tr><th>Strategy</th><th>Side</th><th>Entry</th><th>SL</th><th>Target</th><th>Option</th><th>Expiry</th></tr></thead><tbody id=\"signalTable\"></tbody></table></div>
          <div id=\"signalEmpty\" class=\"muted\">No signals yet.</div>
        </section>
        <section class=\"card\">
          <h2 class=\"section-title\">Execution Rows</h2>
          <div class=\"table-shell\"><table><thead><tr><th>Trade</th><th>Side</th><th>Status</th><th>Broker</th><th>Price</th><th>Reason</th></tr></thead><tbody id=\"executionTable\"></tbody></table></div>
          <div id=\"executionEmpty\" class=\"muted\">No execution rows yet.</div>
        </section>
        <section class=\"card\">
          <h2 class=\"section-title\">Live Candle Snapshot</h2>
          <pre id=\"candlePreview\">Waiting for market data...</pre>
        </section>
      </div>
    </div>
  </div>
  <script>
    function flash(message, tone='good') {
      const node = document.getElementById('flash');
      node.className = `flash ${tone}`;
      node.textContent = message;
      node.style.display = 'block';
      window.setTimeout(() => { node.style.display = 'none'; }, 3500);
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

    async function postJson(path, body) {
      const response = await fetch(path, { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(body) });
      if (!response.ok) {
        let detail = `${path} failed with ${response.status}`;
        try { const payload = await response.json(); detail = payload.detail || detail; } catch (error) {}
        throw new Error(detail);
      }
      return await response.json();
    }

    function payload() {
      return {
        symbol: document.getElementById('symbol').value,
        strategy: document.getElementById('strategy').value,
        interval: document.getElementById('interval').value,
        period: document.getElementById('period').value,
        capital: Number(document.getElementById('capital').value || 0),
        risk_pct: Number(document.getElementById('riskPct').value || 0),
        rr_ratio: Number(document.getElementById('rrRatio').value || 0),
        trailing_sl_pct: Number(document.getElementById('trailingSlPct').value || 0),
        strike_step: Number(document.getElementById('strikeStep').value || 50),
        moneyness: document.getElementById('moneyness').value,
        strike_steps: Number(document.getElementById('strikeSteps').value || 0),
        fetch_option_metrics: document.getElementById('fetchOptionMetrics').checked,
        send_telegram: document.getElementById('sendTelegram').checked,
        telegram_token: document.getElementById('telegramToken').value,
        telegram_chat_id: document.getElementById('telegramChatId').value,
        auto_execute: document.getElementById('autoExecute').checked,
        execution_type: document.getElementById('executionType').value,
        lot_size: Number(document.getElementById('lotSize').value || 0),
        lots: Number(document.getElementById('lots').value || 0),
        mtf_ema_period: 3,
        mtf_setup_mode: 'either',
        mtf_retest_strength: true,
        mtf_max_trades_per_day: 3
      };
    }

    function renderSignals(rows) {
      const body = document.getElementById('signalTable');
      const empty = document.getElementById('signalEmpty');
      if (!rows || !rows.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
      empty.style.display = 'none';
      body.innerHTML = rows.slice(0, 16).map((row) => `<tr><td>${row.strategy || '-'}</td><td>${row.side || '-'}</td><td>${row.entry_price ?? '-'}</td><td>${row.stop_loss ?? '-'}</td><td>${row.target_price ?? '-'}</td><td>${row.option_strike || '-'}</td><td>${row.option_expiry || '-'}</td></tr>`).join('');
    }

    function renderExecutions(rows) {
      const body = document.getElementById('executionTable');
      const empty = document.getElementById('executionEmpty');
      if (!rows || !rows.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
      empty.style.display = 'none';
      body.innerHTML = rows.slice(0, 16).map((row) => `<tr><td>${row.trade_id || row.trade_label || '-'}</td><td>${row.side || '-'}</td><td>${row.execution_status || row.trade_status || '-'}</td><td>${row.broker_name || '-'}</td><td>${row.price ?? '-'}</td><td>${row.reason || row.blocked_reason || row.validation_error || '-'}</td></tr>`).join('');
    }

    function renderResult(result) {
      document.getElementById('statStrategy').textContent = result.strategy || '-';
      document.getElementById('statCandles').textContent = String(result.candle_count || 0);
      document.getElementById('statSignals').textContent = String(result.signal_count || 0);
      document.getElementById('statExecution').textContent = result.execution_summary?.mode || 'NONE';
      document.getElementById('sideCounts').textContent = JSON.stringify(result.side_counts || {});
      document.getElementById('telegramStatus').textContent = result.telegram_sent ? 'Sent' : (result.telegram_error || 'Not sent');
      document.getElementById('generatedAt').textContent = result.generated_at || '-';
      document.getElementById('candlePreview').textContent = JSON.stringify((result.candles || []).slice(-8), null, 2);
      renderSignals(result.signals || []);
      renderExecutions(result.execution_rows || []);
    }

    document.getElementById('runAnalysisBtn').addEventListener('click', async () => {
      try {
        const result = await postJson('/dashboard/live-analysis', payload());
        renderResult(result);
        flash('Live analysis completed.');
      } catch (error) {
        flash(error.message, 'bad');
      }
    });

    document.getElementById('loadCandlesBtn').addEventListener('click', async () => {
      try {
        const p = payload();
        const result = await getJson(`/dashboard/candles?symbol=${encodeURIComponent(p.symbol)}&interval=${encodeURIComponent(p.interval)}&period=${encodeURIComponent(p.period)}`);
        document.getElementById('candlePreview').textContent = JSON.stringify((result.candles || []).slice(-8), null, 2);
        flash('Loaded latest candle snapshot.');
      } catch (error) {
        flash(error.message, 'bad');
      }
    });
  </script>
</body>
</html>
"""
