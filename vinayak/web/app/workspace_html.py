WORKSPACE_HTML = """
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Vinayak Trading Workspace</title>
  <style>
    :root {
      --bg: #f4f7fb;
      --panel: #ffffff;
      --panel-soft: #f8fbff;
      --text: #10243a;
      --muted: #5f7692;
      --line: #dbe5f0;
      --accent: #0f766e;
      --accent-2: #2563eb;
      --accent-3: #f59e0b;
      --danger: #dc2626;
      --good: #15803d;
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Segoe UI, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(37, 99, 235, 0.08), transparent 26%),
        radial-gradient(circle at top right, rgba(15, 118, 110, 0.10), transparent 30%),
        linear-gradient(180deg, #f7faff 0%, #f3f7fb 100%);
    }
    .shell { max-width: 1480px; margin: 0 auto; padding: 22px 18px 40px; }
    .nav {
      display:flex; justify-content:space-between; align-items:center; gap:14px;
      padding: 14px 18px; margin-bottom: 18px; border: 1px solid var(--line);
      border-radius: 20px; background: rgba(255,255,255,0.86); backdrop-filter: blur(10px);
      box-shadow: var(--shadow);
    }
    .brand-wrap { display:flex; flex-direction:column; gap:4px; }
    .eyebrow { font-size: 11px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; color: var(--accent); }
    .brand { font-size: 24px; font-weight: 800; }
    .subbrand { color: var(--muted); font-size: 13px; }
    .nav-actions { display:flex; gap:10px; flex-wrap:wrap; }
    .button, button, .download-link {
      border: 0; border-radius: 14px; min-height: 42px; padding: 10px 15px;
      font-weight: 800; cursor: pointer; text-decoration: none; display:inline-flex;
      align-items:center; justify-content:center;
    }
    .button.primary, button.primary { background: linear-gradient(135deg, var(--accent), #0ea5a0); color:#fff; }
    .button.secondary, button.secondary, .download-link { background: #fff; color: var(--text); border:1px solid var(--line); }
    .hero {
      display:grid; grid-template-columns: 1.2fr .8fr; gap:18px; margin-bottom:18px;
    }
    .card {
      background: var(--panel); border:1px solid var(--line); border-radius: 24px;
      padding: 20px; box-shadow: var(--shadow);
    }
    h1 { margin: 10px 0 8px; font-size: clamp(28px, 4vw, 46px); line-height:1.04; }
    .lead { margin:0; color:var(--muted); font-size:16px; line-height:1.7; max-width:760px; }
    .ribbon { display:flex; gap:10px; flex-wrap:wrap; margin-top:16px; }
    .pill { padding:8px 12px; border-radius:999px; border:1px solid var(--line); background:var(--panel-soft); color:#17324a; font-size:13px; font-weight:700; }
    .stats { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    .stat { padding:16px; border:1px solid var(--line); border-radius:18px; background: linear-gradient(180deg, #ffffff, #f8fbff); }
    .label { color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .value { margin-top:7px; font-size:24px; font-weight:800; }
    .layout { display:grid; grid-template-columns: 360px minmax(0, 1fr); gap:18px; }
    .stack { display:grid; gap:18px; }
    .section-title { margin:0 0 14px; font-size:18px; }
    .grid2 { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    .grid3 { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; }
    label { display:block; margin-bottom:6px; color:var(--muted); font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }
    input, select {
      width:100%; min-height:44px; border-radius:14px; border:1px solid var(--line);
      background: #fff; color:var(--text); padding:10px 12px;
    }
    .toggle { display:flex; align-items:center; gap:10px; padding:11px 12px; border:1px solid var(--line); border-radius:14px; background:var(--panel-soft); }
    .toggle input { width:auto; min-height:auto; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }
    .flash { margin-bottom:12px; padding:12px 14px; border-radius:14px; border:1px solid var(--line); display:none; font-weight:700; }
    .flash.good { display:block; background: rgba(21, 128, 61, 0.10); color:#166534; border-color: rgba(21, 128, 61, 0.16); }
    .flash.bad { display:block; background: rgba(220, 38, 38, 0.10); color:#991b1b; border-color: rgba(220, 38, 38, 0.16); }
    .tabs { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px; }
    .tab-button {
      border:1px solid var(--line); background:#fff; color:var(--text); border-radius:12px; padding:10px 12px;
      font-weight:800; cursor:pointer;
    }
    .tab-button.active { background: linear-gradient(135deg, var(--accent-2), #38bdf8); color:#fff; border-color: transparent; }
    .tab-panel { display:none; }
    .tab-panel.active { display:block; }
    .metric-row { display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap:12px; }
    .metric {
      border:1px solid var(--line); border-radius:18px; padding:16px; background: linear-gradient(180deg, #fff, #f8fbff);
    }
    .metric strong { display:block; margin-top:8px; font-size:24px; }
    table { width:100%; border-collapse:collapse; }
    th, td { text-align:left; padding:11px 10px; border-bottom:1px solid var(--line); font-size:14px; vertical-align:top; }
    th { color:#5d7591; font-size:12px; text-transform:uppercase; letter-spacing:.06em; background:#fbfdff; position: sticky; top: 0; }
    .table-shell { overflow:auto; max-height: 360px; border:1px solid var(--line); border-radius:16px; }
    code, pre {
      color:#17324a; background: #f7fbff; border:1px solid var(--line); border-radius:12px;
    }
    pre { margin:0; padding:14px; white-space:pre-wrap; font-size:12px; line-height:1.5; max-height: 320px; overflow:auto; }
    .muted { color:var(--muted); }
    .report-grid { display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }
    .report-card { border:1px solid var(--line); border-radius:18px; padding:16px; background: linear-gradient(180deg, #fff, #f8fbff); }
    .footer-note { margin-top:10px; font-size:12px; color:var(--muted); }
    @media (max-width: 1180px) {
      .hero, .layout, .metric-row, .report-grid { grid-template-columns: 1fr; }
      .grid3 { grid-template-columns: 1fr; }
    }
    @media (max-width: 760px) {
      .grid2, .stats { grid-template-columns: 1fr; }
      .nav { flex-direction:column; align-items:flex-start; }
    }
  </style>
</head>
<body>
  <div class=\"shell\">
    <div class=\"nav\">
      <div class=\"brand-wrap\">
        <div class=\"eyebrow\">Integrated Trading Workspace</div>
        <div class=\"brand\">Vinayak Workspace</div>
        <div class=\"subbrand\">Old Trading-style operational layout, now powered by the Vinayak backend.</div>
      </div>
      <div class=\"nav-actions\">
        <a class=\"button secondary\" href=\"/admin\">Admin</a>
        <a class=\"button secondary\" href=\"/health\">Health</a>
        <form method=\"post\" action=\"/admin/logout\"><button class=\"button primary\" type=\"submit\">Logout</button></form>
      </div>
    </div>

    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Trading Control Room</div>
        <h1>Run live market analysis, review trades, manage execution, and export reports in one workspace.</h1>
        <p class=\"lead\">This page keeps the new Vinayak live-analysis, Redis/S3-ready reporting, Telegram, and execution features, but brings back the older Trading app feeling: a control sidebar, market overview, trades area, and downloadable outputs.</p>
        <div class=\"ribbon\">
          <div class=\"pill\">Live analysis: <code>/dashboard/live-analysis</code></div>
          <div class=\"pill\">Candles: <code>/dashboard/candles</code></div>
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
        <h2 class=\"section-title\">Strategy Control Panel</h2>
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
        <div class=\"footer-note\">Tip: Run analysis first, then review Overview, Trades, Reports, and Downloads like the older Trading workspace flow.</div>
      </section>

      <div class=\"stack\">
        <section class=\"card\">
          <div class=\"tabs\">
            <button class=\"tab-button active\" type=\"button\" data-tab=\"overview\">Overview</button>
            <button class=\"tab-button\" type=\"button\" data-tab=\"trades\">Trades</button>
            <button class=\"tab-button\" type=\"button\" data-tab=\"reports\">Reports</button>
            <button class=\"tab-button\" type=\"button\" data-tab=\"downloads\">Downloads</button>
          </div>

          <div id=\"panel-overview\" class=\"tab-panel active\">
            <div class=\"metric-row\">
              <div class=\"metric\"><div class=\"label\">Side Counts</div><strong id=\"sideCounts\">No run yet.</strong></div>
              <div class=\"metric\"><div class=\"label\">Telegram</div><strong id=\"telegramStatus\">Not sent.</strong></div>
              <div class=\"metric\"><div class=\"label\">Generated At</div><strong id=\"generatedAt\">-</strong></div>
              <div class=\"metric\"><div class=\"label\">Report Status</div><strong id=\"reportStatus\">Not generated</strong></div>
            </div>
            <div class=\"card\" style=\"margin-top:16px; padding:16px;\">
              <h2 class=\"section-title\">Market Overview</h2>
              <div class=\"table-shell\" style=\"max-height:none;\"><table><thead><tr><th>Timestamp</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th><th>Source</th></tr></thead><tbody id=\"candleTable\"></tbody></table></div>
              <div id=\"candleEmpty\" class=\"muted\" style=\"margin-top:10px;\">No candle snapshot yet.</div>
            </div>
            <div class=\"card\" style=\"margin-top:16px; padding:16px;\">
              <h2 class=\"section-title\">Raw Candle Snapshot</h2>
              <pre id=\"candlePreview\">Waiting for market data...</pre>
            </div>
          </div>

          <div id=\"panel-trades\" class=\"tab-panel\">
            <div class=\"card\" style=\"padding:16px;\">
              <h2 class=\"section-title\">Signal Rows</h2>
              <div class=\"table-shell\"><table><thead><tr><th>Strategy</th><th>Side</th><th>Entry</th><th>SL</th><th>Target</th><th>Option</th><th>Expiry</th></tr></thead><tbody id=\"signalTable\"></tbody></table></div>
              <div id=\"signalEmpty\" class=\"muted\" style=\"margin-top:10px;\">No signals yet.</div>
            </div>
            <div class=\"card\" style=\"padding:16px; margin-top:16px;\">
              <h2 class=\"section-title\">Execution Rows</h2>
              <div class=\"table-shell\"><table><thead><tr><th>Trade</th><th>Side</th><th>Status</th><th>Broker</th><th>Price</th><th>Reason</th></tr></thead><tbody id=\"executionTable\"></tbody></table></div>
              <div id=\"executionEmpty\" class=\"muted\" style=\"margin-top:10px;\">No execution rows yet.</div>
            </div>
          </div>

          <div id=\"panel-reports\" class=\"tab-panel\">
            <div class=\"report-grid\">
              <div class=\"report-card\">
                <div class=\"label\">JSON Report</div>
                <div id=\"jsonReportPath\" class=\"value\" style=\"font-size:16px;\">-</div>
                <div id=\"jsonReportS3\" class=\"muted\" style=\"margin-top:8px;\">No S3 artifact.</div>
              </div>
              <div class=\"report-card\">
                <div class=\"label\">Summary Report</div>
                <div id=\"summaryReportPath\" class=\"value\" style=\"font-size:16px;\">-</div>
                <div id=\"summaryReportS3\" class=\"muted\" style=\"margin-top:8px;\">No S3 artifact.</div>
              </div>
            </div>
            <div class=\"card\" style=\"padding:16px; margin-top:16px;\">
              <h2 class=\"section-title\">Latest Run Payload</h2>
              <pre id=\"reportPreview\">Run live analysis to generate report artifacts.</pre>
            </div>
          </div>

          <div id=\"panel-downloads\" class=\"tab-panel\">
            <div class=\"actions\" style=\"margin-top:0;\">
              <a id=\"downloadCandles\" class=\"download-link\" download=\"vinayak_candles.json\" href=\"#\">Download Candles JSON</a>
              <a id=\"downloadSignals\" class=\"download-link\" download=\"vinayak_signals.json\" href=\"#\">Download Signals JSON</a>
              <a id=\"downloadRun\" class=\"download-link\" download=\"vinayak_run.json\" href=\"#\">Download Full Run JSON</a>
            </div>
            <div class=\"card\" style=\"padding:16px; margin-top:16px;\">
              <h2 class=\"section-title\">Download Notes</h2>
              <p class=\"muted\">This mirrors the older Trading app pattern where raw data and output rows are available for export after each run. The backend now also stores report artifacts through the Vinayak reporting layer.</p>
            </div>
          </div>
        </section>
      </div>
    </div>
  </div>
  <script>
    let latestRun = null;

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

    function setDownloadLink(id, filename, payload) {
      const node = document.getElementById(id);
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      node.href = URL.createObjectURL(blob);
      node.download = filename;
    }

    function renderSignals(rows) {
      const body = document.getElementById('signalTable');
      const empty = document.getElementById('signalEmpty');
      if (!rows || !rows.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
      empty.style.display = 'block';
      empty.textContent = `${rows.length} signal row(s) loaded.`;
      body.innerHTML = rows.slice(0, 24).map((row) => `<tr><td>${row.strategy || '-'}</td><td>${row.side || '-'}</td><td>${row.entry_price ?? '-'}</td><td>${row.stop_loss ?? '-'}</td><td>${row.target_price ?? '-'}</td><td>${row.option_strike || '-'}</td><td>${row.option_expiry || '-'}</td></tr>`).join('');
    }

    function renderExecutions(rows) {
      const body = document.getElementById('executionTable');
      const empty = document.getElementById('executionEmpty');
      if (!rows || !rows.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
      empty.style.display = 'block';
      empty.textContent = `${rows.length} execution row(s) loaded.`;
      body.innerHTML = rows.slice(0, 24).map((row) => `<tr><td>${row.trade_id || row.trade_label || '-'}</td><td>${row.side || '-'}</td><td>${row.execution_status || row.trade_status || '-'}</td><td>${row.broker_name || '-'}</td><td>${row.price ?? '-'}</td><td>${row.reason || row.blocked_reason || row.validation_error || '-'}</td></tr>`).join('');
    }

    function renderCandles(rows) {
      const body = document.getElementById('candleTable');
      const empty = document.getElementById('candleEmpty');
      if (!rows || !rows.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
      empty.style.display = 'none';
      body.innerHTML = rows.slice(-12).reverse().map((row) => `<tr><td>${row.timestamp || '-'}</td><td>${row.open ?? '-'}</td><td>${row.high ?? '-'}</td><td>${row.low ?? '-'}</td><td>${row.close ?? '-'}</td><td>${row.volume ?? '-'}</td><td>${row.source || '-'}</td></tr>`).join('');
      document.getElementById('candlePreview').textContent = JSON.stringify(rows.slice(-8), null, 2);
    }

    function renderReports(result) {
      const artifacts = result.report_artifacts || {};
      const jsonReport = artifacts.json_report || {};
      const summaryReport = artifacts.summary_report || {};
      document.getElementById('jsonReportPath').textContent = jsonReport.local_path || '-';
      document.getElementById('jsonReportS3').textContent = jsonReport.s3_uri || jsonReport.s3_error || 'No S3 artifact.';
      document.getElementById('summaryReportPath').textContent = summaryReport.local_path || '-';
      document.getElementById('summaryReportS3').textContent = summaryReport.s3_uri || summaryReport.s3_error || 'No S3 artifact.';
      document.getElementById('reportStatus').textContent = jsonReport.local_path || summaryReport.local_path ? 'Generated' : 'Not generated';
      document.getElementById('reportPreview').textContent = JSON.stringify({ report_artifacts: artifacts, execution_summary: result.execution_summary || {}, telegram_payload: result.telegram_payload || {} }, null, 2);
    }

    function renderResult(result) {
      latestRun = result;
      document.getElementById('statStrategy').textContent = result.strategy || '-';
      document.getElementById('statCandles').textContent = String(result.candle_count || 0);
      document.getElementById('statSignals').textContent = String(result.signal_count || 0);
      document.getElementById('statExecution').textContent = result.execution_summary?.mode || 'NONE';
      document.getElementById('sideCounts').textContent = JSON.stringify(result.side_counts || {});
      document.getElementById('telegramStatus').textContent = result.telegram_sent ? 'Sent' : (result.telegram_error || 'Not sent');
      document.getElementById('generatedAt').textContent = result.generated_at || '-';
      renderCandles(result.candles || []);
      renderSignals(result.signals || []);
      renderExecutions(result.execution_rows || []);
      renderReports(result);
      setDownloadLink('downloadCandles', 'vinayak_candles.json', result.candles || []);
      setDownloadLink('downloadSignals', 'vinayak_signals.json', result.signals || []);
      setDownloadLink('downloadRun', 'vinayak_run.json', result);
    }

    function activateTab(name) {
      document.querySelectorAll('.tab-button').forEach((node) => node.classList.toggle('active', node.dataset.tab === name));
      document.querySelectorAll('.tab-panel').forEach((node) => node.classList.toggle('active', node.id === `panel-${name}`));
    }

    document.querySelectorAll('.tab-button').forEach((button) => {
      button.addEventListener('click', () => activateTab(button.dataset.tab));
    });

    document.getElementById('runAnalysisBtn').addEventListener('click', async () => {
      try {
        const result = await postJson('/dashboard/live-analysis', payload());
        renderResult(result);
        activateTab('overview');
        flash('Live analysis completed.');
      } catch (error) {
        flash(error.message, 'bad');
      }
    });

    document.getElementById('loadCandlesBtn').addEventListener('click', async () => {
      try {
        const p = payload();
        const result = await getJson(`/dashboard/candles?symbol=${encodeURIComponent(p.symbol)}&interval=${encodeURIComponent(p.interval)}&period=${encodeURIComponent(p.period)}`);
        renderCandles(result.candles || []);
        setDownloadLink('downloadCandles', 'vinayak_candles.json', result.candles || []);
        flash('Loaded latest candle snapshot.');
      } catch (error) {
        flash(error.message, 'bad');
      }
    });
  </script>
</body>
</html>
"""
