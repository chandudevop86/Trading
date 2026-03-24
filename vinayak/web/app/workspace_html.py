from __future__ import annotations


def _page(title: str, body: str, script: str = '') -> str:
    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #07111d;
      --panel: rgba(12, 24, 41, 0.94);
      --panel-soft: rgba(18, 35, 56, 0.88);
      --text: #eef4ff;
      --muted: #8ea6c7;
      --line: rgba(132, 166, 207, 0.20);
      --accent: #f59e0b;
      --accent-2: #38bdf8;
      --accent-3: #22c55e;
      --danger: #fb7185;
      --good: #4ade80;
      --shadow: 0 24px 60px rgba(0, 0, 0, 0.28);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: Segoe UI, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(245, 158, 11, 0.16), transparent 24%),
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.14), transparent 28%),
        linear-gradient(180deg, #0c1828 0%, #07111d 64%, #050b15 100%);
    }}
    .shell {{ max-width: 1560px; margin: 0 auto; padding: 24px 22px 44px; }}
    .nav {{
      display:flex; justify-content:space-between; align-items:center; gap:14px; padding: 16px 20px;
      margin-bottom: 18px; border: 1px solid var(--line); border-radius: 22px; background: rgba(9,18,30,0.84);
      backdrop-filter: blur(12px); box-shadow: var(--shadow);
    }}
    .brand-wrap {{ display:flex; flex-direction:column; gap:4px; }}
    .top-nav-brand {{ display:flex; align-items:center; gap:10px; font-weight:800; color:#f4f8ff; }}
    .top-nav-logo {{ width:34px; height:34px; border-radius:12px; display:grid; place-items:center; background: linear-gradient(135deg, #ffb45f, var(--accent)); color:#111; font-weight:900; }}
    .eyebrow {{ font-size: 11px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; color: #ffd089; }}
    .brand {{ font-size: 28px; font-weight: 800; }}
    .subbrand {{ color: var(--muted); font-size: 13px; }}
    .nav-actions {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .button, button, .download-link, .top-tab {{
      border: 1px solid var(--line); border-radius: 14px; min-height: 42px; padding: 10px 15px;
      font-weight: 800; cursor: pointer; text-decoration: none; display:inline-flex; align-items:center; justify-content:center;
      background: rgba(255,255,255,0.03); color: var(--text);
    }}
    .button.primary, button.primary {{ background: linear-gradient(135deg, #ffb45f, var(--accent)); color:#111; border-color: transparent; }}
    .button.highlight {{ background: linear-gradient(135deg, #5aa8ff, var(--accent-2)); color:#07111d; border-color: transparent; }}
    .top-tabs {{ display:flex; gap:10px; flex-wrap:wrap; margin: 0 0 18px 0; padding: 12px 14px; border:1px solid var(--line); border-radius: 18px; background: rgba(10,20,34,0.82); box-shadow: var(--shadow); }}
    .top-tab {{ border-radius:999px; }}
    .top-tab.active {{ background: linear-gradient(135deg, #5aa8ff, var(--accent-2)); color:#07111d; border-color: transparent; }}
    .hero {{ display:grid; grid-template-columns: 1.2fr .8fr; gap:18px; margin-bottom:18px; }}
    .card {{ background: linear-gradient(180deg, var(--panel), rgba(8,16,28,0.98)); border:1px solid var(--line); border-radius: 24px; padding: 22px; box-shadow: var(--shadow); }}
    h1 {{ margin: 10px 0 8px; font-size: clamp(34px, 4.5vw, 56px); line-height:1.02; }}
    .lead {{ margin:0; color:var(--muted); font-size:17px; line-height:1.7; max-width:760px; }}
    .ribbon {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:16px; }}
    .pill {{ padding:8px 12px; border-radius:999px; border:1px solid var(--line); background:rgba(255,255,255,0.04); color:#e8f1ff; font-size:13px; font-weight:700; }}
    .stats, .metric-row, .grid2, .grid3, .report-grid {{ display:grid; gap:12px; }}
    .stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .metric-row {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
    .grid2 {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .grid3 {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    .report-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .stat, .metric, .report-card {{ padding:16px; border:1px solid var(--line); border-radius:18px; background: rgba(255,255,255,0.03); }}
    .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .value {{ margin-top:7px; font-size:26px; font-weight:800; }}
    .layout {{ display:grid; grid-template-columns: 390px minmax(0, 1fr); gap:18px; }}
    .stack {{ display:grid; gap:18px; }}
    .section-title {{ margin:0 0 14px; font-size:19px; color:#f4f8ff; }}
    label {{ display:block; margin-bottom:6px; color:var(--muted); font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; }}
    input, select {{ width:100%; min-height:46px; border-radius:14px; border:1px solid var(--line); background: rgba(255,255,255,0.04); color:var(--text); padding:10px 12px; }}
    .toggle {{ display:flex; align-items:center; gap:10px; padding:11px 12px; border:1px solid var(--line); border-radius:14px; background:rgba(255,255,255,0.03); }}
    .toggle input {{ width:auto; min-height:auto; accent-color: var(--accent); }}
    .actions, .page-links {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }}
    .flash {{ margin-bottom:12px; padding:12px 14px; border-radius:14px; border:1px solid var(--line); display:none; font-weight:700; }}
    .flash.good {{ display:block; background: rgba(34,197,94,0.12); color:#bbf7d0; border-color: rgba(74,222,128,0.24); }}
    .flash.bad {{ display:block; background: rgba(251,113,133,0.12); color:#fecdd3; border-color: rgba(251,113,133,0.24); }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ text-align:left; padding:11px 10px; border-bottom:1px solid rgba(132,166,207,0.12); font-size:14px; vertical-align:top; }}
    th {{ color:#b8ccea; font-size:12px; text-transform:uppercase; letter-spacing:.06em; background:#102034; position: sticky; top: 0; }}
    .table-shell {{ overflow:auto; max-height: 420px; border:1px solid var(--line); border-radius:16px; background: rgba(5,12,22,0.34); }}
    code, pre {{ color:#d9e8ff; background: rgba(255,255,255,0.04); border:1px solid var(--line); border-radius:12px; }}
    pre {{ margin:0; padding:14px; white-space:pre-wrap; font-size:12px; line-height:1.5; max-height: 360px; overflow:auto; }}
    .muted {{ color:var(--muted); }}
    .footer-note {{ margin-top:10px; font-size:12px; color:var(--muted); }}
    @media (max-width: 1180px) {{ .hero, .layout, .metric-row, .report-grid {{ grid-template-columns: 1fr; }} .grid3 {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 760px) {{ .grid2, .stats {{ grid-template-columns: 1fr; }} .nav {{ flex-direction:column; align-items:flex-start; }} }}
  </style>
</head>
<body>
{body}
{script}
</body>
</html>
"""


WORKSPACE_HTML = _page(
    'Vinayak Trading Workspace',
    """
  <div class=\"shell\">
    <div class=\"nav\">
      <div class=\"brand-wrap\">
        <div class=\"top-nav-brand\"><div class=\"top-nav-logo\">K</div><div>KRSH <span style=\"color:#8ea6c7; font-weight:700;\">Solutions</span></div></div>
        <div class=\"eyebrow\">KRSH Trading Workspace</div>
        <div class=\"brand\">Vinayak Workspace</div>
        <div class=\"subbrand\">Common trading actions on one page, related outputs split into dedicated pages.</div>
      </div>
      <div class=\"nav-actions\">
        <a class=\"button secondary\" href=\"/workspace/reports\">Reports</a>
        <a class=\"button secondary\" href=\"/workspace/downloads\">Downloads</a>
        <a class=\"button secondary\" href=\"/admin\">Admin</a>
        <a class=\"button secondary\" href=\"/health\">Health</a>
        <form method=\"post\" action=\"/admin/logout\"><button class=\"button primary\" type=\"submit\">Logout</button></form>
      </div>
    </div>

    <div class=\"top-tabs\">
      <button class=\"top-tab active\" type=\"button\" data-scroll=\"heroSection\">Home</button>
      <button class=\"top-tab\" type=\"button\" data-scroll=\"controlSection\">Desk Controls</button>
      <button class=\"top-tab\" type=\"button\" data-scroll=\"marketSection\">Market</button>
      <button class=\"top-tab\" type=\"button\" data-scroll=\"tradesSection\">Trades</button>
    </div>

    <div id=\"heroSection\" class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Common Actions</div>
        <h1>Run live market analysis and review the latest trading setup from one clean page.</h1>
        <p class=\"lead\">This page keeps the most-used KRSH trading flow together: choose your symbol and strategy, fetch candles, run analysis, and review signals. Reports and downloads are moved to separate pages so the main workspace stays simple.</p>
        <div class=\"ribbon\">
          <div class=\"pill\">Live analysis</div>
          <div class=\"pill\">Trade review</div>
          <div class=\"pill\">Option metrics</div>
          <div class=\"pill\">Telegram and execution</div>
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
      <section id=\"controlSection\" class=\"card\">
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
          <div><label for=\"lotSize\">Lot Size</label><input id=\"lotSize\" type=\"number\" value=\"65\" /></div>
          <div><label for=\"lots\">Lots</label><input id=\"lots\" type=\"number\" value=\"1\" /></div>
          <div><label for=\"executionType\">Execution Type</label><select id=\"executionType\"><option selected>NONE</option><option>PAPER</option><option>LIVE</option></select></div>
          <div><label for=\"strikeSteps\">Strike Steps</label><input id=\"strikeSteps\" type=\"number\" value=\"0\" /></div>
        </div>
        <div class=\"grid3\" style=\"margin-top:12px;\">
          <div class=\"toggle\"><input id=\"fetchOptionMetrics\" type=\"checkbox\" /><label for=\"fetchOptionMetrics\" style=\"margin:0;\">Fetch Option Metrics</label></div>
          <div class=\"toggle\"><input id=\"sendTelegram\" type=\"checkbox\" /><label for=\"sendTelegram\" style=\"margin:0;\">Send Telegram</label></div>
          <div class=\"toggle\"><input id=\"autoExecute\" type=\"checkbox\" /><label for=\"autoExecute\" style=\"margin:0;\">Auto Execute</label></div>
        </div>
        <div class=\"grid2\" style=\"margin-top:12px;\">
          <div><label for=\"telegramToken\">Telegram Token</label><input id=\"telegramToken\" placeholder=\"Bot token\" /></div>
          <div><label for=\"telegramChatId\">Telegram Chat ID</label><input id=\"telegramChatId\" placeholder=\"Chat id\" /></div>
        </div>
        <div class=\"actions\">
          <button id=\"runAnalysisBtn\" class=\"primary\" type=\"button\">Run Live Analysis</button>
          <button id=\"loadCandlesBtn\" class=\"highlight\" type=\"button\">Preview Candles</button>
        </div>
        <div class=\"page-links\">
          <a class=\"button secondary\" href=\"/workspace/reports\">Open Reports Page</a>
          <a class=\"button secondary\" href=\"/workspace/downloads\">Open Downloads Page</a>
        </div>
        <div class=\"footer-note\">Only the common trading controls stay here. Related outputs are separated to reduce button clutter.</div>
      </section>

      <div class=\"stack\">
        <section class=\"card\">
          <div class=\"metric-row\">
            <div class=\"metric\"><div class=\"label\">Side Counts</div><strong id=\"sideCounts\">No run yet.</strong></div>
            <div class=\"metric\"><div class=\"label\">Telegram</div><strong id=\"telegramStatus\">Not sent.</strong></div>
            <div class=\"metric\"><div class=\"label\">Generated At</div><strong id=\"generatedAt\">-</strong></div>
            <div class=\"metric\"><div class=\"label\">Related Pages</div><strong style=\"font-size:16px;\">Reports / Downloads</strong></div>
          </div>
        </section>

        <section id=\"marketSection\" class=\"card\">
          <h2 class=\"section-title\">Market Overview</h2>
          <div class=\"table-shell\"><table><thead><tr><th>Timestamp</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Volume</th><th>Source</th></tr></thead><tbody id=\"candleTable\"></tbody></table></div>
          <div id=\"candleEmpty\" class=\"muted\" style=\"margin-top:10px;\">No candle snapshot yet.</div>
        </section>

        <section id=\"tradesSection\" class=\"card\">
          <h2 class=\"section-title\">Signal Rows</h2>
          <div class=\"table-shell\"><table><thead><tr><th>Strategy</th><th>Side</th><th>Entry</th><th>SL</th><th>Target</th><th>Option</th><th>Expiry</th></tr></thead><tbody id=\"signalTable\"></tbody></table></div>
          <div id=\"signalEmpty\" class=\"muted\" style=\"margin-top:10px;\">No signals yet.</div>
        </section>

        <section class=\"card\">
          <h2 class=\"section-title\">Execution Rows</h2>
          <div class=\"table-shell\"><table><thead><tr><th>Trade</th><th>Side</th><th>Status</th><th>Broker</th><th>Price</th><th>Reason</th></tr></thead><tbody id=\"executionTable\"></tbody></table></div>
          <div id=\"executionEmpty\" class=\"muted\" style=\"margin-top:10px;\">No execution rows yet.</div>
        </section>
      </div>
    </div>
  </div>
    """,
    """
  <script>
    const STORAGE_KEY = 'vinayak_latest_run';

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
    }

    function renderResult(result) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(result));
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
    }

    function loadStoredRun() {
      try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (!raw) return;
        renderResult(JSON.parse(raw));
      } catch (error) {}
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
        renderCandles(result.candles || []);
        flash('Loaded latest candle snapshot.');
      } catch (error) {
        flash(error.message, 'bad');
      }
    });

    document.querySelectorAll('.top-tab').forEach((button) => {
      button.addEventListener('click', () => {
        document.querySelectorAll('.top-tab').forEach((node) => node.classList.remove('active'));
        button.classList.add('active');
        const target = document.getElementById(button.dataset.scroll);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      });
    });

    loadStoredRun();
  </script>
    """
)


WORKSPACE_REPORTS_HTML = _page(
    'Vinayak Reports',
    """
  <div class=\"shell\">
    <div class=\"nav\">
      <div class=\"brand-wrap\">
        <div class=\"top-nav-brand\"><div class=\"top-nav-logo\">K</div><div>KRSH <span style=\"color:#8ea6c7; font-weight:700;\">Solutions</span></div></div>
        <div class=\"eyebrow\">KRSH Trading Workspace</div>
        <div class=\"brand\">Reports</div>
        <div class=\"subbrand\">Related report outputs are separated from the common trading page.</div>
      </div>
      <div class=\"nav-actions\">
        <a class=\"button secondary\" href=\"/workspace\">Main Workspace</a>
        <a class=\"button secondary\" href=\"/workspace/downloads\">Downloads</a>
        <a class=\"button secondary\" href=\"/admin\">Admin</a>
      </div>
    </div>
    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Related Page</div>
        <h1>Review the latest report artifacts and execution summary.</h1>
        <p class=\"lead\">This page reads the latest run stored in the browser after you execute analysis from the main workspace. It keeps reporting-related output separate from the common trading controls.</p>
      </section>
      <aside class=\"card\">
        <div class=\"stats\">
          <div class=\"stat\"><div class=\"label\">Strategy</div><div id=\"statStrategy\" class=\"value\">-</div></div>
          <div class=\"stat\"><div class=\"label\">Generated</div><div id=\"statGenerated\" class=\"value\">-</div></div>
        </div>
      </aside>
    </div>
    <div class=\"stack\">
      <section class=\"card\">
        <div class=\"report-grid\">
          <div class=\"report-card\"><div class=\"label\">JSON Report</div><div id=\"jsonReportPath\" class=\"value\" style=\"font-size:16px;\">-</div><div id=\"jsonReportS3\" class=\"muted\" style=\"margin-top:8px;\">No S3 artifact.</div></div>
          <div class=\"report-card\"><div class=\"label\">Summary Report</div><div id=\"summaryReportPath\" class=\"value\" style=\"font-size:16px;\">-</div><div id=\"summaryReportS3\" class=\"muted\" style=\"margin-top:8px;\">No S3 artifact.</div></div>
        </div>
      </section>
      <section class=\"card\"><h2 class=\"section-title\">Execution Summary</h2><pre id=\"executionSummary\">Run live analysis on the main workspace first.</pre></section>
      <section class=\"card\"><h2 class=\"section-title\">Latest Report Payload</h2><pre id=\"reportPreview\">Run live analysis on the main workspace first.</pre></section>
    </div>
  </div>
    """,
    """
  <script>
    const STORAGE_KEY = 'vinayak_latest_run';
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const result = JSON.parse(raw);
        const artifacts = result.report_artifacts || {};
        const jsonReport = artifacts.json_report || {};
        const summaryReport = artifacts.summary_report || {};
        document.getElementById('statStrategy').textContent = result.strategy || '-';
        document.getElementById('statGenerated').textContent = result.generated_at || '-';
        document.getElementById('jsonReportPath').textContent = jsonReport.local_path || '-';
        document.getElementById('jsonReportS3').textContent = jsonReport.s3_uri || jsonReport.s3_error || 'No S3 artifact.';
        document.getElementById('summaryReportPath').textContent = summaryReport.local_path || '-';
        document.getElementById('summaryReportS3').textContent = summaryReport.s3_uri || summaryReport.s3_error || 'No S3 artifact.';
        document.getElementById('executionSummary').textContent = JSON.stringify(result.execution_summary || {}, null, 2);
        document.getElementById('reportPreview').textContent = JSON.stringify({ report_artifacts: artifacts, telegram_payload: result.telegram_payload || {} }, null, 2);
      }
    } catch (error) {}
  </script>
    """
)


WORKSPACE_DOWNLOADS_HTML = _page(
    'Vinayak Downloads',
    """
  <div class=\"shell\">
    <div class=\"nav\">
      <div class=\"brand-wrap\">
        <div class=\"top-nav-brand\"><div class=\"top-nav-logo\">K</div><div>KRSH <span style=\"color:#8ea6c7; font-weight:700;\">Solutions</span></div></div>
        <div class=\"eyebrow\">KRSH Trading Workspace</div>
        <div class=\"brand\">Downloads</div>
        <div class=\"subbrand\">Related export actions are moved away from the common trading page.</div>
      </div>
      <div class=\"nav-actions\">
        <a class=\"button secondary\" href=\"/workspace\">Main Workspace</a>
        <a class=\"button secondary\" href=\"/workspace/reports\">Reports</a>
        <a class=\"button secondary\" href=\"/admin\">Admin</a>
      </div>
    </div>
    <div class=\"hero\">
      <section class=\"card\">
        <div class=\"eyebrow\">Related Page</div>
        <h1>Download candles, signals, and the full run payload from the latest analysis.</h1>
        <p class=\"lead\">The main workspace stays focused on analysis and review. Download actions are separated here so the common page has fewer buttons.</p>
      </section>
      <aside class=\"card\">
        <div class=\"stats\">
          <div class=\"stat\"><div class=\"label\">Signals</div><div id=\"statSignals\" class=\"value\">0</div></div>
          <div class=\"stat\"><div class=\"label\">Candles</div><div id=\"statCandles\" class=\"value\">0</div></div>
        </div>
      </aside>
    </div>
    <div class=\"stack\">
      <section class=\"card\"><h2 class=\"section-title\">Export Actions</h2><div class=\"actions\" style=\"margin-top:0;\"><a id=\"downloadCandles\" class=\"download-link\" download=\"vinayak_candles.json\" href=\"#\">Download Candles JSON</a><a id=\"downloadSignals\" class=\"download-link\" download=\"vinayak_signals.json\" href=\"#\">Download Signals JSON</a><a id=\"downloadRun\" class=\"download-link\" download=\"vinayak_run.json\" href=\"#\">Download Full Run JSON</a></div></section>
      <section class=\"card\"><h2 class=\"section-title\">Latest Run Snapshot</h2><pre id=\"downloadPreview\">Run live analysis on the main workspace first.</pre></section>
    </div>
  </div>
    """,
    """
  <script>
    const STORAGE_KEY = 'vinayak_latest_run';
    function setDownloadLink(id, filename, payload) {
      const node = document.getElementById(id);
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      node.href = URL.createObjectURL(blob);
      node.download = filename;
    }
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const result = JSON.parse(raw);
        document.getElementById('statSignals').textContent = String((result.signals || []).length);
        document.getElementById('statCandles').textContent = String((result.candles || []).length);
        setDownloadLink('downloadCandles', 'vinayak_candles.json', result.candles || []);
        setDownloadLink('downloadSignals', 'vinayak_signals.json', result.signals || []);
        setDownloadLink('downloadRun', 'vinayak_run.json', result);
        document.getElementById('downloadPreview').textContent = JSON.stringify({ symbol: result.symbol, strategy: result.strategy, generated_at: result.generated_at, side_counts: result.side_counts || {} }, null, 2);
      }
    } catch (error) {}
  </script>
    """
)
