from __future__ import annotations

import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.aws_storage import build_s3_key, upload_text_to_s3 # type: ignore
from src.breakout_bot import generate_trades as generate_breakout_trades, load_candles  # type: ignore
try:
    from src.demand_supply_bot import generate_trades as generate_demand_supply_trades  # type: ignore
except (ImportError, ModuleNotFoundError):
    generate_demand_supply_trades = None  # type: ignore
try:
    from .execution_engine import (
        build_execution_candidates,
        default_quantity_for_symbol,
        execute_live_trades,
        execute_paper_trades,
        live_trading_unlock_status,
    ) # type: ignore
except (ImportError, ModuleNotFoundError) as e:
    raise ImportError(f"Could not import execution_engine. Ensure execution_engine.py exists in src/. Error: {e}")
try:
    from src.indicator_bot import IndicatorConfig, build_indicator_summary, generate_indicator_rows  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .indicator_bot import IndicatorConfig, build_indicator_summary, generate_indicator_rows  # type: ignore
try:
    from src.live_ohlcv import fetch_live_ohlcv, write_csv as write_live_ohlcv_csv  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .live_ohlcv import fetch_live_ohlcv, write_csv as write_live_ohlcv_csv  # type: ignore
try:
    from src.one_trade_day_bot import generate_trades as generate_one_trade_day_trades  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .one_trade_day import generate_trades as generate_one_trade_day_trades  # type: ignore
try:
    from src.price_action import annotate_trades_with_zones  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .price_action import annotate_trades_with_zones  # type: ignore
try:
    from src.strike_selector import attach_option_strikes  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .strike_selector import attach_option_strikes  # type: ignore
try:
    from src.telegram_notifier import build_trade_summary, send_telegram_message  # type: ignore
except (ImportError, ModuleNotFoundError):
    from .telegram_notifier import build_trade_summary, send_telegram_message  # type: ignore


REQUIRED_COLUMNS = {"timestamp", "open", "high", "low", "close", "volume"}
STRATEGIES = [
    "Breakout (15m)",
    "Demand/Supply",
    "Indicator (RSI/ADX/MACD+VWAP)",
    "One Trade/Day (All Indicators)",
    "Paper Trading (Auto)",
    "Dhan API (Future)",
]
STRATEGY_PAGES = {
    "Breakout Page": "Breakout (15m)",
    "Demand/Supply Page": "Demand/Supply",
    "Indicator Page": "Indicator (RSI/ADX/MACD+VWAP)",
    "One Trade/Day Page": "One Trade/Day (All Indicators)",
    "Paper Trading Page": "Paper Trading (Auto)",
    "Dhan API Page": "Dhan API (Future)",
}


import streamlit as st

st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="ðŸ“ˆ",
    layout="wide"
)

st.markdown("""
<style>
.main {
    background-color: #f5f7fb;
}

h1 {
    color: #1f4e79;
    font-weight: 700;
}

.stButton>button {
    background-color: #1f77b4;
    color: white;
    border-radius: 10px;
    padding: 10px 20px;
}

.stDataFrame {
    border-radius: 10px;
}
</style>
""", unsafe_allow_html=True)


def _inject_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&display=swap');
        :root {
            --bg-1: #081019;
            --bg-2: #10263f;
            --bg-3: #1a3550;
            --panel-a: rgba(7, 16, 28, 0.74);
            --panel-b: rgba(17, 35, 57, 0.76);
            --text-main: #f5fbff;
            --text-soft: #bdd4e8;
            --mint: #43d9ad;
            --amber: #ffb347;
            --line: rgba(186, 212, 236, 0.24);
            --shadow: 0 10px 26px rgba(2, 7, 14, 0.4);
        }
        @keyframes drift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        html, body, .stApp {
    font-family: 'Outfit', 'Segoe UI', sans-serif;
    font-size: 14px;
    line-height: 1.4;
    color: var(--text-main);
}
        .block-container {
    padding-top: 1rem !important;
    padding-bottom: 2rem !important;
    max-width: 1400px !important;
    margin: auto !important;
}
        [data-testid="stSidebar"] {
            display: none !important;
        }
        .trade-hero {
            background: linear-gradient(140deg, rgba(9, 21, 35, 0.92), rgba(18, 39, 60, 0.86));
            border: 1px solid var(--line);
            border-radius: 14px;
            padding: 0.5rem 0.65rem;
            box-shadow: var(--shadow);
            margin-bottom: 0.3rem;
        }
        .trade-hero h1 { margin: 0; letter-spacing: 0.2px; }
        .trade-hero p { margin: 0.08rem 0 0; color: var(--text-soft); }
        [data-testid="stMetric"] {
            background: linear-gradient(165deg, var(--panel-a), var(--panel-b));
            border: 1px solid var(--line);
            border-radius: 10px;
            padding: 0.22rem 0.35rem;
            box-shadow: var(--shadow);
            min-height: 54px !important;
        }
        [data-testid="stMetric"] [data-testid="stMetricLabel"] {
            color: var(--text-soft) !important;
            font-weight: 700 !important;
        }
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: var(--mint) !important;
            font-weight: 900 !important;
            font-size: 1.25rem !important;
        }
        .stButton > button,
        .stDownloadButton > button,
        button[kind="secondary"],
        [data-testid="baseButton-secondary"] {
            border: 1px solid rgba(255, 255, 255, 0.22) !important;
            border-radius: 8px !important;
            min-height: 30px !important;
            padding: 0.15rem 0.55rem !important;
            line-height: 1.1 !important;
            font-weight: 800 !important;
        }
        .stButton > button {
            background: linear-gradient(120deg, var(--mint), #23b084);
            color: #042016;
            box-shadow: 0 8px 20px rgba(67, 217, 173, 0.2);
        }
        .stDownloadButton > button {
            background: linear-gradient(120deg, rgba(255, 179, 71, 0.28), rgba(255, 179, 71, 0.12));
            color: var(--text-main);
        }
        [data-baseweb="tab"] {
            border: 1px solid rgba(186, 212, 236, 0.35) !important;
            border-radius: 8px !important;
            margin-right: 3px !important;
            background: rgba(16, 41, 63, 0.72) !important;
            min-height: 24px !important;
            padding: 0 0.45rem !important;
        }
        [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(120deg, rgba(67, 217, 173, 0.28), rgba(255, 179, 71, 0.25)) !important;
            border-color: rgba(255, 179, 71, 0.8) !important;
            box-shadow: 0 0 0 1px rgba(67, 217, 173, 0.55) !important;
        }
        [data-testid="stDataFrame"] {
            border: 1px solid var(--line);
            border-radius: 10px;
            overflow: hidden;
            box-shadow: var(--shadow);
        }
        .stDataFrame td,
        .stDataFrame th,
        [data-testid="stDataFrame"] div[role="gridcell"] {
            background: #132b40 !important;
            color: #f2f8ff !important;
            font-weight: 700 !important;
        }
        [data-testid="stNumberInput"] input,
        [data-testid="stTextInput"] input,
        [data-testid="stSelectbox"] div[data-baseweb="select"] > div {
            background: #11293f !important;
            border: 1px solid #2f4e69 !important;
            color: #eff7ff !important;
            -webkit-text-fill-color: #eff7ff !important;
            font-weight: 700 !important;
        }
        [data-testid="stFileUploader"] {
            border: 1px dashed rgba(176, 210, 236, 0.4);
            border-radius: 8px;
            background: rgba(13, 29, 45, 0.6);
            padding: 0.2rem;
        }
        [data-testid="stAppViewContainer"],
        .stApp,
        section.main,
        .block-container {
            height: 100vh !important;
            max-height: 100vh !important;
            overflow: hidden !important;
        }
        .block-container {
            padding-top: 0.08rem !important;
            padding-bottom: 0.08rem !important;
            padding-left: 0.45rem !important;
            padding-right: 0.45rem !important;
            max-width: 1380px !important;
            margin: 0 auto !important;
        }
        [data-testid="stMainBlockContainer"] { row-gap: 0.3rem !important; }
        [data-testid="stVerticalBlock"] { gap: 0.3rem !important; }
        [data-testid="stHorizontalBlock"] { gap: 0.3rem !important; }
        h1, h2, h3, h4 { margin: 0.02rem 0 !important; line-height: 1.05 !important; }
        [data-testid="stMarkdownContainer"] p { margin-bottom: 0.08rem !important; }
        .element-container { margin-bottom: 0.2rem !important; }
        @media (min-width: 1600px) {
            .block-container { max-width: 1480px !important; }
        }
        @media (max-width: 900px) {
            .block-container {
                max-width: 100% !important;
                padding-left: 0.3rem !important;
                padding-right: 0.3rem !important;
            }
            [data-testid="stHorizontalBlock"] { gap: 0.2rem !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _auto_refresh(seconds: int) -> None:
    if seconds <= 0:
        return
    ms = int(seconds * 1000)
    components.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {ms});
        </script>
        """,
        height=0,
    )



def _decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _load_rows_from_upload(uploaded_file) -> list[dict[str, str]]:
    content = _decode_bytes(uploaded_file.getvalue())
    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames is None:
        raise ValueError("CSV must include a header row")

    missing = REQUIRED_COLUMNS.difference({h.strip() for h in reader.fieldnames})
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    return list(reader)


def _to_csv(rows: list[dict[str, object]]) -> str:
    if not rows:
        return ""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()



def _dhan_candidates_from_rows(rows: list[dict[str, object]], default_qty: int) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for row in rows:
        side = str(row.get("side", "HOLD")).upper()
        quantity_val = row.get("quantity")
        quantity = int(str(quantity_val)) if quantity_val else default_qty
        candidates.append(
            {
                "strategy": str(row.get("strategy", "DHAN_SIGNAL")),
                "symbol": str(row.get("symbol", "NIFTY")),
                "signal_time": str(row.get("timestamp", "")),
                "side": side,
                "price": row.get("price", ""),
                "quantity": quantity,
                "reason": str(row.get("reason", "Dhan tab signal")),
            }
        )
    return candidates

def _strategy_selector() -> str:
    page_names = list(STRATEGY_PAGES.keys())
    selected_page = st.selectbox("Page", options=page_names, index=0, label_visibility="collapsed")
    return STRATEGY_PAGES.get(str(selected_page), STRATEGIES[0])

def _signal_time_map(candidates: list[dict[str, object]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for c in candidates:
        side = str(c.get("side", "")).upper()
        if side not in {"BUY", "SELL"}:
            continue
        out[str(c.get("signal_time", ""))] = side
    return out


def _render_live_chart(
    live_rows: list[dict[str, object]],
    indicator_rows: list[dict[str, object]],
    cand_breakout: list[dict[str, object]],
    cand_ds: list[dict[str, object]],
    cand_ind: list[dict[str, object]],
) -> None:
    ind_map = {str(r.get("timestamp", "")): r for r in indicator_rows}
    b_map = _signal_time_map(cand_breakout)
    d_map = _signal_time_map(cand_ds)
    i_map = _signal_time_map(cand_ind)

    rows: list[dict[str, object]] = []
    for r in live_rows:
        ts = str(r["timestamp"])
        close = float(str(r["close"]))
        vwap = ind_map.get(ts, {}).get("vwap", None)
        rows.append(
            {
                "timestamp": ts,
                "close": close,
                "vwap": None if vwap in (None, "") else float(vwap),
                "breakout_signal": close if ts in b_map else None,
                "ds_signal": close if ts in d_map else None,
                "ind_signal": close if ts in i_map else None,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    base = alt.Chart(df).encode(x=alt.X("timestamp:T", title="Time"))
    close_line = base.mark_line(color="#43d9ad", strokeWidth=2).encode(y=alt.Y("close:Q", title="Price"))
    vwap_line = base.mark_line(color="#ffb347", strokeDash=[6, 4], strokeWidth=2).encode(y="vwap:Q")

    b_points = alt.Chart(df.dropna(subset=["breakout_signal"])).mark_point(shape="triangle-up", size=110, color="#64b5f6").encode(
        x="timestamp:T", y="breakout_signal:Q"
    )
    d_points = alt.Chart(df.dropna(subset=["ds_signal"])).mark_point(shape="diamond", size=100, color="#ff8a65").encode(
        x="timestamp:T", y="ds_signal:Q"
    )
    i_points = alt.Chart(df.dropna(subset=["ind_signal"])).mark_point(shape="circle", size=90, color="#ba68c8").encode(
        x="timestamp:T", y="ind_signal:Q"
    )

    chart = alt.layer(close_line, vwap_line, b_points, d_points, i_points).properties(height=130).interactive()
    st.altair_chart(chart, use_container_width=True)
    st.caption("Chart markers: Blue=Breakout, Orange=Demand/Supply, Purple=Indicator. Signals are execution-ready points.")

def main() -> None:
    st.set_page_config(
        page_title="Intratrade Algo Bot",
        page_icon="ðŸ“ˆ",
        layout="wide"
    )

    _inject_theme()

    # HEADER
    h1, h2 = st.columns([3, 1])

    with h1:
        st.markdown(
            """
            <div class="trade-hero">
                <h1>ðŸ“ˆ Intratrade Algo Desk</h1>
                <p>Live Trading Dashboard â€¢ Breakout â€¢ Demand/Supply â€¢ Indicator Bots</p>
                <p><b>UI Version:</b> V2</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with h2:
        st.metric("Bot Status", "Running")

    st.divider()

# WEBSITE STYLE NAVIGATION
tab1, tab2, tab3, tab4 = st.tabs([
    "ðŸ“Š Dashboard",
    "ðŸ“ˆ Charts",
    "ðŸ¤– Strategies",
    "âš™ï¸ Settings"
])

# DASHBOARD
with tab1:
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Capital", "â‚¹100000")

    with c2:
        st.metric("Trades", "12")

    with c3:
        st.metric("Win Rate", "67%")

    with c4:
        st.metric("PnL", "â‚¹5,200")

# CHART PAGE
with tab2:
    st.subheader("Market Chart")
    st.write("Your live chart will appear here")

# STRATEGY PAGE
with tab3:
    st.subheader("Trading Strategies")
    st.write("Breakout / Demand Supply / Indicator")

# SETTINGS PAGE
with tab4:
    st.subheader("Bot Settings")
    st.write("Risk management and configuration")

# PAGE SELECTOR
st.markdown("### ðŸ“„ Pages")
strategy = _strategy_selector()

# Strategy modes
indicator_mode = strategy == "Indicator (RSI/ADX/MACD+VWAP)"
paper_mode = strategy == "Paper Trading (Auto)"
dhan_mode = strategy == "Dhan API (Future)"
one_trade_mode = strategy == "One Trade/Day (All Indicators)"
ds_mode = strategy == "Demand/Supply"

# CREATE PARAMETER TABS
p1, p2, p3, p4 = st.tabs(["Risk", "Strategy", "Data", "Integrations"])

# RISK TAB
with p1:
    st.markdown("### Risk Management")
    
    r1, r2 = st.columns(2)
    
    with r1:
        capital = st.number_input(
            "Capital (â‚¹)",
            min_value=1000,
            max_value=10000000,
            value=100000,
            step=1000
        )
        
        risk_pct = st.slider(
            "Risk per trade (%)",
            min_value=0.1,
            max_value=5.0,
            value=1.0,
            step=0.1
        )
    
    with r2:
        rr_ratio = st.slider(
            "Risk/Reward ratio",
            min_value=1.0,
            max_value=5.0,
            value=2.0,
            step=0.1
        )
        
        trailing_sl_pct = st.slider(
            "Trailing SL (%)",
            min_value=0.1,
            max_value=5.0,
            value=1.0,
            step=0.1
        )

# STRATEGY TAB
with p2:

    st.markdown("### Strategy Parameters")

    c1, c2 = st.columns(2)

    with c1:
        pivot_window = st.number_input(
            "Zone pivot window",
            min_value=1,
            max_value=4,
            value=2,
            step=1,
            disabled=indicator_mode
        )

        entry_cutoff = st.text_input(
            "Entry cutoff (HH:MM)",
            value="11:30"
        )

        rsi_overbought = st.slider(
            "RSI overbought",
            min_value=55.0,
            max_value=90.0,
            value=70.0,
            step=1.0
        )

    with c2:
        rsi_oversold = st.slider(
            "RSI oversold",
            min_value=10.0,
            max_value=45.0,
            value=30.0,
            step=1.0
        )

        adx_trend_min = st.slider(
            "ADX trend min",
            min_value=10.0,
            max_value=40.0,
            value=20.0,
            step=1.0
        )

        rsi_oversold = st.slider(
            "RSI oversold",
            min_value=10.0,
            max_value=45.0,
            value=30.0,
            step=1.0
        )

    with c2:
        adx_trend_min = st.slider(
            "ADX trend min",
            min_value=10.0,
            max_value=40.0,
            value=20.0,
            step=1.0
        )

        auto_strike_enabled = st.checkbox(
            "Enable auto strike",
            value=True,
            disabled=indicator_mode or paper_mode
        )

        strike_step = st.selectbox(
            "Strike interval",
            options=[50, 100],
            index=0,
            disabled=(not auto_strike_enabled) or indicator_mode or paper_mode
        )

        moneyness = st.selectbox(
            "Moneyness",
            options=["ATM", "OTM", "ITM"],
            index=0,
            disabled=(not auto_strike_enabled) or indicator_mode or paper_mode
        )

        strike_steps = st.number_input(
            "OTM/ITM steps",
            min_value=1,
            max_value=10,
            value=1,
            step=1,
            disabled=(not auto_strike_enabled)
            or indicator_mode
            or paper_mode
            or (moneyness == "ATM"),
        )

    with p3:
     st.markdown("### Data and Automation")

     d1, d2 = st.columns(2)

    with d1:
        execute_choice = st.radio(
            "Execute trades?",
            options=["No", "Yes"],
            horizontal=True
        )

        execution_symbol = st.text_input(
            "Execution symbol",
            value="NIFTY"
        )

        live_symbol = st.text_input(
            "Live symbol",
            value="^NSEI",
            disabled=not paper_mode
        )

        live_interval = st.selectbox(
            "Live interval",
            options=[
                "1m","2m","5m","15m","30m",
                "60m","90m","1h","1d","5d",
                "1wk","1mo","3mo"
            ],
            index=2,
            disabled=not paper_mode
        )

    with d2:
        live_period = st.selectbox(
            "Live period",
            options=["1d", "5d", "1mo"],
            index=0,
            disabled=not paper_mode
        )

        refresh_sec = st.number_input(
            "Auto refresh (sec)",
            min_value=10,
            max_value=300,
            value=30,
            step=5,
            disabled=not paper_mode
        )

        auto_paper = st.checkbox(
            "Auto paper execute on refresh",
            value=True,
            disabled=not paper_mode
        )

        website_auto_refresh = st.checkbox(
            "Enable auto page refresh",
            value=True
        )

        website_refresh_sec = st.number_input(
            "Website refresh (sec)",
            min_value=10,
            max_value=600,
            value=30,
            step=5
        )

with p4:
    st.markdown("### Integrations")

    i1, i2, i3 = st.columns(3)

    # DHAN API
    with i1:
        st.markdown("#### Dhan API")

        dhan_enabled = st.checkbox(
            "Enable Dhan integration",
            value=False,
            disabled=not dhan_mode
        )

        dhan_client_id = st.text_input(
            "Dhan client ID",
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

        dhan_access_token = st.text_input(
            "Dhan access token",
            type="password",
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

        dhan_exchange = st.selectbox(
            "Exchange",
            options=["NSE", "BSE", "NFO"],
            index=0,
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

        dhan_product = st.selectbox(
            "Product",
            options=["INTRADAY", "CNC", "MARGIN"],
            index=0,
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

        dhan_order_type = st.selectbox(
            "Order type",
            options=["MARKET", "LIMIT", "SL"],
            index=0,
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

        dhan_default_qty = st.number_input(
            "Default quantity",
            min_value=1,
            max_value=100000,
            value=int(default_quantity_for_symbol(execution_symbol)),
            step=1,
            disabled=(not dhan_mode) or (not dhan_enabled)
        )

    # TELEGRAM
    with i2:
        st.markdown("#### Telegram Alerts")

        telegram_enabled = st.checkbox(
            "Enable Telegram notifications",
            value=False
        )

        telegram_token = st.text_input(
            "Bot token",
            type="password",
            disabled=not telegram_enabled
        )

        telegram_chat_id = st.text_input(
            "Chat ID",
            disabled=not telegram_enabled
        )

    # AWS
    with i3:
        st.markdown("#### AWS Export")

        aws_enabled = st.checkbox(
            "Enable AWS export",
            value=False
        )

        s3_bucket = st.text_input(
            "S3 bucket",
            disabled=not aws_enabled
        )

        s3_region = st.text_input(
            "AWS region",
            value="ap-south-1",
            disabled=not aws_enabled
        )

        s3_prefix = st.text_input(
            "S3 key prefix",
            value="intratrade/exports",
            disabled=not aws_enabled
        )

cfg = IndicatorConfig(
    rsi_period=14,
    adx_period=14,
    macd_fast=12,
    macd_slow=26,
    macd_signal=9
)
output_rows = []
execution_candidates = []

if paper_mode:
    try:
        live_rows = fetch_live_ohlcv(live_symbol, live_interval, live_period)
        candles = load_candles(live_rows)

        breakout_rows = generate_breakout_trades(candles, capital=capital, risk_pct=risk_pct / 100.0, rr_ratio=rr_ratio, trailing_sl_pct=trailing_sl_pct / 100.0)
        for r in breakout_rows:
            r.setdefault("strategy", "BREAKOUT")

        if generate_demand_supply_trades is None:
            st.error("Demand/Supply module not available. Install required dependencies.")
            ds_rows = []
        else:
            ds_rows = generate_demand_supply_trades(
                candles,
                capital=capital,
                risk_pct=risk_pct / 100.0,
                rr_ratio=rr_ratio,
                pivot_window=int(pivot_window),
                entry_cutoff_hhmm=entry_cutoff,
                trailing_sl_pct=trailing_sl_pct / 100.0,
            )

        indicator_rows = generate_indicator_rows(candles, config=cfg)
        latest_ind = indicator_rows[-1]

        cand_breakout = build_execution_candidates("Breakout (15m)", breakout_rows, execution_symbol)
        cand_ds = build_execution_candidates("Demand/Supply", ds_rows, execution_symbol)
        cand_ind = build_execution_candidates("Indicator (RSI/ADX/MACD+VWAP)", indicator_rows, execution_symbol)
        execution_candidates = cand_breakout + cand_ds + cand_ind

        ready_count = sum(1 for c in execution_candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"})
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("RSI", str(latest_ind.get("rsi", "")))
        m2.metric("ADX", str(latest_ind.get("adx", "")))
        m3.metric("MACD", str(latest_ind.get("macd", "")))
        m4.metric("VWAP", str(latest_ind.get("vwap", "")))
        m5.metric("Exec Ready", ready_count)

        st.subheader("Live Market Chart")
        _render_live_chart(live_rows, indicator_rows, cand_breakout, cand_ds, cand_ind)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Breakout Signals", len(cand_breakout))
        c2.metric("Demand/Supply Signals", len(cand_ds))
        c3.metric("Indicator Signals", len(cand_ind))
        c4.metric("Total Candidates", len(execution_candidates))
        st.subheader("Breakout Signals")
        st.dataframe(breakout_rows, use_container_width=True, height=140)
        st.subheader("Demand/Supply Signals")
        st.dataframe(ds_rows, use_container_width=True, height=140)
        st.subheader("Indicator Signals")
        st.dataframe(indicator_rows[-30:], use_container_width=True, height=140)
        output_rows = execution_candidates

        if auto_paper:
            executed_auto = execute_paper_trades(
                execution_candidates,
                Path("data/executed_trades.csv"),
                deduplicate=True
            )

            if executed_auto:
                st.success(f"Auto paper executed {len(executed_auto)} new trade(s).")

        _auto_refresh(int(refresh_sec))

        st.caption(
            f"Auto mode active: refresh every {int(refresh_sec)}s using live {live_symbol} {live_interval} data."
        )

    except Exception as exc:
        st.error(f"Paper mode failed: {exc}")

else:
    if dhan_mode:
        st.subheader("Dhan API Integration (Paper Now, Live After 30 Days)")
        st.caption("Paper execution is enabled now. Live execution unlocks after 30 calendar days of paper-trade history.")
        st.subheader("Dhan Auth")
        st.write("Configure and validate Dhan credentials for API access.")
        st.code('{"client_id":"<DHAN_CLIENT_ID>","access_token":"<DHAN_ACCESS_TOKEN>"}', language="json")

        st.subheader("Dhan Market Feed")
        st.write("Map symbols, intervals, and feed IDs for strategy data ingestion.")
        st.code('{"exchange":"NSE","security_id":"13","interval":"1m"}', language="json")

        st.subheader("Dhan Order Router")
        st.write("Use this payload contract for future order placement.")
        st.code('{"symbol":"NIFTY","side":"BUY","quantity":65,"order_type":"MARKET","product_type":"INTRADAY"}', language="json")

        st.subheader("Dhan Webhooks")
        st.write("Webhook callback format for reconciliation and order status updates.")
        st.code('{"event":"order_update","order_id":"12345","status":"TRADED"}', language="json")

        st.subheader("Dhan Order Signals")
        template_rows = [
                {
                    "timestamp": "2026-03-06T09:45:00+05:30",
                    "symbol": execution_symbol,
                    "side": "BUY",
                    "quantity": int(dhan_default_qty),
                    "price": "",
                    "strategy": "Indicator+DemandSupply+Breakout",
                    "signal_confidence": 0.78,
                }
            ]

        dhan_upload = st.file_uploader("Upload Dhan signal CSV (optional)", type=["csv"], key="dhan_signal_upload")
        signal_rows = template_rows
        if dhan_upload is not None:
                content = _decode_bytes(dhan_upload.getvalue())
                parsed = list(csv.DictReader(io.StringIO(content)))
                if parsed:
                    signal_rows = parsed

        st.dataframe(signal_rows, use_container_width=True, height=140)
        st.download_button(
                "Download Dhan Order Template CSV",
                data=_to_csv(template_rows),
                file_name="dhan_order_template.csv",
                mime="text/csv",
            )

        default_lot = int(default_quantity_for_symbol(execution_symbol))
        dhan_candidates = _dhan_candidates_from_rows(signal_rows, default_qty=default_lot)

        paper_log = Path("data/dhan_paper_trades.csv")
        live_log = Path("data/dhan_live_trades.csv")
        unlocked, paper_days, unlock_date = live_trading_unlock_status(paper_log, min_days=30)

        m1, m2, m3 = st.columns(3)
        m1.metric("Paper Days", paper_days)
        m2.metric("Live Unlock", "YES" if unlocked else "NO")
        m3.metric("NIFTY Lot", default_lot)

        if unlock_date:
                st.caption(f"Live unlock date (UTC): {unlock_date}")
        else:
                st.caption("Live unlock date will be available after first paper trade.")

        if st.button("Execute Dhan Paper Trades"):
                executed = execute_paper_trades(dhan_candidates, paper_log, deduplicate=True)
                if executed:
                    st.success(f"Dhan paper executed {len(executed)} new trade(s).")
                    st.dataframe(executed, use_container_width=True, height=130)
                else:
                    st.warning("No new BUY/SELL signals for paper execution.")

        live_ready = unlocked and dhan_enabled and bool(dhan_client_id.strip()) and bool(dhan_access_token.strip())
        if not unlocked:
                st.warning("Live execution is locked until 30 days of paper history are completed.")

        if st.button("Execute Dhan Live Trades", disabled=not live_ready):
            executed_live = execute_live_trades(dhan_candidates, live_log, deduplicate=True)
            if executed_live:
                st.success(f"Dhan live sent {len(executed_live)} trade(s).")
                st.dataframe(executed_live, use_container_width=True, height=130)
            else:
                st.warning("No new BUY/SELL signals for live execution.")

    else:
        uploaded_file = st.file_uploader("Upload intraday OHLCV CSV", type=["csv"])
    st.markdown("Required columns: `timestamp, open, high, low, close, volume`")
    
    if uploaded_file is None:
            st.info("Upload a CSV to run selected bot.")
            st.stop()

try:
        rows = _load_rows_from_upload(uploaded_file)
        candles = load_candles(rows)

        if indicator_mode:
            output_rows = generate_indicator_rows(candles, config=cfg)
            latest = output_rows[-1]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Signal", str(latest["market_signal"]))
            c2.metric("RSI", str(latest["rsi"]))
            c3.metric("ADX", str(latest["adx"]))
            c4.metric("MACD", str(latest["macd"]))
            st.subheader("Indicator Data")
            st.dataframe(output_rows[-30:], use_container_width=True, height=130)

        elif one_trade_mode:
            output_rows = generate_one_trade_day_trades(
                candles,
                capital=capital,
                risk_pct=risk_pct / 100.0,
                rr_ratio=rr_ratio,
                config=cfg,
                entry_cutoff_hhmm=entry_cutoff,
                trailing_sl_pct=trailing_sl_pct / 100.0,
            )

        elif ds_mode:
            if generate_demand_supply_trades is None:
                st.error("Demand/Supply module not available. Install required dependencies.")
                output_rows = []
            else:
                output_rows = generate_demand_supply_trades(
                    candles,
                    capital=capital,
                    risk_pct=risk_pct / 100.0,
                    rr_ratio=rr_ratio,
                    pivot_window=int(pivot_window),
                    entry_cutoff_hhmm=entry_cutoff,
                    trailing_sl_pct=trailing_sl_pct / 100.0,
                )

        else:
            output_rows = generate_breakout_trades(candles, capital=capital, risk_pct=risk_pct / 100.0, rr_ratio=rr_ratio, trailing_sl_pct=trailing_sl_pct / 100.0)
            output_rows = annotate_trades_with_zones(output_rows, candles, pivot_window=int(pivot_window))
            for trade in output_rows:
                trade.setdefault("strategy", "BREAKOUT")

        if (not indicator_mode) and auto_strike_enabled:
            output_rows = attach_option_strikes(
                output_rows,
                strike_step=int(strike_step),
                moneyness=str(moneyness),
                steps=int(strike_steps),
            )

        execution_candidates = build_execution_candidates(strategy, output_rows, execution_symbol)

        if not indicator_mode:
            total_pnl = sum(float(t["pnl"]) for t in output_rows) if output_rows else 0.0
            win_count = sum(1 for t in output_rows if float(t.get("pnl", 0.0)) > 0) if output_rows else 0
            win_rate = (win_count / len(output_rows)) * 100.0 if output_rows else 0.0
            c1, c2, c3 = st.columns(3)
            c1.metric("Trades", len(output_rows))
            c2.metric("Total PnL", f"{total_pnl:.2f}")
            c3.metric("Win Rate", f"{win_rate:.2f}%")

except Exception as exc:
        st.error(f"Could not run strategy: {exc}")
        st.stop()

if not output_rows:
        st.warning("No output generated.")
        st.stop()

st.caption(f"Active strategy: {strategy}")

st.subheader("Execution Data")
st.dataframe(execution_candidates, use_container_width=True, height=130)

if (execute_choice == "Yes") and st.button("Execute Trades Now (Paper)"):
        executed = execute_paper_trades(execution_candidates, Path("data/executed_trades.csv"), deduplicate=True)
        if executed:
            st.success(f"Paper executed {len(executed)} new trade(s). Log: data/executed_trades.csv")
            st.dataframe(executed, use_container_width=True, height=130)
        else:
            st.warning("No new executable BUY/SELL signals.")

if telegram_enabled and st.button("Send Telegram summary"):
        try:
            message = build_indicator_summary(output_rows) if (strategy == "Indicator (RSI/ADX/MACD+VWAP)") else build_trade_summary(output_rows)
            send_telegram_message(telegram_token, telegram_chat_id, message)
            st.success("Telegram notification sent.")
        except Exception as exc:
            st.error(f"Telegram send failed: {exc}")

st.subheader("Output")
st.dataframe(output_rows, use_container_width=True, height=130)

csv_data = _to_csv(output_rows)

if aws_enabled and st.button("Upload CSV to S3"):
        try:
            s3_key = build_s3_key(s3_prefix, "output.csv")
            s3_uri = upload_text_to_s3(
                bucket=s3_bucket,
                key=s3_key,
                body=csv_data,
                region=s3_region,
            )
            st.success(f"Uploaded to {s3_uri}")
        except Exception as exc:
            st.error(f"AWS upload failed: {exc}")

st.download_button(
        "Download CSV",
        data=csv_data,
        file_name="output.csv",
        mime="text/csv",
    )

    # AUTO WEBSITE REFRESH
  #  if website_auto_refresh:
     #   import time
     #   time.sleep(int(website_refresh_sec))
     #   st.rerun()

if __name__ == "__main__":
    main()






