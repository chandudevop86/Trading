from __future__ import annotations

import csv
import io
import json
import os
import inspect
import sys
from datetime import datetime, timedelta
from math import floor
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

from src.breakout_bot import Candle
from src.charting import build_live_market_chart, build_market_depth_summary, compute_market_levels
from src.breakout_bot import generate_trades as generate_breakout_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes, pick_option_strike
from src.telegram_notifier import build_trade_summary, send_telegram_message

try:
    from src.execution_engine import (
        build_analysis_queue,
        build_execution_candidates,
        execute_live_trades,
        execute_paper_trades,
    )
except Exception:
    build_analysis_queue = None
    build_execution_candidates = None
    execute_live_trades = None
    execute_paper_trades = None

try:
    from src.supply_demand import generate_trades as generate_demand_supply_trades
except Exception:
    generate_demand_supply_trades = None

try:
    from src.nse_option_chain import (
        build_metrics_map,
        extract_option_records,
        fetch_option_chain,
        normalize_index_symbol,
    )
except Exception:
    build_metrics_map = None
    extract_option_records = None
    fetch_option_chain = None
    normalize_index_symbol = None

try:
    from src.dhan_api import build_order_request_from_candidate, load_security_map
except Exception:
    build_order_request_from_candidate = None
    load_security_map = None



st.set_page_config(page_title="KRSH SOLUTIONS", page_icon="chart", layout="wide")

st.markdown(
    """
    <script>
    (function () {
        const ensureViewport = () => {
            let meta = document.querySelector('meta[name="viewport"]');
            if (!meta) {
                meta = document.createElement('meta');
                meta.name = 'viewport';
                document.head.appendChild(meta);
            }
            meta.content = 'width=device-width, initial-scale=1.0, maximum-scale=1.0';
        };
        ensureViewport();
        setTimeout(ensureViewport, 0);
    })();
    </script>
    """,
    unsafe_allow_html=True,
)
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")


def prepare_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = df.copy().reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.columns = [str(c).strip().lower() for c in df.columns]

    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "timestamp"})
    elif "date" in df.columns:
        df = df.rename(columns={"date": "timestamp"})
    elif "timestamp" not in df.columns:
        df = df.rename(columns={df.columns[0]: "timestamp"})

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required].copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    df["unix"] = df["timestamp"].astype("int64") // 10**9
    return df


def _df_to_candles(df: pd.DataFrame) -> list[Candle]:
    if df is None or df.empty:
        return []
    if "timestamp" not in df.columns:
        raise ValueError("Candles missing timestamp column")

    candles: list[Candle] = []
    for row in df.itertuples(index=False):
        ts = getattr(row, "timestamp", None)
        if ts is None:
            continue
        if isinstance(ts, pd.Timestamp):
            ts_dt = ts.to_pydatetime()
        else:
            ts_dt = pd.to_datetime(ts, errors="coerce")
            if pd.isna(ts_dt):
                continue
            ts_dt = ts_dt.to_pydatetime()

        candles.append(
            Candle(
                timestamp=ts_dt,
                open=float(getattr(row, "open", 0.0) or 0.0),
                high=float(getattr(row, "high", 0.0) or 0.0),
                low=float(getattr(row, "low", 0.0) or 0.0),
                close=float(getattr(row, "close", 0.0) or 0.0),
                volume=float(getattr(row, "volume", 0.0) or 0.0),
            )
        )

    candles.sort(key=lambda c: c.timestamp)
    return candles


def fetch_ohlcv_data(symbol: str, interval: str = "1m", period: str = "1d") -> pd.DataFrame:
    data = yf.download(
        tickers=symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
    )
    if data is None or data.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return prepare_trading_data(data)


def _to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()

def _order_trade_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    preferred = [
        "trade_label",
        "trade_no",
        "strategy",
        "symbol",
        "side",
        "entry_price",
        "spot_ltp",
        "target_1",
        "target_2",
        "target_3",
        "target_price",
        "stop_loss",
        "option_strike",
        "option_type",
        "option_ltp",
        "quantity",
        "lots",
        "order_value",
        "signal_time",
        "entry_time",
        "timestamp",
        "analysis_status",
        "execution_ready",
        "execution_type",
        "execution_status",
    ]
    ordered = [c for c in preferred if c in df.columns]
    ordered.extend([c for c in df.columns if c not in ordered])
    return df.loc[:, ordered]


def attach_futures_contracts(trades: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    annotated: list[dict[str, object]] = []
    future_symbol = f"{str(symbol).strip().upper()} FUT".strip()
    for trade in trades:
        row = dict(trade)
        row["instrument_mode"] = "Futures"
        row["trading_symbol"] = future_symbol
        row.setdefault("option_strike", future_symbol)
        annotated.append(row)
    return annotated

def send_signal_alert(row: dict[str, object], *, strategy: str, symbol: str, refresh_seconds: int) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        payload = dict(row)
        payload.setdefault("strategy", strategy)
        payload.setdefault("symbol", symbol)
        payload.setdefault("refresh_seconds", refresh_seconds)
        summary = build_trade_summary([payload])
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, summary)
    except Exception:
        return

def _resolve_live_execution_kwargs(security_map_path: str) -> dict[str, object]:
    security_map: dict[str, dict[str, str]] = {}
    if load_security_map is not None:
        try:
            security_map = load_security_map(Path(str(security_map_path)))
        except Exception:
            security_map = {}
    return {"broker_name": "DHAN", "security_map": security_map}

def _build_dhan_preview_rows(candidates: list[dict[str, object]], security_map_path: str) -> list[dict[str, object]]:
    if build_order_request_from_candidate is None or load_security_map is None:
        return [{"status": "Broker payload builder unavailable"}]
    try:
        security_map = load_security_map(Path(str(security_map_path)))
    except Exception as exc:
        return [{"status": f"Security map load failed: {exc}"}]
    preview_rows: list[dict[str, object]] = []
    client_id = os.getenv("DHAN_CLIENT_ID", "")
    for candidate in candidates:
        try:
            order_request = build_order_request_from_candidate(candidate, client_id=client_id, security_map=security_map)
            preview_rows.append({"status": "READY", **order_request.to_payload()})
        except Exception as exc:
            preview_rows.append({"status": "ERROR", "symbol": candidate.get("symbol", ""), "side": candidate.get("side", ""), "message": str(exc)})
    return preview_rows

def _run_dhan_readiness_check(symbol: str, security_map_path: str) -> list[str]:
    notes: list[str] = []
    notes.append("PASS client id detected" if os.getenv("DHAN_CLIENT_ID", "").strip() else "FAIL missing DHAN_CLIENT_ID")
    notes.append("PASS access token detected" if os.getenv("DHAN_ACCESS_TOKEN", "").strip() else "FAIL missing DHAN_ACCESS_TOKEN")
    path = Path(str(security_map_path))
    notes.append(f"PASS security map found: {path}" if path.exists() else f"WARN security map missing: {path}")
    notes.append(f"INFO live symbol context: {symbol}")
    return notes

def _render_live_execution_feedback(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    st.markdown('<div class="section-shell" style="margin-top:12px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">Live Execution Feedback</div><div class="section-copy">Latest broker-side execution rows from this run.</div>', unsafe_allow_html=True)
    st.dataframe(_order_trade_columns(pd.DataFrame(rows)), use_container_width=True, height=220)
    st.markdown('</div>', unsafe_allow_html=True)

def run_strategy(*, strategy: str, candles: pd.DataFrame, capital: float, risk_pct: float, rr_ratio: float, trailing_sl_pct: float, symbol: str, strike_step: int, moneyness: str, strike_steps: int, fetch_option_metrics: bool, mtf_ema_period: int, mtf_setup_mode: str, mtf_retest_strength: bool, mtf_max_trades_per_day: int) -> list[dict[str, object]]:
    candle_rows = _df_to_candles(candles)
    strategy_name = str(strategy or "Breakout").strip()
    risk_fraction = float(risk_pct) / 100.0
    rows: list[dict[str, object]] = []
    if strategy_name == "Breakout":
        rows = generate_breakout_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))
    elif strategy_name == "Demand Supply":
        rows = generate_demand_supply_trades(candles) if generate_demand_supply_trades is not None else []
    elif strategy_name == "Indicator":
        indicator_rows = generate_indicator_rows(candle_rows, config=IndicatorConfig())
        mapped: list[dict[str, object]] = []
        for row in indicator_rows:
            item = dict(row)
            signal = str(item.get("market_signal", "")).upper()
            item["side"] = "BUY" if signal in {"BULLISH_TREND", "OVERSOLD", "BUY", "LONG"} else "SELL" if signal in {"BEARISH_TREND", "OVERBOUGHT", "SELL", "SHORT"} else ""
            item.setdefault("entry_price", item.get("close", item.get("price", 0.0)))
            item.setdefault("timestamp", item.get("timestamp", ""))
            item.setdefault("strategy", "INDICATOR")
            mapped.append(item)
        rows = mapped
    elif strategy_name == "One Trade/Day":
        rows = generate_one_trade_day_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), config=IndicatorConfig(), trailing_sl_pct=float(trailing_sl_pct))
    elif strategy_name == "MTF 5m":
        rows = generate_mtf_trade_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct), ema_period=int(mtf_ema_period), setup_mode=str(mtf_setup_mode), require_retest_strength=bool(mtf_retest_strength), max_trades_per_day=int(mtf_max_trades_per_day))
    else:
        rows = generate_breakout_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))
    normalized: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item.setdefault("strategy", strategy_name.upper().replace(" ", "_"))
        item.setdefault("symbol", symbol)
        item.setdefault("trade_no", idx)
        item.setdefault("trade_label", f"Trade {idx}")
        item.setdefault("entry_time", item.get("timestamp", ""))
        normalized.append(item)
    actionable = [r for r in normalized if str(r.get("side", "")).upper() in {"BUY", "SELL"}]
    if actionable:
        normalized = attach_option_strikes(actionable, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
    return normalized
def _render_sidebar_shell() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 20%),
                radial-gradient(circle at bottom left, rgba(34,197,94,0.10), transparent 22%),
                linear-gradient(180deg, #020617 0%, #07111f 44%, #0b1728 100%);
        }
        [data-testid="stHeader"] {
            background: rgba(2, 6, 23, 0.72);
        }
        [data-testid="stAppViewContainer"] .main .block-container {
            max-width: 1600px;
            padding-top: 1.2rem;
        }
        [data-testid="stAppViewContainer"] [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(15,23,42,0.92), rgba(9,14,26,0.96));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 18px;
            padding: 10px 12px;
            box-shadow: 0 14px 30px rgba(2, 6, 23, 0.22);
        }
        [data-testid="stAppViewContainer"] [data-testid="stDataFrame"] {
            background: rgba(15, 23, 42, 0.7);
            border-radius: 16px;
            border: 1px solid rgba(148, 163, 184, 0.10);
            padding: 4px;
        }
        .hero-strip {
            border-radius: 24px;
            padding: 10px 12px;
            margin: 10px 0 16px 0;
            box-shadow: 0 24px 48px rgba(2, 6, 23, 0.32);
        }
        .hero-strip.hero-bull {
            background: linear-gradient(135deg, rgba(6,78,59,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(34,197,94,0.90) 100%);
            border: 1px solid rgba(74, 222, 128, 0.18);
        }
        .hero-strip.hero-bear {
            background: linear-gradient(135deg, rgba(127,29,29,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(239,68,68,0.90) 100%);
            border: 1px solid rgba(248, 113, 113, 0.18);
        }
        .hero-strip.hero-range {
            background: linear-gradient(135deg, rgba(120,53,15,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(245,158,11,0.88) 100%);
            border: 1px solid rgba(251, 191, 36, 0.18);
        }
        .hero-kicker {
            color: #93c5fd;
            font-size: 11px;
            letter-spacing: 0.2em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }
        .hero-symbol {
            color: #f8fafc;
            font-size: 34px;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 4px;
        }
        .hero-price {
            color: #e0f2fe;
            font-size: 28px;
            font-weight: 700;
        }
        .hero-change {
            font-size: 15px;
            font-weight: 700;
            margin-top: 6px;
        }
        .hero-grid {
            display: grid;
            grid-template-columns: minmax(220px, 1.3fr) repeat(3, minmax(120px, 1fr));
            gap: 12px;
            align-items: stretch;
        }
        .hero-tile {
            background: rgba(15, 23, 42, 0.66);
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 18px;
            padding: 10px 12px;
        }
        .hero-label {
            color: #94a3b8;
            font-size: 11px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }
        .hero-value {
            color: #f8fafc;
            font-size: 18px;
            font-weight: 700;
        }
        @media (max-width: 900px) {
            .hero-grid {
                grid-template-columns: 1fr;
            }
        }
        [data-testid="stTabs"] [role="tablist"] {
            gap: 10px;
            background: rgba(15, 23, 42, 0.56);
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 18px;
            padding: 8px;
            margin-bottom: 14px;
        }
        [data-testid="stTabs"] [role="tab"] {
            background: rgba(15, 23, 42, 0.82);
            color: #cbd5e1;
            border-radius: 14px;
            border: 1px solid rgba(148, 163, 184, 0.10);
            padding: 10px 16px;
            font-weight: 700;
        }
        [data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, rgba(14,165,233,0.95), rgba(34,197,94,0.90));
            color: #04111d;
            border-color: transparent;
            box-shadow: 0 12px 26px rgba(14, 165, 233, 0.20);
        }
        .stButton > button {
            background: linear-gradient(135deg, #0f172a 0%, #162338 100%);
            color: #e2e8f0;
            border: 1px solid rgba(125, 211, 252, 0.16);
            border-radius: 14px;
            font-weight: 700;
            padding: 0.62rem 1rem;
            box-shadow: 0 12px 24px rgba(2, 6, 23, 0.22);
        }
        .stButton > button:hover {
            border-color: rgba(74, 222, 128, 0.28);
            color: #f8fafc;
        }
        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #0ea5e9 0%, #22c55e 100%);
            color: #04111d;
            border-color: transparent;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: linear-gradient(180deg, rgba(14,165,233,0.12), rgba(15,23,42,0.92));
            color: #e2e8f0;
            font-weight: 700;
            border-bottom: 1px solid rgba(125, 211, 252, 0.14);
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            background: rgba(8, 15, 28, 0.78);
            color: #dbeafe;
            border-color: rgba(148, 163, 184, 0.08);
        }
        [data-testid="stExpander"] {
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 18px;
            background: rgba(15, 23, 42, 0.62);
        }
        [data-testid="stVerticalBlock"] [data-testid="stAltairChart"],
        [data-testid="stVerticalBlock"] [data-testid="stDataFrame"] {
            box-shadow: 0 18px 36px rgba(2, 6, 23, 0.22);
        }
        .chart-shell {
            background: linear-gradient(180deg, rgba(15,23,42,0.88), rgba(8,15,28,0.92));
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 20px;
            padding: 12px 14px 6px 14px;
            margin-bottom: 12px;
            box-shadow: 0 20px 40px rgba(2, 6, 23, 0.24);
        }
        .section-shell {
            background: linear-gradient(180deg, rgba(15,23,42,0.82), rgba(8,15,28,0.90));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 22px;
            padding: 16px 18px;
            margin-bottom: 16px;
            box-shadow: 0 20px 38px rgba(2, 6, 23, 0.20);
        }
        .live-panel {
            background: linear-gradient(180deg, rgba(15,23,42,0.92), rgba(8,15,28,0.96));
            border: 1px solid rgba(56, 189, 248, 0.18);
            border-radius: 22px;
            padding: 18px 20px;
            margin-bottom: 12px;
            box-shadow: 0 20px 42px rgba(2, 6, 23, 0.24);
        }
        .live-kicker {
            color: #7dd3fc;
            font-size: 11px;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            margin-bottom: 6px;
            font-weight: 700;
        }
        .live-title {
            color: #f8fafc;
            font-size: 30px;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 8px;
        }
        .live-sub {
            color: #94a3b8;
            font-size: 14px;
            line-height: 1.5;
            max-width: 760px;
        }
        .control-ribbon {
            background: linear-gradient(180deg, rgba(12,32,57,0.90), rgba(9,24,44,0.96));
            border: 1px solid rgba(118, 164, 210, 0.16);
            border-radius: 20px;
            padding: 14px 16px 8px 16px;
            margin-bottom: 14px;
            box-shadow: 0 18px 34px rgba(2, 12, 27, 0.18);
        }
        .control-ribbon-title {
            color: #f8fafc;
            font-size: 18px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        .control-ribbon-copy {
            color: #94a3b8;
            font-size: 13px;
            margin-bottom: 10px;
        }
        [data-testid="stSidebar"] {
            display: none;
            background:
                radial-gradient(circle at top left, rgba(34,197,94,0.14), transparent 24%),
                linear-gradient(180deg, #08111f 0%, #0b1220 48%, #0f172a 100%);
            border-right: 1px solid rgba(148, 163, 184, 0.18);
        }
        [data-testid="stSidebar"] .block-container {
            padding-top: 1rem;
        }
        [data-testid="stSidebar"] .live-panel {
            background: linear-gradient(180deg, rgba(15,23,42,0.96), rgba(8,15,28,0.98));
            border: 1px solid rgba(56, 189, 248, 0.2);
            border-radius: 18px;
            padding: 10px 12px;
            margin-bottom: 12px;
            box-shadow: 0 16px 40px rgba(2, 6, 23, 0.35);
        }
        [data-testid="stSidebar"] .live-kicker {
            color: #7dd3fc;
            font-size: 11px;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            margin-bottom: 4px;
        }
        [data-testid="stSidebar"] .live-title {
            color: #e2e8f0;
            font-size: 24px;
            font-weight: 700;
            line-height: 1.1;
            margin-bottom: 8px;
        }
        [data-testid="stSidebar"] .live-sub {
            color: #94a3b8;
            font-size: 12px;
            line-height: 1.4;
        }
        [data-testid="stSidebar"] .live-section {
            color: #f8fafc;
            font-size: 12px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            margin: 14px 0 6px 0;
        }
        [data-testid="stSidebar"] .stSegmentedControl,
        [data-testid="stSidebar"] .stPills {
            background: rgba(15, 23, 42, 0.72);
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 16px;
            padding: 6px;
        }
        [data-testid="stSidebar"] .stSegmentedControl [role="radiogroup"],
        [data-testid="stSidebar"] .stPills [role="radiogroup"] {
            gap: 8px;
        }
        [data-testid="stSidebar"] .stSegmentedControl label,
        [data-testid="stSidebar"] .stPills label {
            border-radius: 12px !important;
            border: 1px solid rgba(148, 163, 184, 0.14) !important;
            background: rgba(15, 23, 42, 0.95) !important;
            color: #cbd5e1 !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebar"] .stSegmentedControl label[data-selected="true"],
        [data-testid="stSidebar"] .stPills label[data-selected="true"] {
            background: linear-gradient(135deg, #0ea5e9, #22c55e) !important;
            color: #04111d !important;
            border-color: transparent !important;
            box-shadow: 0 10px 24px rgba(14, 165, 233, 0.22);
        }
        [data-testid="stSidebar"] .status-card {
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 14px;
            padding: 10px 12px;
        }
        [data-testid="stSidebar"] .status-label {
            color: #94a3b8;
            font-size: 10px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 4px;
        }
        [data-testid="stSidebar"] .status-value {
            color: #f8fafc;
            font-size: 15px;
            font-weight: 700;
            line-height: 1.2;
        }
        [data-testid="stSidebar"] .status-price {
            color: #4ade80;
        }
        [data-testid="stSidebar"] .stButton button,
        [data-testid="stSidebar"] .stDownloadButton button {
            border-radius: 14px;
        }
        </style>
        """,

        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 20%),
                radial-gradient(circle at bottom left, rgba(34,197,94,0.10), transparent 22%),
                radial-gradient(circle at top right, rgba(255,184,77,0.16), transparent 24%),
                radial-gradient(circle at bottom left, rgba(111,188,255,0.14), transparent 26%),
                linear-gradient(180deg, #04101d 0%, #08192c 44%, #0b1f36 100%);
        }
        [data-testid="stHeader"] {
            background: rgba(4, 17, 31, 0.74);
            border-bottom: 1px solid rgba(118, 164, 210, 0.10);
        }
        [data-testid="stAppViewContainer"] .main .block-container {
            max-width: 1600px;
            max-width: 1480px;
            padding-top: 1.1rem;
        }
        h1, h2, h3, h4, h5, h6, p, label, span, div {
            color: #e5eef8;
        }
        [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(12,32,57,0.94), rgba(9,24,44,0.96)) !important;
            border: 1px solid rgba(118, 164, 210, 0.14) !important;
            border-radius: 18px !important;
            box-shadow: 0 14px 30px rgba(2, 12, 27, 0.22) !important;
        }
        [data-testid="stMetricLabel"] {
            color: #89a7c7 !important;
        }
        [data-testid="stMetricValue"] {
            color: #ffffff !important;
        }
        [data-testid="stTabs"] [role="tablist"] {
            background: rgba(12, 32, 57, 0.82);
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 14px 28px rgba(2, 12, 27, 0.18);
        }
        [data-testid="stTabs"] [role="tab"] {
            background: rgba(255, 255, 255, 0.05);
            color: #d8e6f5;
            border: 1px solid rgba(148, 196, 232, 0.12);
        }
        [data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, #ffb84d 0%, #ff8a2a 100%);
            color: #ffffff;
            box-shadow: none;
        }
        .stButton > button {
            background: linear-gradient(135deg, #0c2039 0%, #12365b 100%);
            color: #eaf4ff;
            border: 1px solid rgba(148, 196, 232, 0.14);
            box-shadow: 0 12px 24px rgba(2, 12, 27, 0.18);
        }
        .stButton > button:hover {
            border-color: rgba(255, 122, 47, 0.36);
            color: #ffffff;
        }
        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #ffb84d 0%, #ff8a2a 100%);
            color: #ffffff;
            border-color: transparent;
        }
        [data-testid="stDataFrame"] {
            background: rgba(12, 32, 57, 0.92) !important;
            border: 1px solid rgba(118, 164, 210, 0.14) !important;
            border-radius: 18px !important;
            box-shadow: 0 16px 30px rgba(2, 12, 27, 0.18) !important;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: rgba(255, 255, 255, 0.06);
            color: #eaf4ff;
            border-bottom: 1px solid rgba(148, 196, 232, 0.12);
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            background: rgba(9, 24, 44, 0.92);
            color: #dbeafe;
        }
        [data-testid="stExpander"] {
            background: rgba(12, 32, 57, 0.88);
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 16px 30px rgba(2, 12, 27, 0.16);
        }
        .section-shell, .chart-shell {
            background: linear-gradient(180deg, rgba(12,32,57,0.94), rgba(9,24,44,0.98));
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 18px 32px rgba(2, 12, 27, 0.20);
        }
        [data-testid="stSidebar"] {
            display: none;
            background: radial-gradient(circle at top left, rgba(255,184,77,0.12), transparent 26%), linear-gradient(180deg, #071524 0%, #0b1d33 100%);
            border-right: 1px solid rgba(118, 164, 210, 0.16);
        }
        [data-testid="stSidebar"] .live-panel,
        [data-testid="stSidebar"] .status-card {
            background: linear-gradient(180deg, rgba(12,32,57,0.96), rgba(9,24,44,0.98));
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 14px 28px rgba(2, 12, 27, 0.22);
        }
        [data-testid="stSidebar"] .live-kicker,
        [data-testid="stSidebar"] .live-sub,
        [data-testid="stSidebar"] .status-label,
        [data-testid="stSidebar"] .live-section {
            color: #89a7c7;
        }
        [data-testid="stSidebar"] .live-title,
        [data-testid="stSidebar"] .status-value {
            color: #ffffff;
        }
        [data-testid="stSidebar"] .status-price {
            color: #7dd3fc;
        }
        [data-testid="stSidebar"] .stSegmentedControl,
        [data-testid="stSidebar"] .stPills {
            background: rgba(12, 32, 57, 0.82);
            border: 1px solid rgba(118, 164, 210, 0.14);
        }
        [data-testid="stSidebar"] .stSegmentedControl label,
        [data-testid="stSidebar"] .stPills label {
            background: rgba(255, 255, 255, 0.05) !important;
            color: #d8e6f5 !important;
            border: 1px solid rgba(148, 196, 232, 0.12) !important;
        }
        [data-testid="stSidebar"] .stSegmentedControl label[data-selected="true"],
        [data-testid="stSidebar"] .stPills label[data-selected="true"] {
            background: linear-gradient(135deg, #ff7a2f 0%, #ff5b21 100%) !important;
            color: #ffffff !important;
            box-shadow: none;
        }
                .page-masthead {
            display: block;
            background: linear-gradient(180deg, #050505 0%, #090909 100%);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 0;
            padding: 24px 28px 30px 28px;
            margin-bottom: 14px;
            box-shadow: 0 24px 50px rgba(0, 0, 0, 0.34);
            overflow: hidden;
            position: relative;
        }
        .page-masthead::before,
        .page-masthead::after {
            display: none;
        }        .top-nav {
            position: relative;
            z-index: 3;
            display: grid;
            grid-template-columns: auto 1fr auto auto auto;
            align-items: center;
            gap: 18px;
            border: none;
            border-radius: 0;
            padding: 8px 0 18px 0;
            margin-bottom: 14px;
            backdrop-filter: none;
        }
        .top-nav-brand {
            display: inline-flex;
            align-items: center;
            justify-content: flex-start;
            gap: 10px;
            color: #ffffff;
            font-size: 16px;
            font-weight: 800;
            letter-spacing: 0;
            white-space: nowrap;
        }
        .top-nav-logo {
            width: 28px;
            height: 28px;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: #1fba81;
            color: #ffffff;
            font-size: 13px;
            font-weight: 900;
            box-shadow: none;
        }
        .top-nav-brand span {
            color: #ffffff;
        }
        .top-nav-menu {
            display: flex;
            align-items: center;
            gap: 14px;
            flex-wrap: wrap;
            color: rgba(255,255,255,0.72);
            font-size: 15px;
            font-weight: 700;
            position: relative;
        }
        .top-nav-pill {
            min-width: 0;
            padding: 10px 12px;
            border-radius: 12px;
            background: transparent;
            border: 1px solid transparent;
            color: rgba(255,255,255,0.78);
            font-size: 15px;
            font-weight: 700;
            line-height: 1.1;
        }
        .top-nav-pill.active-nav {
            color: #ffffff;
        }
        .nav-dropdown-shell {
            position: relative;
            display: inline-flex;
            align-items: center;
            padding-bottom: 26px;
            margin-bottom: -26px;
        }
        .nav-dropdown-trigger {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            min-height: 42px;
            cursor: default;
        }
        .nav-dropdown-caret {
            color: rgba(255,255,255,0.78);
            font-size: 13px;
            transform: translateY(-1px);
        }
        .top-nav-pill .active,
        .top-nav-pill-value {
            color: inherit;
            display: inline;
            font-size: inherit;
            font-weight: inherit;
            margin-top: 0;
        }
        .top-nav-search {
            border-radius: 14px;
            background: #171717;
            border: 1px solid rgba(255,255,255,0.10);
            color: rgba(255,255,255,0.72);
            padding: 13px 18px;
            font-size: 14px;
            font-weight: 700;
            min-height: 42px;
            display: inline-flex;
            align-items: center;
            min-width: 250px;
        }
        .top-nav-secondary {
            border-radius: 14px;
            border: 1px solid rgba(255,255,255,0.16);
            color: #ffffff;
            background: rgba(255,255,255,0.02);
            padding: 13px 22px;
            font-size: 14px;
            font-weight: 700;
            min-height: 42px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }
        .top-nav-cta {
            background: #f7a600;
            color: #101010;
            border-radius: 14px;
            padding: 13px 22px;
            font-size: 14px;
            font-weight: 800;
            letter-spacing: 0;
            min-height: 42px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
        }
        .mega-panel {
            position: absolute;
            z-index: 8;
            top: calc(100% + 14px);
            left: -8px;
            width: min(820px, 70vw);
            max-height: 68vh;
            overflow-y: auto;
            overflow-x: hidden;
            background: #ffffff;
            color: #111827;
            border-radius: 22px;
            padding: 28px 30px;
            box-shadow: 0 28px 50px rgba(0,0,0,0.28);
            opacity: 0;
            visibility: hidden;
            transform: translateY(12px);
            transition: opacity 180ms ease, transform 180ms ease, visibility 180ms ease;
            pointer-events: none;
            scrollbar-width: thin;
            scrollbar-color: rgba(17,24,39,0.35) transparent;
        }
        .mega-panel::before {
            content: "";
            position: absolute;
            top: -14px;
            left: 190px;
            width: 28px;
            height: 28px;
            background: #ffffff;
            transform: rotate(45deg);
            border-radius: 4px;
        }
        .mega-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 28px 34px;
        }
        .mega-item {
            display: grid;
            grid-template-columns: 36px 1fr;
            gap: 16px;
            align-items: start;
        }
        .mega-icon {
            width: 36px;
            height: 36px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 15px;
            font-weight: 800;
            color: #ffffff;
            background: linear-gradient(135deg, #14b87a 0%, #0f9f6c 100%);
        }
        .mega-icon.alt {
            background: linear-gradient(135deg, #7c4dff 0%, #a46dff 100%);
        }
        .mega-icon.dark {
            background: linear-gradient(135deg, #2d3748 0%, #111827 100%);
        }
        .mega-icon.blue {
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        }
        .mega-title {
            color: #121826;
            font-size: 18px;
            font-weight: 800;
            margin-bottom: 4px;
        }
        .markets-panel {
            width: min(1000px, 78vw);
            left: -120px;
            padding: 0;
            overflow: hidden;
        }
        .markets-panel::before {
            left: 320px;
        }
        .markets-tabs {
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
            padding: 16px 24px;
            border-bottom: 1px solid #e5e7eb;
            background: #ffffff;
        }
        .markets-tab {
            padding: 10px 18px;
            border-radius: 10px;
            border: 1px solid #d1d5db;
            color: #4b5563;
            font-size: 14px;
            font-weight: 700;
            background: #ffffff;
        }
        .markets-tab.active {
            background: #f7a600;
            border-color: #f7a600;
            color: #ffffff;
        }
        .markets-body {
            display: grid;
            grid-template-columns: 260px 1fr;
            min-height: 320px;
        }
        .markets-side {
            border-right: 1px solid #e5e7eb;
            background: #fbfbfb;
            padding: 0;
        }
        .markets-side-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 24px;
            font-size: 15px;
            font-weight: 700;
            color: #9ca3af;
        }
        .markets-side-item.active {
            color: #f59e0b;
            background: #fff7ed;
        }
        .markets-arrow {
            color: #f59e0b;
            font-size: 18px;
        }
        .markets-list-wrap {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 26px 48px;
            padding: 26px 30px;
            align-content: start;
        }
        .market-link {
            display: flex;
            align-items: center;
            gap: 12px;
            color: #1f2937;
            font-size: 17px;
            font-weight: 700;
        }
        .market-dot {
            width: 22px;
            height: 22px;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            color: #ffffff;
            font-size: 11px;
            font-weight: 800;
            background: linear-gradient(135deg, #ef4444 0%, #f59e0b 100%);
        }
        .market-dot.blue {
        .more-panel {
            width: min(820px, 72vw);
            left: -180px;
        }
        .more-panel::before {
        .investments-panel {
            width: min(900px, 74vw);
            left: -80px;
        }
        .investments-panel::before {
            left: 250px;
        }
            left: 210px;
        }
            background: linear-gradient(135deg, #38bdf8 0%, #2563eb 100%);
        }
        .mega-copy {
            color: #6b7280;
            font-size: 14px;
            line-height: 1.55;
        }
        .masthead-grid {
            position: relative;
            z-index: 2;
            display: grid;
            grid-template-columns: minmax(420px, 1.05fr) minmax(380px, 0.95fr);
            gap: 32px;
            align-items: end;
            padding: 8px 8px 0 8px;
        }
        .breadcrumb-list {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            align-items: center;
            margin-bottom: 12px;
            color: rgba(255,255,255,0.72);
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .breadcrumb-sep {
            color: rgba(255,255,255,0.34);
        }
        .breadcrumb-current {
            color: #ffffff;
        }
        .page-eyebrow {
            color: #88bfff;
            font-size: 11px;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            margin-bottom: 10px;
        }
        .page-title {
            color: #ffffff;
            font-size: 74px;
            font-weight: 900;
            line-height: 0.94;
            margin: 0;
            max-width: 620px;
        }
        .page-title .accent {
            color: #ffffff;
        }
        .page-subtitle {
            color: #dbe7f8;
            font-size: 19px;
            line-height: 1.65;
            margin-top: 22px;
            max-width: 560px;
        }
        .page-badge {
            position: absolute;
            right: 22px;
            bottom: 22px;
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 18px;
            color: #111827;
            font-size: 13px;
            font-weight: 700;
            padding: 16px 18px;
            box-shadow: none;
        }
        .masthead-pills {
            display: none;
        }
        .hero-search {
            margin-top: 24px;
            max-width: 520px;
            border-radius: 18px;
            background: #ffffff;
            color: #4b5563;
            padding: 16px 20px;
            font-size: 16px;
            font-weight: 700;
            box-shadow: none;
            border: 1px solid rgba(0,0,0,0.08);
        }
        .hero-chip-row {
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
            margin-top: 22px;
        }
        .hero-chip {
            min-width: 132px;
            border-radius: 16px;
            padding: 14px 16px;
            background: rgba(255, 255, 255, 0.06);
            border: 1px solid rgba(255,255,255,0.10);
            box-shadow: none;
        }
        .hero-chip-label {
            color: #ffffff;
            font-size: 13px;
            font-weight: 800;
        }
        .hero-chip-meta {
            color: #89a7c7;
            font-size: 12px;
            margin-top: 4px;
        }
        .hero-visual {
            position: relative;
            min-height: 430px;
            border-radius: 24px;
            background: radial-gradient(circle at 18% 18%, rgba(76, 180, 255, 0.20), transparent 22%), linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
            border: 1px solid rgba(255,255,255,0.08);
            overflow: hidden;
            padding: 28px;
        }
        .hero-visual::before {
            content: "";
            position: absolute;
            inset: auto 22px 26px 22px;
            height: 132px;
            border-radius: 18px;
            background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
            border: 1px solid rgba(255,255,255,0.06);
        }
        .hero-visual::after {
            content: "KRSH SOLUTIONS";
            position: absolute;
            top: 28px;
            left: 28px;
            color: rgba(255,255,255,0.92);
            font-size: 30px;
            font-weight: 800;
            letter-spacing: 0.02em;
        }
        .hero-orb {
            position: absolute;
            border-radius: 999px;
            filter: blur(10px);
            opacity: 0.85;
        }
        .hero-orb-one {
            width: 160px;
            height: 160px;
            right: 40px;
            top: 54px;
            background: radial-gradient(circle, rgba(255,193,92,0.85) 0%, rgba(255,193,92,0.08) 72%);
        }
        .hero-orb-two {
            width: 190px;
            height: 190px;
            right: 110px;
            bottom: 90px;
            background: radial-gradient(circle, rgba(53,168,255,0.55) 0%, rgba(53,168,255,0.06) 72%);
        }
        .hero-curve {
            position: absolute;
            border-top: 3px solid;
            border-radius: 999px;
            opacity: 0.95;
        }
        .hero-curve-one {
            right: -8px;
            top: 110px;
            width: 340px;
            height: 180px;
            border-color: rgba(43, 170, 255, 0.72);
            transform: rotate(-4deg);
        }
        .hero-curve-two {
            right: 34px;
            top: 150px;
            width: 300px;
            height: 170px;
            border-color: rgba(102, 222, 255, 0.44);
            transform: rotate(8deg);
        }
        .hero-curve-three {
            right: 76px;
            top: 208px;
            width: 240px;
            height: 120px;
            border-color: rgba(110, 103, 255, 0.45);
            transform: rotate(-12deg);
        }
        .hero-preview-card {
            position: absolute;
            left: 28px;
            right: 28px;
            bottom: 28px;
            z-index: 2;
            display: grid;
            grid-template-columns: 1.2fr 0.8fr;
            gap: 16px;
            align-items: center;
            padding: 18px 20px;
            background: linear-gradient(180deg, rgba(19,19,19,0.90), rgba(19,19,19,0.82));
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 18px;
        }
        .hero-preview-copy {
            color: rgba(255,255,255,0.78);
            font-size: 14px;
            line-height: 1.6;
            max-width: 290px;
        }
        .hero-preview-title {
            color: #ffffff;
            font-size: 18px;
            font-weight: 800;
            margin-bottom: 6px;
        }
        .hero-preview-metrics {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 10px;
        }
        .hero-preview-metric {
            border-radius: 14px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.08);
            padding: 12px 14px;
        }
        .hero-preview-label {
            color: rgba(255,255,255,0.62);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .hero-preview-value {
            color: #ffffff;
            font-size: 15px;
            font-weight: 800;
            margin-top: 4px;
        }
        .hero-strip,
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 20px;
            box-shadow: none;
        }
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            border-left: 4px solid #f7a600;
        }
        .hero-kicker, .hero-label {
            color: #6b7280;
        }
        .hero-symbol, .hero-value, .hero-price {
            color: #111827;
        }
        .hero-tile {
            background: #f8fafc;
            border: 1px solid rgba(0,0,0,0.06);
        }
        .hero-kicker, .hero-label {
            color: #89a7c7;
        }
        .hero-symbol, .hero-value, .hero-price {
            color: #ffffff;
        }
        .hero-tile {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(148, 196, 232, 0.14);
        }
        .section-heading {
            color: #ffffff;
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 2px;
        }
        .section-copy {
            color: #89a7c7;
            font-size: 13px;
            margin-bottom: 14px;
        }
        @media (max-width: 900px) {
            .masthead-grid {
                grid-template-columns: 1fr;
            }
            .mega-panel {
                position: static;
                width: 100%;
                opacity: 1;
                visibility: visible;
                transform: none;
                pointer-events: auto;
                margin-top: 16px;
            }
            .masthead-pills {
                grid-template-columns: repeat(2, minmax(120px, 1fr));
            }
            .page-title {
                font-size: 40px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
def _render_page_masthead(
    symbol: str,
    strategy: str,
    execution_mode: str,
    auto_execute: bool,
    *,
    workspace: str,
    content_view: str,
    interval: str,
    period: str,
    instrument_mode: str,
    lots: int,
    lot_size: int,
    risk_pct: float,
    rr_ratio: float,
    last_signal_side: str,
    open_trades: int,
    account_status: str,
) -> None:
    auto_text = "Auto execute ready" if auto_execute else "Review before send"
    signal_text = str(last_signal_side or "-").upper()
    order_text = f"{instrument_mode} {int(lots)} x {int(lot_size)}"
    risk_text = f"{float(risk_pct):.1f}% risk / {float(rr_ratio):.1f}R"
    st.markdown(
        f"""
        <div class="page-masthead">
            <div class="top-nav">
                <div class="top-nav-brand"><div class="top-nav-logo">K</div><div>KRSH<span> Solutions</span></div></div>
                <div class="top-nav-menu">
                    <div class="nav-dropdown-shell">
                        <div class="top-nav-pill nav-dropdown-trigger"><span class="active">Products</span><span class="nav-dropdown-caret">v</span></div>
                        <div class="mega-panel">
                            <div class="mega-grid">
                                <div class="mega-item"><div class="mega-icon">K</div><div><div class="mega-title">KRSH App</div><div class="mega-copy">Strategy-led trading app built for active desk users and disciplined execution workflows.</div></div></div>
                                <div class="mega-item"><div class="mega-icon dark">W</div><div><div class="mega-title">KRSH Web</div><div class="mega-copy">Web trading platform for users who prefer a bigger live trading screen.</div></div></div>
                                <div class="mega-item"><div class="mega-icon alt">O</div><div><div class="mega-title">Options Trader</div><div class="mega-copy">Current instrument mode: {instrument_mode}. Built to evaluate F&O setups with clear sizing.</div></div></div>
                                <div class="mega-item"><div class="mega-icon">S</div><div><div class="mega-title">Strategy Selection</div><div class="mega-copy">Active strategy: {strategy}. Market read: {symbol} on {interval} / {period} with {execution_mode} workflow.</div><div class="mega-pill-row"><span class="mega-pill active">{strategy}</span><span class="mega-pill">Breakout</span><span class="mega-pill">Demand Supply</span><span class="mega-pill">Indicator</span><span class="mega-pill">One Trade/Day</span><span class="mega-pill">MTF 5m</span></div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">T</div><div><div class="mega-title">Connect to TradingView</div><div class="mega-copy">Review {symbol} on {interval}, then route orders with {execution_mode} mode and {account_status} broker status.</div></div></div>
                                <div class="mega-item"><div class="mega-icon dark">S</div><div><div class="mega-title">Signal Engine</div><div class="mega-copy">Latest signal state: {signal_text}. Open setups available: {open_trades}. Strategy: {strategy}.</div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">D</div><div><div class="mega-title">Desk Tabs</div><div class="mega-copy">Keep the main page simple. The page selectors live in one compact row under the hero and the current section stays on {content_view}.</div><div class="mega-pill-row"><span class="mega-pill active">{workspace}</span><span class="mega-pill">{strategy}</span><span class="mega-pill">{content_view}</span><span class="mega-pill">{execution_mode}</span></div></div></div>
                            </div>
                        </div>
                    </div>
                    <div class="nav-dropdown-shell">
                        <div class="top-nav-pill nav-dropdown-trigger"><span>Investments</span><span class="nav-dropdown-caret">v</span></div>
                        <div class="mega-panel investments-panel">
                            <div class="mega-grid">
                                <div class="mega-item"><div class="mega-icon">S</div><div><div class="mega-title">Stocks</div><div class="mega-copy">Equity-focused market access, delivery review, and active desk monitoring for listed names.</div></div></div>
                                <div class="mega-item"><div class="mega-icon alt">M</div><div><div class="mega-title">Mutual Funds</div><div class="mega-copy">Longer-horizon investment views, allocation tracking, and fund-side monitoring from the same workspace.</div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">E</div><div><div class="mega-title">ETFs</div><div class="mega-copy">Track index-linked instruments and broader market exposure alongside live trade dashboards.</div></div></div>
                                <div class="mega-item"><div class="mega-icon dark">G</div><div><div class="mega-title">Gold & Silver</div><div class="mega-copy">Commodity-linked investment tracking with cleaner visibility into non-equity exposure.</div></div></div>
                                <div class="mega-item"><div class="mega-icon">I</div><div><div class="mega-title">IPO Watch</div><div class="mega-copy">Follow new listings, allocation status, and pre-open interest in one place.</div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">C</div><div><div class="mega-title">Capital View</div><div class="mega-copy">See how capital, risk percentage, and deployment fit alongside your active trading flow.</div></div></div>
                            </div>
                        </div>
                    </div>
                    <div class="top-nav-pill"><span>Markets</span></div>
                    <div class="nav-dropdown-shell">
                        <div class="top-nav-pill nav-dropdown-trigger active-nav"><span class="active">More</span><span class="nav-dropdown-caret">^</span></div>
                        <div class="mega-panel more-panel">
                            <div class="mega-grid">
                                <div class="mega-item"><div class="mega-icon alt">P</div><div><div class="mega-title">Pricing</div><div class="mega-copy">Open free demat style onboarding, broker access details, and account-readiness information for your desk.</div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">B</div><div><div class="mega-title">Become a Partner</div><div class="mega-copy">Extend KRSH Solutions with partner workflows, integrations, and shared execution operations.</div></div></div>
                                <div class="mega-item"><div class="mega-icon">F</div><div><div class="mega-title">fuzz</div><div class="mega-copy">Ask strategy, finance, and market questions inside your research-led trading workspace.</div></div></div>
                                <div class="mega-item"><div class="mega-icon dark">S</div><div><div class="mega-title">Dhan Support</div><div class="mega-copy">Browse setup help for broker routing, paper/live mode behavior, and execution readiness.</div></div></div>
                                <div class="mega-item"><div class="mega-icon alt">B</div><div><div class="mega-title">KRSH Blog</div><div class="mega-copy">Read notes on markets, trading strategies, execution discipline, and desk improvements.</div></div></div>
                                <div class="mega-item"><div class="mega-icon">I</div><div><div class="mega-title">Indicator by KRSH</div><div class="mega-copy">Review indicator-led market context, insights, and deeper setup visibility.</div></div></div>
                                <div class="mega-item"><div class="mega-icon blue">C</div><div><div class="mega-title">MadeForTrade Community</div><div class="mega-copy">Stay connected to the active trade review workflow and collaborative execution queue.</div></div></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="top-nav-search">Search Stocks, Mutual Funds, F&O</div>
                <div class="top-nav-secondary">Login</div>
                <div class="top-nav-cta">Open Account</div>
            </div>
            <div class="masthead-grid">
                <div>
                    <div class="breadcrumb-list"><span>KRSH Solutions</span><span class="breadcrumb-sep">/</span><span>TradingView Connect</span><span class="breadcrumb-sep">/</span><span>{symbol}</span><span class="breadcrumb-sep">/</span><span class="breadcrumb-current">{strategy}</span></div>
                    <div class="page-eyebrow">KRSH Solutions Connect Workspace</div>
                    <h1 class="page-title">Trade Directly from<br><span class="accent">TradingView.com</span></h1>
                    <div class="page-subtitle">Available exclusively for KRSH Solutions users. Review live setups, connect broker routing, and execute directly from your trading workspace.</div>
                    <div class="hero-search">Connect to trading workflow  |  {symbol}  |  {interval}  |  {strategy}</div>
                </div>
                <div class="hero-visual">
                    <div class="hero-orb hero-orb-one"></div>
                    <div class="hero-orb hero-orb-two"></div>
                    <div class="hero-curve hero-curve-one"></div>
                    <div class="hero-curve hero-curve-two"></div>
                    <div class="hero-curve hero-curve-three"></div>

