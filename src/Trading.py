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



st.set_page_config(page_title="KRSH", page_icon="chart", layout="wide")

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
            background: linear-gradient(180deg, rgba(15,23,42,0.76), rgba(8,15,28,0.84));
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 20px;
            padding: 10px 12px;
            margin-bottom: 14px;
            box-shadow: 0 18px 34px rgba(2, 6, 23, 0.18);
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
            background:
                linear-gradient(90deg, rgba(4, 12, 24, 0.96) 0%, rgba(10, 31, 53, 0.92) 42%, rgba(28, 54, 88, 0.82) 100%),
                radial-gradient(circle at 72% 22%, rgba(122, 214, 255, 0.18), transparent 24%),
                radial-gradient(circle at 82% 68%, rgba(122, 214, 255, 0.12), transparent 22%);
            border: 1px solid rgba(118, 164, 210, 0.14);
            border-radius: 26px;
            padding: 24px 24px 18px 24px;
            margin-bottom: 10px;
            box-shadow: 0 26px 54px rgba(2, 12, 27, 0.34);
            overflow: hidden;
            position: relative;
        }
        .page-masthead::after {
            content: "";
            position: absolute;
            inset: auto -90px -60px auto;
            width: 320px;
            height: 320px;
            border-radius: 999px;
            border: 2px solid rgba(125, 211, 252, 0.16);
            opacity: 0.6;
        }
        .top-nav {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            background: linear-gradient(90deg, #091a2d 0%, #123052 52%, #1a446f 100%);
            border: 1px solid rgba(120, 166, 214, 0.18);
            border-radius: 18px;
            padding: 12px 18px;
            margin-bottom: 12px;
            box-shadow: 0 18px 34px rgba(3, 18, 35, 0.28);
        }
        .top-nav-brand {
            color: #ffffff;
            font-size: 22px;
            font-weight: 800;
            letter-spacing: 0.03em;
        }
        .top-nav-brand span {
            color: #ffb84d;
        }
        .top-nav-menu {
            display: flex;
            gap: 18px;
            align-items: center;
            flex-wrap: wrap;
            color: #d8e6f5;
            font-size: 13px;
            font-weight: 600;
        }
        .top-nav-menu .active {
            color: #ffb84d;
        }
        .top-nav-cta {
            background: linear-gradient(135deg, #ffb84d 0%, #ff8a2a 100%);
            color: #ffffff;
            border-radius: 10px;
            padding: 10px 14px;
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .masthead-grid {
            display: grid;
            grid-template-columns: minmax(320px, 1.35fr) minmax(240px, 0.75fr);
            gap: 18px;
            align-items: end;
        }
        .page-eyebrow {
            color: #7dd3fc;
            font-size: 11px;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            margin-bottom: 8px;
        }
        .page-title {
            color: #ffffff;
            font-size: 42px;
            font-weight: 800;
            line-height: 1.03;
            margin: 0;
            max-width: 640px;
        }
        .page-title .accent {
            color: #ffb84d;
        }
        .page-subtitle {
            color: #d5e5f6;
            font-size: 15px;
            line-height: 1.65;
            margin-top: 14px;
            max-width: 620px;
        }
        .page-badge {
            background: rgba(255, 255, 255, 0.08);
            border: 1px solid rgba(148, 196, 232, 0.20);
            border-radius: 18px;
            color: #eaf4ff;
            font-size: 13px;
            font-weight: 700;
            padding: 18px 20px;
            white-space: normal;
            box-shadow: 0 12px 26px rgba(2, 12, 27, 0.24);
            backdrop-filter: blur(8px);
        }
        .masthead-pills {
            display: grid;
            grid-template-columns: repeat(4, minmax(120px, 1fr));
            gap: 12px;
            margin-top: 14px;
        }
        .masthead-pill {
            background: linear-gradient(180deg, rgba(214, 231, 255, 0.16), rgba(165, 192, 229, 0.10));
            border: 1px solid rgba(148, 196, 232, 0.18);
            border-radius: 14px;
            color: #e7f1fb;
            padding: 12px 14px;
            font-size: 12px;
            font-weight: 700;
            text-align: center;
            backdrop-filter: blur(6px);
        }
        .hero-strip,
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            background: linear-gradient(180deg, #0c2039 0%, #102b49 100%);
            border: 1px solid rgba(118, 164, 210, 0.14);
            border-radius: 22px;
            box-shadow: 0 20px 40px rgba(2, 12, 27, 0.26);
        }
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            border-left: 4px solid #ff6b2c;
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
    auto_text = "Auto Send On" if auto_execute else "Manual Review"
    mode_badge = "Live Broker Mode" if str(execution_mode).upper() == "LIVE" else "Paper Desk Mode"
    signal_text = str(last_signal_side or "-").upper()
    order_text = f"{instrument_mode} {int(lots)} x {int(lot_size)}"
    risk_text = f"{float(risk_pct):.1f}% / {float(rr_ratio):.1f}R"
    st.markdown(
        f"""
        <div class="top-nav">
            <div class="top-nav-brand">KRSH<span>TRADE</span></div>
            <div class="top-nav-menu">
                <span class="active">Live Desk: {symbol} {interval} {period}</span>
                <span>Signals: {signal_text} / {open_trades}</span>
                <span>Orders: {order_text}</span>
                <span>Risk: {risk_text}</span>
                <span>Account: {account_status}</span>
            </div>
            <div class="top-nav-cta">{execution_mode}</div>
        </div>
        <div class="page-masthead">
            <div class="masthead-grid">
                <div>
                    <div class="page-eyebrow">KRSH Broker Workspace</div>
                    <h1 class="page-title">KRSH <span class="accent">Live Trading</span> Desk</h1>
                    <div class="page-subtitle">KRSH combines live market structure, order preparation, and Dhan execution for {symbol} in one modern trading workspace.</div>
                </div>
                <div class="page-badge">{strategy}<br/>{mode_badge}<br/>{auto_text}</div>
            </div>
            <div class="masthead-pills">
                <div class="masthead-pill">Payload Preview</div>
                <div class="masthead-pill">Live Routing</div>
                <div class="masthead-pill">{account_status}</div>
                <div class="masthead-pill">{order_text}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _fmt_num(val: object) -> str:
    if val is None:
        return "-"
    text = str(val).strip()
    if text in {"", "-", "N/A"}:
        return "-"
    try:
        num = float(text)
    except Exception:
        return text
    out = f"{num:.2f}"
    return out[:-3] if out.endswith(".00") else out

def _render_hero_strip(
    symbol: str,
    last_price: object,
    day_change: float,
    strategy: str,
    execution_mode: str,
    open_trades: int,
    support_band: str,
    resistance_band: str,
    option_bias: str,
    market_status: str,
) -> None:
    price_text = _fmt_num(last_price)
    change_color = "#4ade80" if day_change >= 0 else "#f87171"
    change_prefix = "+" if day_change > 0 else ""
    status_text = "ACTIVE" if open_trades > 0 else "STANDBY"
    market_upper = str(market_status or "").upper()
    if "BREAKOUT" in market_upper or "LIVE BUY" in market_upper:
        hero_tone = "hero-bull"
    elif "BREAKDOWN" in market_upper or "LIVE SELL" in market_upper:
        hero_tone = "hero-bear"
    else:
        hero_tone = "hero-range"
    st.markdown(
        f"""
        <div class="hero-strip {hero_tone}">
            <div class="hero-grid">
                <div>
                    <div class="hero-kicker">Market Snapshot</div>
                    <div class="hero-symbol">{symbol}</div>
                    <div class="hero-price">Spot LTP {price_text}</div>
                    <div class="hero-change" style="color:{change_color};">{change_prefix}{day_change:.2f} vs previous close</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Strategy</div>
                    <div class="hero-value">{strategy}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Execution</div>
                    <div class="hero-value">{execution_mode}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Status</div>
                    <div class="hero-value">{market_status}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Support</div>
                    <div class="hero-value">{support_band}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Resistance</div>
                    <div class="hero-value">{resistance_band}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">CE / PE Bias</div>
                    <div class="hero-value">{option_bias}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Open Trades</div>
                    <div class="hero-value">{status_text} / {open_trades}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



def _sidebar_section(title: str, subtitle: str = "") -> None:
    text = f'<div class="live-section">{title}</div>'
    if subtitle:
        text += f'<div class="live-sub" style="margin-bottom:8px;">{subtitle}</div>'
    st.markdown(text, unsafe_allow_html=True)




def _render_page_footer() -> None:
    st.markdown(
        """
        <div style="margin-top:18px;padding:14px 18px;border-radius:18px;background:linear-gradient(90deg, rgba(8,18,32,0.96), rgba(18,48,82,0.92));border:1px solid rgba(118,164,210,0.16);display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;">
            <div style="font-size:18px;font-weight:800;color:#ffffff;letter-spacing:0.04em;">KRSH <span style="color:#ffb84d;">TRADE</span></div>
            <div style="color:#9bb6d3;font-size:12px;">Modern live-trading workspace for strategy review, payload preview, and broker execution.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def main() -> None:
    _render_sidebar_shell()
    st.markdown(
        """
        <div class="live-panel" style="margin-bottom:12px;">
            <div class="live-kicker">Execution Console</div>
            <div class="live-title">Dhan Live Desk</div>
            <div class="live-sub">Main-page workspace for routing, strategy analysis, and live execution controls.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    workspace = st.segmented_control(
        "Workspace",
        ["Desk", "Breakout", "Demand Supply", "Indicator", "One Trade/Day", "MTF 5m"],
        default="Desk",
    )
    strategy = "Breakout"
    if workspace == "Desk":
        strategy = st.segmented_control(
            "Active strategy",
            ["Breakout", "Demand Supply", "Indicator", "One Trade/Day", "MTF 5m"],
            default="Breakout",
        )
    else:
        strategy = str(workspace)

    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">Trading Controls</div><div class="section-copy">All required inputs are on the main page. Strategy-specific inputs appear only for the selected workspace.</div>', unsafe_allow_html=True)

    row1 = st.columns([1.25, 1, 1, 1, 1])
    with row1[0]:
        symbol = st.text_input("Symbol", "^NSEI")
    with row1[1]:
        interval = st.segmented_control("Interval", ["1m", "5m", "15m", "30m", "1h"], default="1m")
    with row1[2]:
        period = st.segmented_control("Period", ["1d", "5d", "1mo", "3mo"], default="1d")
    with row1[3]:
        execution_mode = st.segmented_control("Execution mode", ["PAPER", "LIVE"], default="PAPER")
    with row1[4]:
        instrument_mode = st.segmented_control("Instrument", ["Options", "Futures"], default="Options")

    row2 = st.columns([1, 1, 1, 1, 1, 1, 1])
    with row2[0]:
        lot_size = st.number_input("Lot size", min_value=1, value=65, step=1)
    with row2[1]:
        lots = st.slider("Lots", 1, 10, 2)
    with row2[2]:
        capital = st.number_input("Capital (INR)", min_value=1000, value=100000, step=1000)
    with row2[3]:
        risk_pct = st.slider("Risk per trade (%)", 0.1, 10.0, 1.0)
    with row2[4]:
        rr_ratio = st.slider("Risk / Reward", 1.0, 10.0, 2.0)
    with row2[5]:
        trailing_sl_pct = st.slider("Trailing stop loss %", 0.1, 10.0, 1.0, 0.1)
    with row2[6]:
        auto_execute_generated = st.toggle("Auto execute", value=False)

    strike_step = 50
    moneyness = "ATM"
    strike_steps = 0
    fetch_option_metrics = False
    if instrument_mode == "Options":
        st.markdown('<div class="section-copy" style="margin-top:8px;">Option contract controls</div>', unsafe_allow_html=True)
        option_cols = st.columns([1, 1, 1, 1])
        with option_cols[0]:
            strike_step = int(st.segmented_control("Strike step", [25, 50, 100], default=50))
        with option_cols[1]:
            moneyness = st.segmented_control("Moneyness", ["ATM", "ITM", "OTM"], default="ATM")
        with option_cols[2]:
            strike_steps = st.slider("ITM / OTM steps", 0, 5, 0)
        with option_cols[3]:
            fetch_option_metrics = st.checkbox("Fetch option chain metrics", value=False)
    else:
        st.caption("Futures mode uses the monthly futures contract automatically.")

    mtf_ema_period = 3
    mtf_setup_mode = "either"
    mtf_retest_strength = True
    mtf_max_trades_per_day = 3
    if strategy == "MTF 5m":
        st.markdown('<div class="section-copy" style="margin-top:8px;">MTF 5m controls</div>', unsafe_allow_html=True)
        mtf_cols = st.columns([1, 1, 1, 1])
        with mtf_cols[0]:
            mtf_ema_period = int(st.number_input("EMA period (1h)", min_value=2, max_value=20, value=3, step=1))
        with mtf_cols[1]:
            mtf_setup_label = st.segmented_control("15m setup filter", ["Either", "BOS only", "FVG only"], default="Either")
            mtf_setup_mode = {"Either": "either", "BOS only": "bos", "FVG only": "fvg"}[str(mtf_setup_label)]
        with mtf_cols[2]:
            mtf_retest_strength = st.checkbox("Require strong 5m retest candle", value=True)
        with mtf_cols[3]:
            mtf_max_trades_per_day = int(st.segmented_control("Max trades/day", [1, 2, 3], default=3))

    live_update = False
    refresh_seconds = 10
    send_telegram = False
    paper_log_output = "data/paper_trading_logs_all.csv"
    live_log_output = "data/live_trading_logs_all.csv"
    with st.expander("Advanced execution controls", expanded=False):
        adv_cols = st.columns([1, 1, 1, 1])
        with adv_cols[0]:
            live_update = st.checkbox("Auto refresh", value=False)
        with adv_cols[1]:
            refresh_seconds = st.slider("Refresh every (seconds)", 2, 120, 10)
        with adv_cols[2]:
            send_telegram = st.checkbox("Send Telegram alert", value=False)
        with adv_cols[3]:
            st.caption("Use the main-page Auto execute toggle above.")

    dhan_client_id = ""
    st.markdown('<div class="section-copy" style="margin-top:8px;">Trade integration</div>', unsafe_allow_html=True)
    if execution_mode == "PAPER":
        paper_cols = st.columns([1.6, 1, 1])
        with paper_cols[0]:
            paper_log_output = st.text_input("Paper trade log path", value="data/paper_trading_logs_all.csv")
        with paper_cols[1]:
            st.info("Execution type: simulated")
        with paper_cols[2]:
            st.info("Broker: disabled")
    dhan_token_present = False
    dhan_security_map_path = "data/dhan_security_map.csv"
    if execution_mode == "LIVE":
        live_cols = st.columns([1.3, 1.3, 1])
        with live_cols[0]:
            live_log_output = st.text_input("Live trade log path", value="data/live_trading_logs_all.csv")
        with live_cols[1]:
            dhan_security_map_path = st.text_input("Security map path", value="data/dhan_security_map.csv")
        with live_cols[2]:
            st.info("Broker: Dhan")
        st.markdown('<div class="section-copy" style="margin-top:8px;">Dhan live routing</div>', unsafe_allow_html=True)
        dhan_client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        dhan_token_present = bool(os.getenv("DHAN_ACCESS_TOKEN", "").strip())
        route_cols = st.columns([1.4, 1, 1])
        with route_cols[0]:
            dhan_security_map_path = st.text_input("Security map path", value="data/dhan_security_map.csv")
        with route_cols[1]:
            if dhan_client_id and dhan_token_present:
                st.success("Dhan credentials detected")
            else:
                st.warning("Add Dhan credentials to .env")
        with route_cols[2]:
            if st.button("Check Dhan Live Ready", use_container_width=True):
                readiness_notes = _run_dhan_readiness_check(symbol, dhan_security_map_path)
                for note in readiness_notes:
                    if note.startswith("FAIL"):
                        st.error(note)
                    elif note.startswith("WARN"):
                        st.warning(note)
                    elif note.startswith("PASS"):
                        st.success(note)
                    else:
                        st.info(note)

    st.markdown('</div>', unsafe_allow_html=True)

    if live_update:
        components.html(
            f"""<script>
                const ms = {int(refresh_seconds)} * 1000;
                setTimeout(() => window.location.reload(), ms);
            </script>""",
            height=0,
        )

    st.info("Fetching live OHLCV data...")
    try:
        candles = fetch_ohlcv_data(symbol, interval=interval, period=period)
    except Exception as exc:
        st.error(f"Data fetch failed: {exc}")
        candles = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    st.caption(f"Total candles fetched: {len(candles)}")

    try:
        if strategy == "MTF 5m" and interval != "5m":
            st.warning("MTF 5m strategy requires the base interval to be 5m so it can derive 15m and 1h candles.")
            output_rows = []
        else:
            output_rows = run_strategy(
                strategy=strategy,
                candles=candles,
                capital=capital,
                risk_pct=risk_pct,
                rr_ratio=rr_ratio,
                trailing_sl_pct=trailing_sl_pct,
                symbol=symbol,
                strike_step=int(strike_step),
                moneyness=str(moneyness),
                strike_steps=int(strike_steps),
                fetch_option_metrics=bool(fetch_option_metrics),
                mtf_ema_period=int(mtf_ema_period),
                mtf_setup_mode=str(mtf_setup_mode),
                mtf_retest_strength=bool(mtf_retest_strength),
                mtf_max_trades_per_day=int(mtf_max_trades_per_day),
            )
    except Exception as exc:
        st.error(f"Strategy execution failed: {exc}")
        output_rows = []

    if output_rows:
        if instrument_mode == "Futures":
            output_rows = attach_futures_contracts(output_rows, symbol)
        output_rows = attach_lots(output_rows, lot_size=int(lot_size), lots=int(lots))


    latest_sidebar_price = candles["close"].iloc[-1] if not candles.empty else "-"
    signal_rows = [r for r in output_rows if str(r.get("side", "")).upper() in {"BUY", "SELL"}]
    last_signal_side = str(signal_rows[-1].get("side", "-")) if signal_rows else "-"
    if send_telegram and output_rows and not auto_execute_generated:
        latest = None
        for r in reversed(output_rows):
            if str(r.get("side")) in {"BUY", "SELL"}:
                latest = r
                break
        if latest is None:
            latest = output_rows[-1]
        send_signal_alert(latest, strategy=strategy, symbol=symbol, refresh_seconds=int(refresh_seconds))

    execution_candidates: list[dict[str, object]] = []
    analyzed_candidates: list[dict[str, object]] = []
    if build_execution_candidates is not None:
        try:
            execution_candidates = build_execution_candidates(strategy, output_rows, symbol)
        except Exception as exc:
            st.warning(f"Could not build execution candidates: {exc}")
            execution_candidates = []
    if build_analysis_queue is not None:
        analyzed_candidates = build_analysis_queue(execution_candidates)

    auto_executed_rows: list[dict[str, object]] = []
    if auto_execute_generated and execution_candidates:
        try:
            if execution_mode == "LIVE":
                if execute_live_trades is None:
                    st.error("Live execution module is not available.")
                else:
                    auto_executed_rows = execute_live_trades(execution_candidates, Path(live_log_output), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
            else:
                if execute_paper_trades is None:
                    st.error("Paper execution module is not available.")
                else:
                    auto_executed_rows = execute_paper_trades(execution_candidates, Path(paper_log_output), deduplicate=True)
        except Exception as exc:
            st.error(f"Auto execution failed: {exc}")
            auto_executed_rows = []

        if auto_executed_rows:
            st.success(f"Auto executed {len(auto_executed_rows)} trade(s) in {execution_mode} mode.")
            if execution_mode == "LIVE":
                _render_live_execution_feedback(auto_executed_rows)
            if send_telegram:
                signal_map = {
                    f"{row.get('strategy','')}|{row.get('symbol','')}|{row.get('entry_time', row.get('timestamp',''))}|{row.get('side','')}": row
                    for row in output_rows
                    if isinstance(row, dict)
                }
                for executed in auto_executed_rows:
                    exec_key = f"{executed.get('strategy','')}|{executed.get('symbol','')}|{executed.get('signal_time','')}|{executed.get('side','')}"
                    alert_row = dict(signal_map.get(exec_key, {}))
                    alert_row.update(executed)
                    send_signal_alert(alert_row, strategy=strategy, symbol=symbol, refresh_seconds=int(refresh_seconds))
    if "analyzed_trade_queue" not in st.session_state:
        st.session_state["analyzed_trade_queue"] = []


    account_status = "Paper" if execution_mode != "LIVE" else ("Dhan Ready" if (dhan_client_id and dhan_token_present) else "Dhan Missing")
    _render_page_masthead(
        symbol=str(symbol),
        strategy=str(strategy),
        execution_mode=str(execution_mode),
        auto_execute=bool(auto_execute_generated),
        interval=str(interval),
        period=str(period),
        instrument_mode=str(instrument_mode),
        lots=int(lots),
        lot_size=int(lot_size),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        last_signal_side=str(last_signal_side),
        open_trades=len(signal_rows),
        account_status=account_status,
    )

    hero_last_price = float(candles["close"].iloc[-1]) if not candles.empty else 0.0
    if not candles.empty and len(candles) >= 2:
        hero_day_change = float(candles["close"].iloc[-1]) - float(candles["close"].iloc[-2])
    else:
        hero_day_change = 0.0
    hero_levels = compute_market_levels(candles) if not candles.empty else {"support_low": 0.0, "support_high": 0.0, "resistance_low": 0.0, "resistance_high": 0.0}
    support_band = f"{hero_levels['support_low']:.2f}-{hero_levels['support_high']:.2f}" if hero_levels['support_high'] else "-"
    resistance_band = f"{hero_levels['resistance_low']:.2f}-{hero_levels['resistance_high']:.2f}" if hero_levels['resistance_high'] else "-"
    ce_count = sum(1 for r in signal_rows if str(r.get("option_type", "")).upper() == "CE")
    pe_count = sum(1 for r in signal_rows if str(r.get("option_type", "")).upper() == "PE")
    if ce_count > pe_count:
        option_bias = f"CE {ce_count}:{pe_count}"
    elif pe_count > ce_count:
        option_bias = f"PE {pe_count}:{ce_count}"
    elif ce_count == pe_count and ce_count > 0:
        option_bias = f"BAL {ce_count}:{pe_count}"
    else:
        option_bias = "NEUTRAL"
    if hero_last_price and hero_levels["resistance_high"] and hero_last_price >= hero_levels["resistance_high"]:
        market_status = "BREAKOUT"
    elif hero_last_price and hero_levels["support_low"] and hero_last_price <= hero_levels["support_low"]:
        market_status = "BREAKDOWN"
    else:
        market_status = "RANGE" if not signal_rows else f"LIVE {last_signal_side}"
    _render_hero_strip(
        symbol=str(symbol),
        last_price=hero_last_price if not candles.empty else "-",
        day_change=float(hero_day_change),
        strategy=str(strategy),
        execution_mode=str(execution_mode),
        open_trades=len(signal_rows),
        support_band=support_band,
        resistance_band=resistance_band,
        option_bias=option_bias,
        market_status=market_status,
    )
    tab1, tab2, tab3 = st.tabs(["Dashboard", "Charts", "Trades"])

    with tab1:
        st.markdown('<div class="section-shell">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">Market Overview</div><div class="section-copy">Live market snapshot with the latest price, volume, and recent candles.</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)

        if not candles.empty:
            latest_close = float(candles["close"].iloc[-1])
            latest_high = float(candles["high"].iloc[-1])
            latest_low = float(candles["low"].iloc[-1])
            latest_volume = float(candles["volume"].iloc[-1]) if "volume" in candles.columns else 0.0
        else:
            latest_close = latest_high = latest_low = latest_volume = 0.0

        c1.metric("Close", round(latest_close, 2))
        c2.metric("High", round(latest_high, 2))
        c3.metric("Low", round(latest_low, 2))
        c4.metric("Volume", int(latest_volume))

        if not candles.empty:
            st.dataframe(candles.tail(6), use_container_width=True, height=240)
        else:
            st.warning("No candle data available.")
        st.markdown("</div>", unsafe_allow_html=True)

    with tab2:
        st.markdown('<div class="chart-shell">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">Market Chart</div><div class="section-copy">Intraday candlestick view with support, resistance, and market depth context.</div>', unsafe_allow_html=True)
        if not candles.empty:
            latest_move = 0.0
            if len(candles) >= 2:
                try:
                    latest_move = float(candles["close"].iloc[-1]) - float(candles["close"].iloc[-2])
                except Exception:
                    latest_move = 0.0

            levels = compute_market_levels(candles)
            move_color = "#16a34a" if latest_move >= 0 else "#dc2626"
            move_prefix = "+" if latest_move > 0 else ""
            st.markdown(
                f"""
                <div style=\"background:#f8fafc;border:1px solid #dbe4ee;border-radius:18px;padding:18px 20px;margin-bottom:12px;box-shadow:0 10px 24px rgba(15,23,42,0.05);\"> 
                    <div style=\"display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap;\">
                        <div>
                            <div style=\"color:#64748b;font-size:12px;letter-spacing:0.12em;text-transform:uppercase;\">Live Price</div>
                            <div style=\"color:#0f172a;font-size:34px;font-weight:700;line-height:1.05;\">{levels['last_price']:.2f}</div>
                        </div>
                        <div style=\"text-align:right;\">
                            <div style=\"color:{move_color};font-size:24px;font-weight:700;\">{move_prefix}{latest_move:.2f}</div>
                            <div style=\"color:#64748b;font-size:12px;\">vs previous candle close</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Session High", round(levels["session_high"], 2))
            h2.metric("Session Low", round(levels["session_low"], 2))
            h3.metric("Support Band", f"{levels['support_low']:.2f}-{levels['support_high']:.2f}")
            h4.metric("Resistance Band", f"{levels['resistance_low']:.2f}-{levels['resistance_high']:.2f}")

            left, right = st.columns([4.4, 1.6])
            with left:
                chart = build_live_market_chart(candles, output_rows=output_rows)
                st.altair_chart(chart, use_container_width=True)
                st.caption("Standard candlestick chart with volume and optional BUY/SELL or CE/PE trade markers.")
            with right:
                st.markdown("**Market Depth View**")
                depth_df = build_market_depth_summary(candles)
                st.dataframe(depth_df, use_container_width=True, hide_index=True)
                st.caption(f"Price spread between support and resistance bands: {levels['spread']:.2f}")
        else:
            st.info("No chart data available.")
        st.markdown("</div>", unsafe_allow_html=True)

    with tab3:
        st.markdown('<div class="section-shell">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">Trade Workspace</div><div class="section-copy">Review live-ready setups, preview broker payloads, and send only the orders you actually want routed.</div>', unsafe_allow_html=True)
        if auto_executed_rows:
            st.caption("Auto-executed trades from this run.")
            st.dataframe(_order_trade_columns(pd.DataFrame(auto_executed_rows)), use_container_width=True)

        if output_rows:
            trades_df = pd.DataFrame(output_rows)
            with st.expander(f"Generated Trades ({len(trades_df)})", expanded=False):
                st.dataframe(trades_df.tail(12), use_container_width=True, height=300)

            try:
                summary = build_trade_summary(output_rows)
                st.text(summary)
            except Exception as exc:
                st.warning(f"Could not build trade summary: {exc}")

            csv_data = _to_csv(output_rows)
            st.download_button("Download CSV", data=csv_data, file_name="trades.csv", mime="text/csv")
        else:
            st.info("No trades generated yet.")

        st.divider()
        st.subheader("Analyze First, Execute Later")
        if execution_candidates:
            st.caption("Current executable candidates generated from the latest strategy run.")
            with st.expander(f"Execution Candidates ({len(execution_candidates)})", expanded=False):
                st.dataframe(_order_trade_columns(pd.DataFrame(execution_candidates)), use_container_width=True, height=280)
            if execution_mode == "LIVE":
                with st.expander("Dhan Live Payload Preview"):
                    if st.button("Preview Live Payloads", use_container_width=True):
                        st.session_state["dhan_payload_preview"] = _build_dhan_preview_rows(
                            execution_candidates,
                            dhan_security_map_path,
                        )
                    preview_rows = st.session_state.get("dhan_payload_preview", [])
                    if preview_rows:
                        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)
                    else:
                        st.caption("Preview the exact Dhan live-order payloads here before sending them to the broker.")
        else:
            st.info("No execution candidates are available for the current strategy output.")

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Analyze Current Trades", use_container_width=True):
                st.session_state["analyzed_trade_queue"] = analyzed_candidates
                if analyzed_candidates:
                    st.success(f"Analyzed {len(analyzed_candidates)} executable trade(s). Review them below before execution.")
                else:
                    st.warning("No BUY/SELL trades were available to analyze.")
        with c2:
            if st.button("Clear Analyzed Queue", use_container_width=True):
                st.session_state["analyzed_trade_queue"] = []
                st.info("Cleared the analyzed trade queue.")
        with c3:
            st.caption(f"Execution mode: {execution_mode}")

        staged_candidates = st.session_state.get("analyzed_trade_queue", [])
        if staged_candidates:
            st.caption("Reviewed trade queue. Only this staged list will be executed.")
            with st.expander(f"Reviewed Queue ({len(staged_candidates)})", expanded=True):
                st.dataframe(_order_trade_columns(pd.DataFrame(staged_candidates)), use_container_width=True, height=260)

            executed_rows: list[dict[str, object]] = []
            execute_clicked = st.button("Execute Reviewed Trades", type="primary", use_container_width=True)
            if execute_clicked:
                if execution_mode == "LIVE":
                    if execute_live_trades is None:
                        st.error("Live execution module is not available.")
                    else:
                        executed_rows = execute_live_trades(staged_candidates, Path(live_log_output), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
                else:
                    if execute_paper_trades is None:
                        st.error("Paper execution module is not available.")
                    else:
                        executed_rows = execute_paper_trades(staged_candidates, Path(paper_log_output), deduplicate=True)

                if executed_rows:
                    st.success(f"Executed {len(executed_rows)} reviewed trade(s) in {execution_mode} mode.")
                    st.dataframe(_order_trade_columns(pd.DataFrame(executed_rows)), use_container_width=True)
                    if execution_mode == "LIVE":
                        _render_live_execution_feedback(executed_rows)
                else:
                    st.warning("No new reviewed trades were executed. They may already be logged.")
        else:
            st.info("Analyze trades first to build a review queue, then execute that reviewed batch later.")
        st.markdown("</div>", unsafe_allow_html=True)
    raw_candles_csv = candles.to_csv(index=False) if not candles.empty else "timestamp,open,high,low,close,volume`n"
    debug_payload = {
        "strategy": strategy,
        "workspace": workspace,
        "symbol": symbol,
        "execution_mode": execution_mode,
        "instrument_mode": instrument_mode,
        "output_rows_count": len(output_rows) if isinstance(output_rows, list) else 0,
        "execution_candidates_count": len(execution_candidates),
        "reviewed_queue_count": len(st.session_state.get("analyzed_trade_queue", [])),
    }

    st.markdown('<div class="section-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">Downloads</div><div class="section-copy">Raw data and debug output are hidden from the page and available only as file downloads.</div>', unsafe_allow_html=True)
    download_cols = st.columns(3)
    with download_cols[0]:
        st.download_button("Download Raw Candles CSV", data=raw_candles_csv, file_name="krsh_raw_candles.csv", mime="text/csv", use_container_width=True)
    with download_cols[1]:
        st.download_button("Download Trades CSV", data=_to_csv(output_rows) if output_rows else "", file_name="krsh_trades.csv", mime="text/csv", use_container_width=True)
    with download_cols[2]:
        st.download_button("Download Debug JSON", data=json.dumps(debug_payload, indent=2), file_name="krsh_debug.json", mime="application/json", use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

    _render_page_footer()


if __name__ == "__main__":
    main()









































































