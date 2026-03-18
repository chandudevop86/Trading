from __future__ import annotations

import csv
import io
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



st.set_page_config(page_title="Trading Dashboard", page_icon="chart", layout="wide")

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
            padding: 20px 22px;
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
            padding: 14px 16px;
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
            padding: 14px 16px;
            margin-bottom: 14px;
            box-shadow: 0 18px 34px rgba(2, 6, 23, 0.18);
        }
        [data-testid="stSidebar"] {
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
            padding: 14px 16px;
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
                linear-gradient(180deg, #04111f 0%, #071a2f 44%, #0b223d 100%);
        }
        [data-testid="stHeader"] {
            background: rgba(4, 17, 31, 0.74);
            border-bottom: 1px solid rgba(118, 164, 210, 0.10);
        }
        [data-testid="stAppViewContainer"] .main .block-container {
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
            background: linear-gradient(135deg, #ff7a2f 0%, #ff5b21 100%);
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
            background: linear-gradient(135deg, #ff7a2f 0%, #ff5b21 100%);
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
            background: linear-gradient(180deg, #08182c 0%, #0b2038 100%);
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
                linear-gradient(90deg, rgba(3, 11, 32, 0.92) 0%, rgba(8, 36, 64, 0.86) 44%, rgba(19, 68, 108, 0.72) 100%),
                radial-gradient(circle at 72% 22%, rgba(122, 214, 255, 0.18), transparent 24%),
                radial-gradient(circle at 82% 68%, rgba(122, 214, 255, 0.12), transparent 22%);
            border: 1px solid rgba(118, 164, 210, 0.14);
            border-radius: 26px;
            padding: 34px 34px 26px 34px;
            margin-bottom: 16px;
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
            background: linear-gradient(90deg, #082340 0%, #0c3155 50%, #123e68 100%);
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
            color: #ff6b2c;
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
            color: #ff6b2c;
        }
        .top-nav-cta {
            background: linear-gradient(135deg, #ff7a2f 0%, #ff5b21 100%);
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
            font-size: 52px;
            font-weight: 800;
            line-height: 1.03;
            margin: 0;
            max-width: 640px;
        }
        .page-title .accent {
            color: #ff6b2c;
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
            margin-top: 22px;
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
def _render_page_masthead(symbol: str, strategy: str, execution_mode: str, auto_execute: bool) -> None:
    auto_text = "Auto Execution Enabled" if auto_execute else "Analysis First Workflow"
    st.markdown(
        f"""
        <div class="top-nav">
            <div class="top-nav-brand">BIT<span>PROFIT</span></div>
            <div class="top-nav-menu">
                <span class="active">Home</span>
                <span>Markets</span>
                <span>Strategies</span>
                <span>Live Charts</span>
                <span>Execution</span>
            </div>
            <div class="top-nav-cta">Broker</div>
        </div>
        <div class="page-masthead">
            <div class="masthead-grid">
                <div>
                    <div class="page-eyebrow">Live Market Intelligence</div>
                    <h1 class="page-title">New Level <span class="accent">Trading</span> Dashboard For <span class="accent">Live Markets</span></h1>
                    <div class="page-subtitle">Track intraday market structure, review strategy output, and move from analysis to execution through a single broker-style workspace for {symbol}.</div>
                </div>
                <div class="page-badge">{strategy}<br/>{execution_mode}<br/>{auto_text}</div>
            </div>
            <div class="masthead-pills">
                <div class="masthead-pill">Enhanced Tools</div>
                <div class="masthead-pill">Trading Guides</div>
                <div class="masthead-pill">Fast Execution</div>
                <div class="masthead-pill">Auto Alerts</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

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
    st.sidebar.markdown(text, unsafe_allow_html=True)


def _render_sidebar_status(
    symbol: str,
    last_price: object,
    strategy: str,
    execution_mode: str,
    open_trades: int = 0,
    last_signal_side: str = "-",
    auto_execute_enabled: bool = False,
) -> None:
    price_text = _fmt_num(last_price)
    auto_text = "ON" if auto_execute_enabled else "OFF"
    signal_text = str(last_signal_side or "-").upper()
    st.sidebar.markdown(
        f"""
        <div class="live-panel" style="padding:12px 14px;">
            <div class="live-kicker">Live Status</div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;">
                <div class="status-card">
                    <div class="status-label">Symbol</div>
                    <div class="status-value">{symbol}</div>
                </div>
                <div class="status-card">
                    <div class="status-label">Last Price</div>
                    <div class="status-value status-price">{price_text}</div>
                </div>
                <div class="status-card">
                    <div class="status-label">Strategy</div>
                    <div class="status-value">{strategy}</div>
                </div>
                <div class="status-card">
                    <div class="status-label">Mode</div>
                    <div class="status-value">{execution_mode}</div>
                </div>
                <div class="status-card">
                    <div class="status-label">Open Trades</div>
                    <div class="status-value">{open_trades}</div>
                </div>
                <div class="status-card">
                    <div class="status-label">Last Signal</div>
                    <div class="status-value">{signal_text}</div>
                </div>
                <div class="status-card" style="grid-column: span 2;">
                    <div class="status-label">Auto Execute</div>
                    <div class="status-value">{auto_text}</div>
                </div>
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



def _format_expiry(expiry: object) -> str:
    if expiry is None:
        return ""
    text = str(expiry).strip()
    if not text or text in {"-", "N/A"}:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


def _format_ts_ist(ts: object) -> str:
    if ts is None:
        return "-"
    try:
        dt = pd.to_datetime(ts, errors="coerce")
    except Exception:
        dt = None
    if dt is None or pd.isna(dt):
        return str(ts)

    if isinstance(dt, pd.Timestamp):
        py = dt.to_pydatetime()
    else:
        py = dt

    if getattr(py, "tzinfo", None) is None:
        py = py.replace(tzinfo=ZoneInfo("UTC"))

    ist = py.astimezone(ZoneInfo("Asia/Kolkata"))
    return ist.strftime("%Y-%m-%d %H:%M:%S IST")


def _safe_float(val: object) -> float | None:
    try:
        num = float(val)  # type: ignore[arg-type]
    except Exception:
        return None
    if pd.isna(num):
        return None
    return float(num)


def _enrich_trade_row(row: dict[str, object], spot_ltp: float | None = None) -> dict[str, object]:
    enriched = dict(row)
    if spot_ltp is not None and not enriched.get("spot_ltp"):
        enriched["spot_ltp"] = round(float(spot_ltp), 2)

    side = str(enriched.get("side", "") or "").upper()
    entry = _safe_float(enriched.get("entry_price", enriched.get("entry")))
    stop = _safe_float(enriched.get("stop_loss", enriched.get("sl")))
    if side not in {"BUY", "SELL"} or entry is None or stop is None:
        return enriched

    risk = abs(float(entry) - float(stop))
    if risk <= 0:
        return enriched

    if side == "BUY":
        targets = [entry + risk, entry + (2.0 * risk), entry + (3.0 * risk)]
    else:
        targets = [entry - risk, entry - (2.0 * risk), entry - (3.0 * risk)]

    for idx, value in enumerate(targets, start=1):
        enriched.setdefault(f"target_{idx}", round(float(value), 4))

    if not enriched.get("target_price"):
        enriched["target_price"] = enriched.get("target_2")

    return enriched



def _estimate_weekly_expiry(symbol: str, now: datetime | None = None) -> str:
    s = (symbol or "").strip().upper()
    if s in {"^NSEI", "NIFTY", "NIFTY 50", "NIFTY50"}:
        tz = ZoneInfo("Asia/Kolkata")
        dt = now or datetime.now(tz)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        # Thursday = 3 (Mon=0)
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ""


def _estimate_monthly_expiry(symbol: str, now: datetime | None = None) -> str:
    s = (symbol or "").strip().upper()
    if s not in {"^NSEI", "NIFTY", "NIFTY 50", "NIFTY50", "NIFTY FUT", "NIFTY FUTURES", "NIFTYFUT"}:
        return ""

    tz = ZoneInfo("Asia/Kolkata")
    dt = now or datetime.now(tz)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)

    first_next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)
    expiry = first_next_month - timedelta(days=1)
    while expiry.weekday() != 3:
        expiry -= timedelta(days=1)
    return expiry.date().isoformat()


def attach_futures_contracts(rows: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    contract_symbol = normalize_index_symbol(symbol) if normalize_index_symbol else str(symbol).strip().upper().replace("^", "")
    expiry = _estimate_monthly_expiry(symbol)
    out: list[dict[str, object]] = []
    for r in rows:
        row = dict(r)
        row["instrument"] = "FUTURES"
        row["contract_type"] = "FUTIDX"
        row["contract_symbol"] = contract_symbol
        if expiry:
            row["contract_expiry"] = _format_expiry(expiry)
            row["contract_expiry_source"] = "EST"
        for key in ["option_type", "strike_price", "option_strike", "option_ltp", "option_oi", "option_vol", "option_iv", "option_expiry", "option_expiry_source"]:
            row.pop(key, None)
        out.append(row)
    return out

def attach_lots(rows: list[dict[str, object]], lot_size: int, lots: int) -> list[dict[str, object]]:
    lot_size = int(lot_size) if lot_size and int(lot_size) > 0 else 0
    lots = int(lots) if lots and int(lots) > 0 else 0
    if lot_size <= 0 or lots <= 0:
        return rows

    qty = lot_size * lots
    out: list[dict[str, object]] = []
    for r in rows:
        row = dict(r)
        row["lots"] = lots
        row["quantity"] = qty
        try:
            ltp = float(row.get("option_ltp", row.get("entry_price", row.get("share_price", row.get("price", 0)))) or 0)
        except Exception:
            ltp = 0.0
        if ltp > 0:
            row["order_value"] = round(ltp * qty, 2)
        out.append(row)
    return out


def send_signal_alert(
    trade: dict[str, object],
    strategy: str,
    symbol: str,
    refresh_seconds: int | None = None,
) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    side = str(trade.get("side", "-") or "-")
    trade_label = str(trade.get("trade_label", "") or "").strip()
    entry = _fmt_num(trade.get("entry_price", trade.get("entry", "-")))
    sl = _fmt_num(trade.get("stop_loss", trade.get("sl", "-")))
    target = _fmt_num(trade.get("target_price", trade.get("target", "-")))
    target_1 = _fmt_num(trade.get("target_1"))
    target_2 = _fmt_num(trade.get("target_2"))
    target_3 = _fmt_num(trade.get("target_3"))
    option = str(trade.get("option_strike", "") or "").strip()
    contract = str(trade.get("contract_symbol", "") or "").strip()
    contract_expiry = str(trade.get("contract_expiry", "") or "").strip()

    spot_ltp = _fmt_num(trade.get("spot_ltp", trade.get("close", trade.get("share_price", "-"))))
    opt_ltp = _fmt_num(trade.get("option_ltp"))
    opt_oi = _fmt_num(trade.get("option_oi"))
    opt_vol = _fmt_num(trade.get("option_vol"))
    opt_iv = _fmt_num(trade.get("option_iv"))
    opt_expiry = _format_expiry(trade.get("option_expiry"))
    opt_expiry_source = str(trade.get("option_expiry_source", "") or "").upper()
    if opt_expiry and opt_expiry_source == "EST":
        opt_expiry = opt_expiry + " (est)"

    lots = str(trade.get("lots", "") or "").strip()
    qty = str(trade.get("quantity", "") or "").strip()
    value = _fmt_num(trade.get("order_value"))

    ts = _format_ts_ist(trade.get("timestamp") or trade.get("entry_time"))

    extra = ""
    if refresh_seconds is not None:
        if refresh_seconds >= 60:
            extra = f" (next update in {refresh_seconds // 60} min)"
        else:
            extra = f" (next update in {refresh_seconds} sec)"

    parts: list[str] = [
        "Trade Signal",
        "",
        f"Strategy: {strategy}",
        f"Symbol: {symbol}",
        f"Side: {side}",
    ]
    if trade_label:
        parts.append(f"Setup: {trade_label}")
    if entry != "-":
        parts.append(f"Entry: {entry}")
    if sl != "-":
        parts.append(f"SL: {sl}")
    if target != "-":
        parts.append(f"Target: {target}")
    if target_1 != "-":
        parts.append(f"Target 1: {target_1}")
    if target_2 != "-":
        parts.append(f"Target 2: {target_2}")
    if target_3 != "-":
        parts.append(f"Target 3: {target_3}")
    if option:
        parts.append(f"Option: {option}")
    if contract:
        contract_line = f"Futures: {contract}"
        if contract_expiry:
            contract_line = contract_line + f" ({contract_expiry})"
        parts.append(contract_line)
    if opt_expiry:
        parts.append(f"Expiry: {opt_expiry}")
    if spot_ltp != "-":
        parts.append(f"Spot LTP: {spot_ltp}")
    if opt_ltp != "-":
        parts.append(f"Option LTP: {opt_ltp}")
    if opt_oi != "-":
        parts.append(f"OI: {opt_oi}")
    if opt_vol != "-":
        parts.append(f"Vol: {opt_vol}")
    if opt_iv != "-":
        parts.append(f"IV: {opt_iv}")
    if lots and qty:
        parts.append(f"Lots: {lots} (Qty: {qty})")
    if value != "-":
        parts.append(f"Value: {value}")

    parts.append(f"Time: {ts}{extra}")

    msg = "\n".join(parts)

    try:
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
    except Exception as exc:
        st.warning(f"Telegram alert failed: {exc}")

def call_strategy_function(func, candles, **kwargs):
    sig = inspect.signature(func)
    accepted = sig.parameters
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in accepted}
    try:
        return func(candles, **filtered_kwargs)
    except TypeError:
        return func(candles)


def _indicator_side(signal: str) -> str:
    s = (signal or "").upper()
    if s in {"BULLISH_TREND", "OVERSOLD", "BUY"}:
        return "BUY"
    if s in {"BEARISH_TREND", "OVERBOUGHT", "SELL"}:
        return "SELL"
    return "-"


def _attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
    for r in rows:
        row = dict(r)
        side = _indicator_side(str(row.get("market_signal", "")))
        row.setdefault("side", side)

        try:
            entry = float(row.get("close", 0.0) or 0.0)
        except Exception:
            entry = 0.0

        if entry > 0 and side in {"BUY", "SELL"}:
            row["entry_price"] = round(entry, 2)
            if sl_frac <= 0:
                sl_frac = 0.002
            if side == "BUY":
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)
            row["stop_loss"] = round(sl, 2)
            row["target_price"] = round(tp, 2)

        if "timestamp" in row:
            row["timestamp"] = str(row["timestamp"])
        out.append(row)

    return out


def run_strategy(
    strategy: str,
    candles: pd.DataFrame,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
    symbol: str,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    fetch_option_metrics: bool,
    mtf_ema_period: int,
    mtf_setup_mode: str,
    mtf_retest_strength: bool,
    mtf_max_trades_per_day: int,
) -> list[dict[str, object]]:
    if candles.empty:
        return []

    strategy_kwargs = {
        "capital": float(capital),
        "risk_pct": float(risk_pct) / 100.0,
        "rr_ratio": float(rr_ratio),
        "trailing_sl_pct": float(trailing_sl_pct) / 100.0,
        "ema_period": int(mtf_ema_period),
        "setup_mode": str(mtf_setup_mode),
        "require_retest_strength": bool(mtf_retest_strength),
        "max_trades_per_day": int(mtf_max_trades_per_day),
    }

    metrics_map: dict[tuple[int, str], dict[str, object]] = {}
    if fetch_option_metrics and fetch_option_chain and extract_option_records and build_metrics_map and normalize_index_symbol:
        try:
            sym = normalize_index_symbol(symbol)
            payload = fetch_option_chain(sym, timeout=10.0)
            records = extract_option_records(payload)
            metrics_map = build_metrics_map(records)  # type: ignore[assignment]
        except Exception:
            metrics_map = {}
    latest_spot_ltp = _safe_float(candles["close"].iloc[-1]) if not candles.empty else None

    if strategy == "Demand Supply":
        if generate_demand_supply_trades is None:
            st.warning("Demand Supply module not available.")
            return []
        zones = call_strategy_function(generate_demand_supply_trades, candles, **strategy_kwargs)

        out_rows: list[dict[str, object]] = []
        sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
        if sl_frac <= 0:
            sl_frac = 0.002

        for z in zones or []:
            if not isinstance(z, dict):
                continue
            zone_type = str(z.get("type", "")).lower()
            side = "BUY" if zone_type == "demand" else "SELL" if zone_type == "supply" else "-"
            if side == "-":
                continue
            try:
                entry = float(z.get("price", 0.0) or 0.0)
            except Exception:
                entry = 0.0
            if entry <= 0:
                continue

            ts = ""
            try:
                idx = int(z.get("index", -1))
                if 0 <= idx < len(candles):
                    ts = str(candles["timestamp"].iloc[idx])
            except Exception:
                ts = ""

            if side == "BUY":
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)

            row: dict[str, object] = {
                "strategy": "Demand Supply",
                "symbol": symbol,
                "zone_type": zone_type,
                "side": side,
                "entry_price": round(entry, 2),
                "stop_loss": round(sl, 2),
                "target_price": round(tp, 2),
                "timestamp": ts,
            }

            try:
                strike, opt = pick_option_strike(
                    spot_price=float(row["entry_price"]),
                    side=str(row["side"]),
                    step=int(strike_step),
                    moneyness=str(moneyness),
                    steps=int(strike_steps),
                )
                row["option_type"] = opt
                row["strike_price"] = strike
                row["option_strike"] = f"{strike}{opt}"

                metrics = metrics_map.get((int(strike), str(opt)))

                if isinstance(metrics, dict):

                    row.update(metrics)

                    if metrics.get("option_expiry"):

                        row["option_expiry_source"] = "NSE"
            except Exception:
                pass

            if not row.get("option_expiry") and row.get("option_strike"):
                est = _estimate_weekly_expiry(symbol)
                if est:
                    row["option_expiry"] = est
                    row["option_expiry_source"] = "EST"

            if "option_expiry" in row:
                row["option_expiry"] = _format_expiry(row.get("option_expiry"))

            out_rows.append(_enrich_trade_row(row, latest_spot_ltp))

        return out_rows

    if strategy == "Indicator":
        candle_list = _df_to_candles(candles)
        rows = generate_indicator_rows(
            candle_list,
            config=IndicatorConfig(
                rsi_period=14,
                adx_period=14,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
            ),
        )
        rows = _attach_indicator_trade_levels(rows, rr_ratio=rr_ratio, trailing_sl_pct=trailing_sl_pct)
        rows = [_enrich_trade_row(dict(r, strategy="Indicator", symbol=symbol), latest_spot_ltp) for r in rows]

        actionable = [r for r in rows if str(r.get("side")) in {"BUY", "SELL"} and r.get("entry_price")]
        if actionable:
            actionable = attach_option_strikes(actionable, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
            for r in actionable:
                strike = int(r.get("strike_price") or 0)
                opt = str(r.get("option_type") or "")
                metrics = metrics_map.get((strike, opt))
                if isinstance(metrics, dict):
                    r.update(metrics)
                    if metrics.get("option_expiry"):
                        r["option_expiry_source"] = "NSE"

                if not r.get("option_expiry") and r.get("option_strike"):
                    est = _estimate_weekly_expiry(symbol)
                    if est:
                        r["option_expiry"] = est
                        r["option_expiry_source"] = "EST"

                if "option_expiry" in r:
                    r["option_expiry"] = _format_expiry(r.get("option_expiry"))

            last_actionable = actionable[-1]
            for i in range(len(rows) - 1, -1, -1):
                if str(rows[i].get("timestamp")) == str(last_actionable.get("timestamp")):
                    rows[i].update(last_actionable)
                    break

        return rows[-200:]

    if strategy == "One Trade/Day":
        candle_list = _df_to_candles(candles)
        trades = call_strategy_function(generate_one_trade_day_trades, candle_list, **strategy_kwargs)
    elif strategy == "Breakout":
        candle_list = _df_to_candles(candles)
        trades = call_strategy_function(generate_breakout_trades, candle_list, **strategy_kwargs)
    elif strategy == "MTF 5m":
        candle_list = _df_to_candles(candles)
        trades = call_strategy_function(generate_mtf_trade_trades, candle_list, **strategy_kwargs)
    else:
        trades = []

    out_rows: list[dict[str, object]] = []
    for t in trades or []:
        if not isinstance(t, dict):
            continue
        row = dict(t)
        row.setdefault("strategy", strategy)
        row.setdefault("symbol", symbol)

        if "timestamp" not in row:
            if "entry_time" in row:
                row["timestamp"] = str(row.get("entry_time"))
            elif "time" in row:
                row["timestamp"] = str(row.get("time"))

        if "target" in row and "target_price" not in row:
            row["target_price"] = row.get("target")

        if "entry_price" in row and "side" in row:
            try:
                annotated = attach_option_strikes([row], strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
                row = annotated[0]
                strike = int(row.get("strike_price") or 0)
                opt = str(row.get("option_type") or "")
                metrics = metrics_map.get((strike, opt))
                if isinstance(metrics, dict):
                    row.update(metrics)
                    if metrics.get("option_expiry"):
                        row["option_expiry_source"] = "NSE"

                if not row.get("option_expiry") and row.get("option_strike"):
                    est = _estimate_weekly_expiry(symbol)
                    if est:
                        row["option_expiry"] = est
                        row["option_expiry_source"] = "EST"

                if "option_expiry" in row:
                    row["option_expiry"] = _format_expiry(row.get("option_expiry"))
            except Exception:
                pass

        out_rows.append(_enrich_trade_row(row, latest_spot_ltp))

    return out_rows


def _resolve_live_execution_kwargs(security_map_path: str) -> dict[str, object]:
    broker_name = "DHAN"
    raw_path = str(security_map_path or "data/dhan_security_map.csv").strip() or "data/dhan_security_map.csv"
    security_map: dict[str, dict[str, str]] | None = None
    if load_security_map is not None:
        try:
            security_map = load_security_map(Path(raw_path))
        except Exception:
            security_map = None
    return {
        "broker_name": broker_name,
        "security_map": security_map,
    }


def _render_live_execution_feedback(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    broker_sent = sum(1 for r in rows if str(r.get("broker_status", "")).upper() not in {"", "ERROR", "NOT_CONFIGURED"})
    broker_error = sum(1 for r in rows if str(r.get("broker_status", "")).upper() == "ERROR")
    broker_not_configured = sum(1 for r in rows if str(r.get("broker_status", "")).upper() == "NOT_CONFIGURED")
    c1, c2, c3 = st.columns(3)
    c1.metric("Broker Sent", broker_sent)
    c2.metric("Broker Errors", broker_error)
    c3.metric("Not Configured", broker_not_configured)


def _build_dhan_preview_rows(candidates: list[dict[str, object]], security_map_path: str) -> list[dict[str, object]]:
    if build_order_request_from_candidate is None:
        return [{"preview_status": "ERROR", "preview_error": "Dhan payload builder unavailable."}]

    resolved = _resolve_live_execution_kwargs(security_map_path)
    security_map = resolved.get("security_map")
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip() or "PREVIEW_CLIENT"
    previews: list[dict[str, object]] = []

    for candidate in candidates:
        if str(candidate.get("side", "")).upper() not in {"BUY", "SELL"}:
            continue
        try:
            request = build_order_request_from_candidate(
                candidate,
                client_id=client_id,
                security_map=security_map,  # type: ignore[arg-type]
            )
            payload = request.to_payload()
            payload["preview_status"] = "READY" if os.getenv("DHAN_CLIENT_ID", "").strip() else "CLIENT_ID_MISSING"
            previews.append(payload)
        except Exception as exc:
            previews.append(
                {
                    "symbol": candidate.get("symbol", ""),
                    "side": candidate.get("side", ""),
                    "signal_time": candidate.get("signal_time", candidate.get("entry_time", "")),
                    "preview_status": "ERROR",
                    "preview_error": str(exc),
                }
            )
    return previews


def _run_dhan_readiness_check(symbol: str, security_map_path: str) -> list[str]:
    notes: list[str] = []
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    if client_id:
        notes.append("PASS: DHAN_CLIENT_ID is configured.")
    else:
        notes.append("FAIL: DHAN_CLIENT_ID is missing.")
    if access_token:
        notes.append("PASS: DHAN_ACCESS_TOKEN is configured.")
    else:
        notes.append("FAIL: DHAN_ACCESS_TOKEN is missing.")
    raw_path = str(security_map_path or "data/dhan_security_map.csv").strip() or "data/dhan_security_map.csv"
    map_path = Path(raw_path)
    if not map_path.exists():
        notes.append(f"FAIL: Security map not found at {raw_path}.")
        return notes
    notes.append(f"PASS: Security map found at {raw_path}.")
    if load_security_map is None:
        notes.append("FAIL: Security map loader is unavailable.")
        return notes
    try:
        security_map = load_security_map(map_path)
    except Exception as exc:
        notes.append(f"FAIL: Could not load security map: {exc}")
        return notes
    notes.append(f"PASS: Loaded {len(security_map)} security-map keys.")
    normalized_symbol = (normalize_index_symbol(symbol) if normalize_index_symbol else str(symbol)).strip().upper().replace("^", "")
    symbol_matches = [k for k in security_map.keys() if normalized_symbol in k or k in {normalized_symbol, f"{normalized_symbol}FUT"}]
    if symbol_matches:
        notes.append(f"PASS: Found symbol coverage for {normalized_symbol}: {", ".join(symbol_matches[:5])}")
    else:
        notes.append(f"WARN: No direct security-map match found for {normalized_symbol}. Live orders may fail until security IDs are added.")
    notes.append("INFO: Dhan live order APIs also require whitelisted static IPs in Dhan settings.")
    return notes


def main() -> None:
    _render_sidebar_shell()

    st.sidebar.markdown(
        """
        <div class="live-panel">
            <div class="live-kicker">Control Center</div>
            <div class="live-title">Live Trade Desk</div>
            <div class="live-sub">Switch symbols, tune intraday filters, and send executions from one compact panel.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    _sidebar_section("Market Data", "Fast symbol and timeframe switching")
    symbol = st.sidebar.text_input("Symbol", "^NSEI")
    interval = st.sidebar.segmented_control("Interval", ["1m", "5m", "15m", "30m", "1h"], default="1m")
    period = st.sidebar.segmented_control("Period", ["1d", "5d", "1mo", "3mo"], default="1d")

    _sidebar_section("Bot Settings", "Risk and trade generation profile")
    capital = st.sidebar.number_input("Capital (INR)", min_value=1000, value=100000, step=1000)
    risk_pct = st.sidebar.slider("Risk per trade (%)", 0.1, 10.0, 1.0)
    rr_ratio = st.sidebar.slider("Risk/Reward Ratio", 1.0, 10.0, 2.0)
    trailing_sl_pct = st.sidebar.slider("Trailing Stop Loss %", 0.1, 10.0, 1.0, 0.1)

    strategy = st.sidebar.pills("Strategy", ["Breakout", "Demand Supply", "Indicator", "One Trade/Day", "MTF 5m"], default="Breakout")

    mtf_ema_period = 3
    mtf_setup_mode = "either"
    mtf_retest_strength = True
    mtf_max_trades_per_day = 3
    if strategy == "MTF 5m":
        _sidebar_section("MTF Settings", "1h bias, 15m setup, 5m trigger")
        mtf_ema_period = int(st.sidebar.number_input("EMA period (1h)", min_value=2, max_value=20, value=3, step=1))
        mtf_setup_label = st.sidebar.segmented_control("15m setup filter", ["Either", "BOS only", "FVG only"], default="Either")
        mtf_setup_mode = {"Either": "either", "BOS only": "bos", "FVG only": "fvg"}[str(mtf_setup_label)]
        mtf_retest_strength = st.sidebar.checkbox("Require strong 5m retest candle", value=True)
        mtf_max_trades_per_day = int(st.sidebar.segmented_control("Max trades/day", [1, 2, 3], default=3))

    _sidebar_section("Contract Setup", "Choose whether to generate options or futures contracts")
    instrument_mode = st.sidebar.segmented_control("Trade instrument", ["Options", "Futures"], default="Options")
    strike_step = int(st.sidebar.segmented_control("Strike step", [25, 50, 100], default=50, disabled=instrument_mode == "Futures"))
    moneyness = st.sidebar.pills("Moneyness", ["ATM", "ITM", "OTM"], default="ATM") if instrument_mode == "Options" else "ATM"
    strike_steps = st.sidebar.slider("Steps (ITM/OTM)", 0, 5, 0, disabled=instrument_mode == "Futures")
    fetch_option_metrics = st.sidebar.checkbox("Fetch option LTP/OI/Vol/IV + Expiry", value=False, disabled=instrument_mode == "Futures")
    if instrument_mode == "Futures":
        st.sidebar.caption("Futures mode attaches the monthly index futures contract instead of option strikes.")
    lot_size = st.sidebar.number_input("Lot size", min_value=1, value=65, step=1)
    lots = st.sidebar.slider("Lots (qty = lots x lot size)", 1, 10, 2)

    _sidebar_section("Execution Flow", "Refresh, alert, and order routing controls")
    live_update = st.sidebar.checkbox("Auto refresh", value=False)
    refresh_seconds = st.sidebar.slider("Refresh every (seconds)", 2, 120, 10)

    send_telegram = st.sidebar.checkbox("Send Telegram alert (latest signal)", value=False)

    execution_mode = st.sidebar.segmented_control("Execution mode", ["PAPER", "LIVE"], default="PAPER")
    if execution_mode == "LIVE":
        _sidebar_section("Dhan Live", "Broker routing for live order placement")
        dhan_client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        dhan_token_present = bool(os.getenv("DHAN_ACCESS_TOKEN", "").strip())
        st.sidebar.caption("Live orders use Dhan env vars and the security map file.")
        st.sidebar.text_input("Dhan client ID", value=dhan_client_id or "Not set", disabled=True)
        st.sidebar.text_input("Dhan token", value="Configured" if dhan_token_present else "Not set", disabled=True)
        dhan_security_map_path = st.sidebar.text_input("Security map path", value="data/dhan_security_map.csv")
        if st.sidebar.button("Test Dhan Ready", use_container_width=True):
            readiness_notes = _run_dhan_readiness_check(symbol, dhan_security_map_path)
            for note in readiness_notes:
                if note.startswith("FAIL"):
                    st.sidebar.error(note)
                elif note.startswith("WARN"):
                    st.sidebar.warning(note)
                elif note.startswith("PASS"):
                    st.sidebar.success(note)
                else:
                    st.sidebar.info(note)
    else:
        dhan_security_map_path = "data/dhan_security_map.csv"
    auto_execute_generated = st.sidebar.checkbox("Auto execute generated trades", value=False)
    _render_page_masthead(
        symbol=str(symbol),
        strategy=str(strategy),
        execution_mode=str(execution_mode),
        auto_execute=bool(auto_execute_generated),
    )
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

    with st.expander("View raw candle data"):
        st.dataframe(candles, use_container_width=True)

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
    _render_sidebar_status(
        symbol=symbol,
        last_price=latest_sidebar_price,
        strategy=str(strategy),
        execution_mode=str(execution_mode),
        open_trades=len(signal_rows),
        last_signal_side=last_signal_side,
        auto_execute_enabled=bool(auto_execute_generated),
    )
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
                    auto_executed_rows = execute_live_trades(execution_candidates, Path("data/live_trading_logs_all.csv"), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
            else:
                if execute_paper_trades is None:
                    st.error("Paper execution module is not available.")
                else:
                    auto_executed_rows = execute_paper_trades(execution_candidates, Path("data/paper_trading_logs_all.csv"), deduplicate=True)
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
            st.dataframe(candles.tail(10), use_container_width=True)
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
        st.markdown('<div class="section-heading">Trade Workspace</div><div class="section-copy">Review generated setups, stage orders, and manage execution from one table flow.</div>', unsafe_allow_html=True)
        if auto_executed_rows:
            st.caption("Auto-executed trades from this run.")
            st.dataframe(_order_trade_columns(pd.DataFrame(auto_executed_rows)), use_container_width=True)

        if output_rows:
            trades_df = pd.DataFrame(output_rows)
            st.dataframe(trades_df, use_container_width=True)

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
            st.dataframe(_order_trade_columns(pd.DataFrame(execution_candidates)), use_container_width=True)
            if execution_mode == "LIVE":
                with st.expander("Dry-Run Dhan Payload Preview"):
                    if st.button("Preview Live Payloads", use_container_width=True):
                        st.session_state["dhan_payload_preview"] = _build_dhan_preview_rows(
                            execution_candidates,
                            dhan_security_map_path,
                        )
                    preview_rows = st.session_state.get("dhan_payload_preview", [])
                    if preview_rows:
                        st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)
                    else:
                        st.caption("Build the exact Dhan order payloads here before sending any live order.")
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
            st.dataframe(_order_trade_columns(pd.DataFrame(staged_candidates)), use_container_width=True)

            executed_rows: list[dict[str, object]] = []
            execute_clicked = st.button("Execute Reviewed Trades", type="primary", use_container_width=True)
            if execute_clicked:
                if execution_mode == "LIVE":
                    if execute_live_trades is None:
                        st.error("Live execution module is not available.")
                    else:
                        executed_rows = execute_live_trades(staged_candidates, Path("data/live_trading_logs_all.csv"), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
                else:
                    if execute_paper_trades is None:
                        st.error("Paper execution module is not available.")
                    else:
                        executed_rows = execute_paper_trades(staged_candidates, Path("data/paper_trading_logs_all.csv"), deduplicate=True)

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
    with st.expander("Debug Output"):
        st.write("Strategy selected:", strategy)
        st.write("Output rows type:", type(output_rows))
        st.write("Output sample:", output_rows[:5] if isinstance(output_rows, list) else output_rows)


if __name__ == "__main__":
    main()





















































