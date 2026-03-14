from __future__ import annotations

import sys
from pathlib import Path
import csv
import io
import os
  
import inspect

sys.path.insert(0, str(Path(__file__).parent.parent))

import altair as alt
import pandas as pd
import streamlit as st
import yfinance as yf

from src.breakout_bot import generate_trades as generate_breakout_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.telegram_notifier import build_trade_summary, send_telegram_message

try:
    from src.supply_demand import generate_trades as generate_demand_supply_trades
except Exception:
    generate_demand_supply_trades = None

try:
    from src.live_data_feed import run_live_feed, get_live_rows
except Exception:
    run_live_feed = None
    get_live_rows = None

try:
    from src.mtf_analysis import get_mtf_data
except Exception:
    get_mtf_data = None

try:
    from src.nifty_options import parse_args, run as run_nifty_options
except Exception:
    parse_args = None
    run_nifty_options = None

try:
    from src.auto_backtest import run as run_auto_backtest
except Exception:
    run_auto_backtest = None

try:
    from src.execution_engine import execute_paper_trades, execute_live_trades
except Exception:
    execute_paper_trades = None
    execute_live_trades = None

try:
    from src.orderflow import generate_trades as generate_orderflow_trades
except Exception:
    generate_orderflow_trades = None

try:
    from src.pattern_detector import generate_trades as generate_pattern_trades
except Exception:
    generate_pattern_trades = None

try:
    from src.price_action import (
        annotate_trades_with_zones,
        _classify_price_action,
        _is_pivot_high,
        _is_pivot_low,
    )
except Exception:
    annotate_trades_with_zones = None
    _classify_price_action = None
    _is_pivot_high = None
    _is_pivot_low = None


st.set_page_config(
    page_title="Trading Dashboard",
    page_icon="📈",
    layout="wide",
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
    elif "timestamp" not in df.columns and len(df.columns) > 0:
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "timestamp"})

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.reset_index(drop=True)
    return df


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


def send_signal_alert(trade: dict) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    msg = f"""
🚨 Trade Signal

    Strategy: {trade.get('strategy', 'Unknown')}
    Symbol: {trade.get('symbol', 'NIFTY')}
    Side: {trade.get('side', '-')}
    Entry: {trade.get('entry_price', '-')}
    SL: {trade.get('stop_loss', '-')}
    Target: {trade.get('target', '-')}
    Time: {trade.get('timestamp', '-')}
""".strip()

    try:
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
    except Exception as exc:
        st.warning(f"Telegram alert failed: {exc}")


def call_strategy_function(func, candles: pd.DataFrame, **kwargs):
    """
    Call a strategy function with only the arguments it actually supports.
    Prevents errors like:
    generate_trades() got an unexpected keyword argument 'capital'
    """
    sig = inspect.signature(func)
    accepted = sig.parameters

    filtered_kwargs = {k: v for k, v in kwargs.items() if k in accepted}

    try:
        return func(candles, **filtered_kwargs)
    except TypeError:
        # Fallback for functions that only take candles
        return func(candles)


def run_strategy(
    strategy: str,
    candles: pd.DataFrame,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
) -> list[dict]:
    if candles.empty:
        return []

    if strategy == "Indicator":
        return generate_indicator_rows(
            candles,
            config=IndicatorConfig(
                rsi_period=14,
                adx_period=14,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
            ),
        )

    strategy_kwargs = {
        "capital": capital,
        "risk_pct": risk_pct / 100,
        "rr_ratio": rr_ratio,
        "trailing_sl_pct": trailing_sl_pct / 100,
    }

    if strategy == "One Trade/Day":
        return call_strategy_function(
            generate_one_trade_day_trades,
            candles,
            **strategy_kwargs,
        )

    if strategy == "Demand Supply":
        if generate_demand_supply_trades is None:
            st.warning("Demand Supply module not available.")
            return []
        return call_strategy_function(
            generate_demand_supply_trades,
            candles,
            **strategy_kwargs,
        )

    if strategy == "Breakout":
        return call_strategy_function(
            generate_breakout_trades,
            candles,
            **strategy_kwargs,
        )

    return []


def main() -> None:
    st.title("📈 Intratrade Algo Desk")

    st.sidebar.header("Market Data")
    symbol = st.sidebar.text_input("Symbol", "^NSEI")
    interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h"], index=0)
    period = st.sidebar.selectbox("Period", ["1d", "5d", "1mo", "3mo"], index=0)

    st.sidebar.header("⚙ Bot Settings")
    capital = st.sidebar.number_input("Capital (₹)", min_value=1000, value=100000, step=1000)
    risk_pct = st.sidebar.slider("Risk per trade (%)", 0.1, 10.0, 1.0)
    rr_ratio = st.sidebar.slider("Risk/Reward Ratio", 1.0, 10.0, 2.0)
    trailing_sl_pct = st.sidebar.slider("Trailing Stop Loss %", 0.1, 10.0, 1.0, 0.1)

    strategy = st.sidebar.selectbox(
        "Strategy",
        ["Breakout", "Demand Supply", "Indicator", "One Trade/Day"],
    )

    send_telegram = st.sidebar.checkbox("Send Telegram alert for first signal", value=False)

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
        output_rows = run_strategy(
            strategy=strategy,
            candles=candles,
            capital=capital,
            risk_pct=risk_pct,
            rr_ratio=rr_ratio,
            trailing_sl_pct=trailing_sl_pct,
        )
    except Exception as exc:
        st.error(f"Strategy execution failed: {exc}")
        output_rows = []

    if send_telegram and output_rows:
        send_signal_alert(output_rows[0])

    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 Charts", "📋 Trades"])

    with tab1:
        st.subheader("Market Overview")
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

    with tab2:
        st.subheader("Live Price Chart")
        if not candles.empty:
            chart = (
                alt.Chart(candles)
                .mark_line()
                .encode(
                    x=alt.X("timestamp:T", title="Time"),
                    y=alt.Y("close:Q", title="Close Price"),
                    tooltip=["timestamp:T", "open:Q", "high:Q", "low:Q", "close:Q", "volume:Q"],
                )
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No chart data available.")

    with tab3:
        st.subheader("Execution Data")
        if output_rows:
            trades_df = pd.DataFrame(output_rows)
            st.dataframe(trades_df, use_container_width=True)

            try:
                summary = build_trade_summary(output_rows)
                st.text(summary)
            except Exception as exc:
                st.warning(f"Could not build trade summary: {exc}")

            csv_data = _to_csv(output_rows)
            st.download_button(
                "Download CSV",
                data=csv_data,
                file_name="trades.csv",
                mime="text/csv",
            )
        else:
            st.info("No trades generated yet.")

    with st.expander("Debug Output"):
        st.write("Strategy selected:", strategy)
        st.write("Output rows type:", type(output_rows))
        st.write("Output sample:", output_rows[:5] if isinstance(output_rows, list) else output_rows)


if __name__ == "__main__":
    main()