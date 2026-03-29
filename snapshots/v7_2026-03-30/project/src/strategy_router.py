from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from src.live_ohlcv import fetch_live_ohlcv
from src.trading_runtime_service import run_strategy as runtime_run_strategy
from src.trading_ui_service import apply_minimal_theme


def run_strategy(strategy: str, candles: Any, capital: float, risk_pct: float, rr_ratio: float, *, symbol: str = "^NSEI") -> list[dict[str, object]]:
    frame = candles if isinstance(candles, pd.DataFrame) else pd.DataFrame(candles)
    return runtime_run_strategy(
        strategy=strategy,
        candles=frame,
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=0.5,
        symbol=str(symbol),
        strike_step=50,
        moneyness="ATM",
        strike_steps=0,
        fetch_option_metrics=False,
        mtf_ema_period=3,
        mtf_setup_mode="either",
        mtf_retest_strength=True,
        mtf_max_trades_per_day=3,
    )


def render_strategy_router_page() -> None:
    apply_minimal_theme()
    st.title("Intratrade Algo Desk")
    st.sidebar.header("Bot Settings")

    capital = st.sidebar.number_input("Capital", 1000, 10000000, 100000, key="capital")
    risk_pct = st.sidebar.slider("Risk %", 0.1, 5.0, 1.0, key="risk")
    rr_ratio = st.sidebar.slider("RR Ratio", 1.0, 5.0, 2.0, key="rr")
    symbol = st.sidebar.text_input("Symbol", "^NSEI")
    interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m"])
    period = st.sidebar.selectbox("Period", ["1d", "5d", "1mo"])
    strategy = st.sidebar.selectbox(
        "Strategy",
        ["Breakout", "Demand Supply (Retest)", "Indicator", "One Trade Day"],
    )

    rows = fetch_live_ohlcv(symbol, interval, period)
    df = pd.DataFrame(rows)
    if df.empty:
        st.warning("No market data")
        return

    st.subheader("Market Data")
    st.dataframe(df.tail(20))

    signals = run_strategy(strategy, df, capital, risk_pct, rr_ratio, symbol=symbol)
    st.subheader("Trade Signals")
    if signals:
        st.dataframe(pd.DataFrame(signals))
    else:
        st.info("No signals generated")


__all__ = ["run_strategy", "render_strategy_router_page"]
