import streamlit as st
import pandas as pd

from src.live_ohlcv import fetch_live_ohlcv
from src.strategy_router import run_strategy

st.set_page_config(layout="wide")

st.title("📈 Intratrade Algo Desk")
st.sidebar.header("⚙ Bot Settings")

capital = st.sidebar.number_input(
    "Capital",
    1000,
    10000000,
    100000,
    key="capital"
)

risk_pct = st.sidebar.slider(
    "Risk %",
    0.1,
    5.0,
    1.0,
    key="risk"
)

rr_ratio = st.sidebar.slider(
    "RR Ratio",
    1.0,
    5.0,
    2.0,
    key="rr"
)
symbol = st.sidebar.text_input("Symbol", "^NSEI")
interval = st.sidebar.selectbox("Interval", ["1m","5m","15m","30m"])
period = st.sidebar.selectbox("Period", ["1d","5d","1mo"])
st.subheader("Select Strategy")

c1,c2,c3,c4 = st.columns(4)

if "strategy" not in st.session_state:
    st.session_state.strategy = "breakout"

with c1:
    if st.button("🚀 Breakout"):
        st.session_state.strategy = "breakout"

with c2:
    if st.button("📊 Demand Supply"):
        st.session_state.strategy = "ds"

with c3:
    if st.button("📈 Indicator"):
        st.session_state.strategy = "indicator"

with c4:
    if st.button("🧠 One Trade"):
        st.session_state.strategy = "one_trade"

strategy = st.session_state.strategy
rows = fetch_live_ohlcv(symbol, interval, period)

df = pd.DataFrame(rows)

if df.empty:
    st.warning("No market data")
    st.stop()

st.subheader("Market Data")
st.dataframe(df.tail(20))
signals = run_strategy(
    strategy,
    rows,
    capital,
    risk_pct,
    rr_ratio
)
st.subheader("Trade Signals")

if signals:
    st.dataframe(signals)
else:
    st.info("No signals generated")