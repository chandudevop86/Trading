from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import csv
import io
import altair as alt
import pandas as pd
import streamlit as st
import yfinance as yf

from src.breakout_bot import generate_trades as generate_breakout_trades, load_candles
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.telegram_notifier import build_trade_summary, send_telegram_message
from src.indicator_bot import run as run_indicator_bot, build_indicator_summary,generate_indicator_rows
from src.live_data_feed import run_live_feed,get_live_rows
from src.mtf_analysis import get_mtf_data
from src.nifty_options import parse_args, run as run_nifty_options
from src.auto_backtest import run as run_auto_backtest
from src.execution_engine import execute_paper_trades, execute_live_trades
from src.orderflow import generate_trades as generate_orderflow_trades 
from src.pattern_detector import generate_trades as generate_pattern_trades
from src.mtf_analysis import get_mtf_data
from src.price_action import annotate_trades_with_zones, _classify_price_action, _is_pivot_high, _is_pivot_low

try:
    from src.supply_demand import generate_trades as generate_demand_supply_trades
except:
    generate_demand_supply_trades: None

   

    
token="8678219535:AAGCTn82ePsk0PuCO2yM-fxuTcTxE-OyBII",
chat_id="1605615725",

# --------------------------------
# Data Preparation
# --------------------------------
def prepare_trading_data(df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()

    # Fix MultiIndex columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # Normalize column names
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Rename date column
    if "date" in df.columns:
        df = df.rename(columns={"date": "datetime"})

    if "timestamp" in df.columns:
        df = df.rename(columns={"timestamp": "datetime"})

    required = ["datetime", "open", "high", "low", "close", "volume"]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df


# --------------------------------
# Convert rows to CSV
# --------------------------------
def _to_csv(rows):

    if not rows:
        return ""

    buffer = io.StringIO()

    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))

    writer.writeheader()

    writer.writerows(rows)

    return buffer.getvalue()


# --------------------------------
# Streamlit App
# --------------------------------
def main():
    st.info("Fetching live OHLCV data...")
output_rows = []
# --------------------------------
# Fetch Market Data
# --------------------------------
def fetch_ohlcv_data(symbol: str, interval: str = "1m", period: str = "1d") -> pd.DataFrame:
    """
    Fetch OHLCV data for a symbol using yfinance.
    Returns a DataFrame with columns: timestamp, open, high, low, close, volume
    """
    data = yf.download(tickers=symbol, interval=interval, period=period)
    if data.empty:
        return pd.DataFrame()  # empty DataFrame
    data.reset_index(inplace=True)
    data.rename(columns={"Open": "open","High": "high","Low": "low","Close": "close","Volume": "volume"}, inplace=True)
    return data[[ "open", "high", "low", "close", "volume"]]
symbol = st.sidebar.text_input("Symbol", "^NSEI")

interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h"], index=0)
period = st.sidebar.selectbox("Period", ["1d", "5d", "1mo", "3mo"], index=0)
try:
    df = yf.download(symbol, period=period, interval=interval)

    df = prepare_trading_data(df)

    rows = df.to_dict("records")


except Exception as e:
    st.error(f"Data fetch failed: {e}")

    candles = []

    # Debug info
candles = fetch_ohlcv_data(symbol)
st.caption(f"Total candles fetched: {len(candles)}")
with st.expander("View raw candle data"):
    st.dataframe(candles, use_container_width=True)
with st.expander("View raw candle data"):
    st.dataframe(candles, use_container_width=True)

with st.expander("View debug output"):
    st.write(output_rows)
st.dataframe(candles.tail(10), use_container_width=True)
     

    # -----------------------------
    # TOP NAVIGATION TABS
    # -----------------------------
st.set_page_config(
        page_title="Trading Dashboard",
        page_icon="📈", 
        layout="wide"
        )

st.title("📈 Intratrade Algo Desk")   
tab1, tab2, tab3 = st.tabs(
        ["📊 Dashboard", "📈 Charts",  "⚙️ Settings"]
    )
with tab1:
   st.subheader("Market Overview")
   c1, c2, c3, c4 = st.columns(4)

   latest_close = float(candles["close"].iloc[-1]) if "close" in candles.columns else 0
   latest_high = float(candles["high"].iloc[-1]) if "high" in candles.columns else 0
   latest_low = float(candles["low"].iloc[-1]) if "low" in candles.columns else 0
   latest_volume = float(candles["volume"].iloc[-1]) if "volume" in candles.columns else 0

   c1.metric("Close", round(latest_close, 2))
   c2.metric("High", round(latest_high, 2))
   c3.metric("Low", round(latest_low, 2))
   c4.metric("Volume", int(latest_volume))
with tab2:
    st.subheader("Live Price Chart")

    chart = alt.Chart(candles).mark_line().encode(
        x=alt.X("timestamp:T", title="Time"),
        y=alt.Y("close:Q", title="Close Price"),
        tooltip=["timestamp:T", "close:Q", "high:Q", "low:Q"]
    ).interactive()

    st.altair_chart(chart, use_container_width=True) 
with tab3:
    st.subheader("Execution Data")

    if "output_rows" in locals() and output_rows:
        import pandas as pd
        trades_df = pd.DataFrame(output_rows)
        st.dataframe(trades_df, use_container_width=True)
    else:
        st.info("No trades generated yet.")       

# Sidebar Settings
st.sidebar.header("⚙ Bot Settings")
trailing_sl_pct = st.sidebar.slider("Trailing Stop Loss %",min_value=0.1,max_value=10.0,value=1.0,step=0.1)
capital = st.sidebar.number_input("Capital (₹)", min_value=1000, value=100000, step=1000)
risk_pct = st.sidebar.slider("Risk per trade (%)", 0.1, 10.0, 1.0)
rr_ratio = st.sidebar.slider("Risk/Reward Ratio", 1.0, 10.0, 2.0)
trailing_sl = st.sidebar.slider("Trailing SL (%)", 0.1, 5.0, 1.0)
config = {
    "capital": capital,
    "risk_pct": risk_pct / 100,
    "rr_ratio": rr_ratio,
    "trailing_sl_pct": trailing_sl_pct
}
strategy = st.sidebar.selectbox(
    "Strategy",
    ["Breakout", "Demand Supply", "Indicator", "One Trade/Day"]
)
strategy = st.sidebar.
selectbox(
    "Select Strategy",
    [
        "Breakout Strategy",
        "Indicator Bot"
    ]
)
# --------------------------------
# Run Strategy
# --------------------------------
output_rows = []
execution_data = []
try:
    if strategy == "Indicator":
        output_rows = generate_indicator_rows(
            candles,
            config=IndicatorConfig(
                rsi_period=14,
                adx_period=14,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
            ),
        )

    elif strategy == "One Trade/Day":
            output_rows = generate_one_trade_day_trades(
                candles,
                capital=capital,
                risk_pct=risk_pct / 100,
                rr_ratio=rr_ratio,
        )

    elif strategy == "Demand/Supply" and generate_demand_supply_trades:
            output_rows = generate_demand_supply_trades(
                candles,
                capital=capital,
                risk_pct=risk_pct / 100,
                rr_ratio=rr_ratio,
        )
    
    elif strategy == "Breakout":
        
            output_rows = generate_breakout_trades(
               candles,
               capital=capital,
               risk_pct= risk_pct / 100,
               rr_ratio= rr_ratio,
               trailing_sl_pct= trailing_sl_pct / 100,
            )

            output_rows = generate_breakout_trades(
                candles,
                capital=capital,
                risk_pct=risk_pct / 100,
                rr_ratio=rr_ratio,
                trailing_sl_pct=trailing_sl_pct
)

except Exception as exc:
    st.error(f"Strategy execution failed: {exc}")
    st.write("Strategy selected:", strategy)
st.write("Output rows type:", type(output_rows))
st.write("Output rows:", output_rows[:5] if isinstance(output_rows, list) else output_rows)
st.write("Trades:", output_rows)
    # --------------------------------
    # Display Results
    # --------------------------------
st.subheader("Execution Data")
st.write(execution_data)
st.dataframe(output_rows, use_container_width=True)
if output_rows:
    st.dataframe(pd.DataFrame(output_rows), use_container_width=True)
else:
    st.info("No trades generated yet.")

# --------------------------------
# Download CSV
# --------------------------------
csv_data = _to_csv(output_rows)

st.download_button(
    "Download CSV",
    data=csv_data,
    file_name="trades.csv",
    mime="text/csv",
)

# --------------------------------
# Price Chart
# --------------------------------
if not df.empty:
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="datetime:T",
            y="close:Q",
        )
        .interactive()
    )

st.altair_chart(chart, use_container_width=True)
summary = build_trade_summary(output_rows)

#-------------------------
# Send Telegram Alert
#-------------------------
def send_signal_alert(trade):

    msg = f"""
🚨 Trade Signal

Strategy: {trade.get('strategy','Breakout')}
Symbol: {trade.get('symbol','NIFTY')}

Side: {trade.get('side')}
Entry: {trade.get('entry_price')}
SL: {trade.get('stop_loss')}
Target: {trade.get('target')}
Time: {trade.get('timestamp')}
"""

    send_telegram_message(
        TELEGRAM_TOKEN,
        TELEGRAM_CHAT_ID,
        msg
    )

if __name__ == "__main__":
    main()
