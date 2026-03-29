# ai_trading_dashboard.py

import streamlit as st
import pandas as pd
import yfinance as yf
import altair as alt

st.set_page_config(layout="wide")

st.title("AI Algo Trading Dashboard")

# --------------------------------------------------
# SETTINGS
# --------------------------------------------------

symbol = st.selectbox(
    "Select Index",
    ["^NSEI","^NSEBANK"]
)

refresh = st.slider("Refresh Seconds",10,120,30)

st.autorefresh(interval=refresh*1000)

# --------------------------------------------------
# FETCH LIVE DATA
# --------------------------------------------------

data = yf.download(symbol, interval="1m", period="1d")

df = data.reset_index()

df.columns = df.columns.str.lower()

# --------------------------------------------------
# INDICATORS
# --------------------------------------------------

def calculate_rsi(df, period=14):

    delta = df["close"].diff()

    gain = delta.clip(lower=0).rolling(period).mean()

    loss = (-delta.clip(upper=0)).rolling(period).mean()

    rs = gain / loss

    df["rsi"] = 100 - (100/(1+rs))

    return df


def calculate_vwap(df):

    tp = (df["high"]+df["low"]+df["close"])/3

    df["vwap"] = (tp*df["volume"]).cumsum()/df["volume"].cumsum()

    return df


df = calculate_rsi(df)
df = calculate_vwap(df)

# --------------------------------------------------
# SUPPLY DEMAND DETECTION
# --------------------------------------------------

def detect_supply_demand(df):

    supply = []
    demand = []

    for i in range(2,len(df)-2):

        if df["high"].iloc[i] > df["high"].iloc[i-1] and df["high"].iloc[i] > df["high"].iloc[i+1]:

            supply.append(df["high"].iloc[i])

        if df["low"].iloc[i] < df["low"].iloc[i-1] and df["low"].iloc[i] < df["low"].iloc[i+1]:

            demand.append(df["low"].iloc[i])

    return supply,demand


supply_zones,demand_zones = detect_supply_demand(df)

# --------------------------------------------------
# METRICS
# --------------------------------------------------

col1,col2,col3 = st.columns(3)

col1.metric("Live Price",round(df["close"].iloc[-1],2))
col2.metric("RSI",round(df["rsi"].iloc[-1],2))
col3.metric("VWAP",round(df["vwap"].iloc[-1],2))

# --------------------------------------------------
# PRICE CHART
# --------------------------------------------------

st.subheader("Price Chart")

price_chart = alt.Chart(df).mark_line().encode(
    x="datetime:T",
    y="close:Q"
)

supply_chart = alt.Chart(
    pd.DataFrame({"price":supply_zones})
).mark_rule(color="red").encode(
    y="price:Q"
)

demand_chart = alt.Chart(
    pd.DataFrame({"price":demand_zones})
).mark_rule(color="green").encode(
    y="price:Q"
)

st.altair_chart(
    price_chart + supply_chart + demand_chart,
    use_container_width=True
)

# --------------------------------------------------
# ZONE TABLE
# --------------------------------------------------

st.subheader("Supply Demand Zones")

zones = []

for s in supply_zones:
    zones.append({"type":"Supply","price":s})

for d in demand_zones:
    zones.append({"type":"Demand","price":d})

zones_df = pd.DataFrame(zones)

st.dataframe(zones_df)

# --------------------------------------------------
# OPTION STRIKE CALCULATOR
# --------------------------------------------------

st.subheader("Option Strike Calculator")

moneyness = st.selectbox(
    "Select Moneyness",
    ["ATM","ITM","OTM"]
)

def get_option_strike(price,step=50):

    atm = round(price/step)*step

    if moneyness=="ATM":
        return atm

    elif moneyness=="ITM":
        return atm-step

    elif moneyness=="OTM":
        return atm+step


strike = get_option_strike(df["close"].iloc[-1])

st.success(f"Suggested Strike: {strike}")



# ai_trading_dashboard.py

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import yfinance as yf
import time

# --------------------------------------------------
#        Live Price Fetching# 
#---------------------------------------------------

def fetch_live_price(symbol):

    ticker_map = {
        "NIFTY": "^NSEI",
        "BANKNIFTY": "^NSEBANK",
        "FINNIFTY": "NIFTY_FIN_SERVICE.NS"
    }

    ticker = ticker_map.get(symbol)

    data = yf.Ticker(ticker)

    price = data.history(period="1d", interval="1m")["Close"].iloc[-1]

    return float(price)
symbol = st.selectbox(
    "Select Index",
    ["NIFTY","BANKNIFTY","FINNIFTY"]
)

price = fetch_live_price(symbol)

st.metric("Live Price", round(price,2))

data = yf.download("NIFTYBEES.NS", interval="1m", period="1d")

ohlc = data[['Open','High','Low','Close']]

print(ohlc.tail())    

symbol = "^NSEI"   # NIFTY
symbol = st.text_input("Enter Symbol", "^NSEI")

refresh = st.slider("Refresh seconds", 10, 120, 30)

st.title("Live OHLCV Data")

while True:
    
    data = yf.download(symbol, interval="1m", period="1d")
    
    ohlcv = data[['Open','High','Low','Close','Volume']]
    
    st.dataframe(ohlcv.tail(10))
    
    time.sleep(refresh)
    st.rerun()

# --------------------------------------------------

# INDICATORS
# --------------------------------------------------

def calculate_rsi(df, period=14):

    delta = df["close"].diff()

    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()

    rs = gain / loss

    df["rsi"] = 100 - (100 / (1 + rs))

    return df


def calculate_vwap(df):

    df["tp"] = (df["high"] + df["low"] + df["close"]) / 3

    df["vwap"] = (df["tp"] * df["volume"]).cumsum() / df["volume"].cumsum()

    return df


def calculate_indicators(df):

    df = calculate_rsi(df)

    df = calculate_vwap(df)

    df["volume_ma"] = df["volume"].rolling(20).mean()

    return df


# --------------------------------------------------
# SUPPLY / DEMAND DETECTION
# --------------------------------------------------

def detect_zones(df, window=2):

    zones = []

    for i in range(window, len(df) - window):

        high = df["high"].iloc[i]
        low = df["low"].iloc[i]

        if high == df["high"].iloc[i-window:i+window].max():

            zones.append({
                "type": "supply",
                "price": high,
                "timestamp": df["timestamp"].iloc[i]
            })

        if low == df["low"].iloc[i-window:i+window].min():

            zones.append({
                "type": "demand",
                "price": low,
                "timestamp": df["timestamp"].iloc[i]
            })

    return zones


# --------------------------------------------------
# BREAKOUT STRATEGY
# --------------------------------------------------

def generate_breakout_trades(df, capital, risk_pct, rr):

    trades = []

    for i in range(20, len(df)):

        prev_high = df["high"].iloc[i-20:i].max()

        price = df["close"].iloc[i]

        if price > prev_high:

            entry = price

            sl = df["low"].iloc[i]

            risk = entry - sl

            if risk <= 0:
                continue

            target = entry + (risk * rr)

            position_size = (capital * risk_pct) / risk

            trades.append({

                "timestamp": df["timestamp"].iloc[i],
                "entry": entry,
                "sl": sl,
                "target": target,
                "qty": round(position_size,2),
                "close": price,
                "rsi": df["rsi"].iloc[i],
                "vwap": df["vwap"].iloc[i],
                "volume": df["volume"].iloc[i],
                "volume_ma": df["volume_ma"].iloc[i]

            })

    return trades
  
  

# --------------------------------------------------
# AI TRADE FILTER
# --------------------------------------------------

def score_trade(row):

    score = 0

    if row["rsi"] > 55:
        score += 1

    if row["close"] > row["vwap"]:
        score += 1

    if row["volume"] > row["volume_ma"]:
        score += 1

    probability = score / 3

    return probability


def filter_ai_trades(trades):

    filtered = []

    for t in trades:

        probability = score_trade(t)

        if probability > 0.66:

            t["probability"] = probability

            filtered.append(t)

    return filtered


# --------------------------------------------------
# STREAMLIT UI
# --------------------------------------------------

st.set_page_config(layout="wide")

st.title("AI Algo Trading Dashboard")

uploaded = st.file_uploader("Upload OHLCV CSV")


if uploaded:

    df = pd.read_csv(uploaded)

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    capital = st.number_input("Capital ₹", value=100000)

    risk_pct = st.slider("Risk %",0.1,5.0,1.0)

    rr = st.slider("Risk Reward",1.0,5.0,2.0)

    df = calculate_indicators(df)

    zones = detect_zones(df)

    trades = generate_breakout_trades(df, capital, risk_pct/100, rr)

    trades = filter_ai_trades(trades)

    trades_df = pd.DataFrame(trades)

    # --------------------------------------------------
    # METRICS
    # --------------------------------------------------

    st.subheader("Performance")

    if not trades_df.empty:

        pnl = ((trades_df["target"] - trades_df["entry"]) * trades_df["qty"]).sum()

        col1,col2 = st.columns(2)

        col1.metric("Trades",len(trades_df))

        col2.metric("Potential PnL ₹",round(pnl,2))

    else:

        st.warning("No trades generated")


    # --------------------------------------------------
    # PRICE CHART
    # --------------------------------------------------

    st.subheader("Price Chart")

    chart = alt.Chart(df).mark_line().encode(
        x="timestamp:T",
        y="close:Q"
    ).interactive()

    st.altair_chart(chart,use_container_width=True)


    # --------------------------------------------------
    # ZONES DISPLAY
    # --------------------------------------------------

    st.subheader("Supply / Demand Zones")

    if zones:

        st.dataframe(pd.DataFrame(zones))


    # --------------------------------------------------
    # TRADES
    # --------------------------------------------------
def get_option_strike(price, step=60):

    strike = round(price / step) * step
    return strike
moneyness = st.selectbox(
    "Select Moneyness",
    ["ATM", "ITM", "OTM"]
)
   # example value

def get_option_strike(price, strike_step=65, moneyness="ATM", steps=1):

    atm = round(price / strike_step) * strike_step

    if moneyness == "ATM":
        return atm
    elif moneyness == "ITM":
        return atm - (strike_step * steps)
    elif moneyness == "OTM":
        return atm + (strike_step * steps)

def detect_supply_demand(df):

    supply_zones = []
    demand_zones = []

    for i in range(2, len(df)-2):

        # Supply
        if df['High'][i] > df['High'][i-1] and df['High'][i] > df['High'][i+1]:
            supply_zones.append(df['High'][i])

        # Demand
        if df['Low'][i] < df['Low'][i-1] and df['Low'][i] < df['Low'][i+1]:
            demand_zones.append(df['Low'][i])

    return supply_zones, demand_zones



