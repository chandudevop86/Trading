def detect_orderflow(df):

    signals = []

    avg_volume = df["volume"].rolling(20).mean()

    for i in range(20,len(df)):

        vol = df["volume"].iloc[i]

        if vol > avg_volume.iloc[i] * 2:

            signals.append({
                "index":i,
                "price":df["close"].iloc[i],
                "type":"institutional_activity"
            })

    return signals
def generate_trades(candles, capital, risk_pct, rr_ratio):

    trades = []

    if candles is None or len(candles) < 20:
        return trades

    last_close = float(candles["close"].iloc[-1])

    trades.append({
        "strategy": "Orderflow",
        "side": "BUY",
        "entry": last_close,
        "sl": last_close - 10,
        "target": last_close + 20,
    })

    return trades