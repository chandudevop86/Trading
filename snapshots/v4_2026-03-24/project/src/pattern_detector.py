import pandas as pd


def is_base(candle):
    body = abs(candle["close"] - candle["open"])
    range_candle = candle["high"] - candle["low"]

    if range_candle == 0:
        return False

    return body < (range_candle * 0.4)


def is_rally(candle):
    return candle["close"] > candle["open"]


def is_drop(candle):
    return candle["close"] < candle["open"]


def detect_patterns(df):
    patterns = []

    if df is None or len(df) < 3:
        return patterns

    df = df.copy()
    df.columns = df.columns.str.lower()

    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    for i in range(2, len(df)):
        c1 = df.iloc[i - 2]   # first candle
        c2 = df.iloc[i - 1]   # base candle
        c3 = df.iloc[i]       # third candle

        # RBR = Rally Base Rally
        if is_rally(c1) and is_base(c2) and is_rally(c3):
            patterns.append({
                "index": i,
                "pattern": "RBR",
                "price": float(c3["close"]),
            })

        # DBR = Drop Base Rally
        elif is_drop(c1) and is_base(c2) and is_rally(c3):
            patterns.append({
                "index": i,
                "pattern": "DBR",
                "price": float(c3["close"]),
            })

        # RBD = Rally Base Drop
        elif is_rally(c1) and is_base(c2) and is_drop(c3):
            patterns.append({
                "index": i,
                "pattern": "RBD",
                "price": float(c3["close"]),
            })

        # DBD = Drop Base Drop
        elif is_drop(c1) and is_base(c2) and is_drop(c3):
            patterns.append({
                "index": i,
                "pattern": "DBD",
                "price": float(c3["close"]),
            })

    return patterns


def generate_trades(df, patterns):
    trades = []

    for p in patterns:
        pattern_type = p["pattern"]
        index = p["index"]
        price = df.iloc[index]["close"]

        if pattern_type in ["RBR", "DBR"]:
            trades.append({
                "type": "BUY",
                "price": float(price),
                "pattern": pattern_type,
                "index": index,
            })

        elif pattern_type in ["RBD", "DBD"]:
            trades.append({
                "type": "SELL",
                "price": float(price),
                "pattern": pattern_type,
                "index": index,
            })

    return trades


if __name__ == "__main__":
    df = pd.read_csv("live_ohlcv.csv")
    df.columns = df.columns.str.lower()

    patterns = detect_patterns(df)
    trades = generate_trades(df, patterns)

    print("Patterns found:")
    for p in patterns:
        print(p)

    print("\nTrades generated:")
    for t in trades:
        print(t)