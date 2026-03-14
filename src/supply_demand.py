import pandas as pd

def generate_trades(df):

    zones = []

    for i in range(2, len(df)-2):

        high = df["high"].iloc[i]
        low = df["low"].iloc[i]

        prev_high = df["high"].iloc[i-1]
        next_high = df["high"].iloc[i+1]

        prev_low = df["low"].iloc[i-1]
        next_low = df["low"].iloc[i+1]

        # Supply zone
        if high > prev_high and high > next_high:
            zones.append({
                "type":"supply",
                "price":high,
                "index":i
            })

        # Demand zone
        if low < prev_low and low < next_low:
            zones.append({
                "type":"demand",
                "price":low,
                "index":i
            })

    return zones