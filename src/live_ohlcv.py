import yfinance as yf

def fetch_live_ohlcv(symbol, interval, period):

    df = yf.download(symbol, interval=interval, period=period)

    df.reset_index(inplace=True)

    df.rename(columns={
        "Datetime":"timestamp",
        "Open":"open",
        "High":"high",
        "Low":"low",
        "Close":"close",
        "Volume":"volume"
    }, inplace=True)

    df["price"] = df["close"]

    return df.to_dict("records")
import csv

def write_csv(rows, path):
    if not rows:
        return

    keys = rows[0].keys()

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)