from __future__ import annotations

import csv
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib import parse, request


def build_url(symbol: str, interval: str, period: str) -> str:
    base = f"https://query1.finance.yahoo.com/v8/finance/chart/{parse.quote(symbol)}"
    qs = parse.urlencode({"interval": interval, "range": period})
    return f"{base}?{qs}"


def fetch_json(url: str) -> dict:
    req = request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def convert_to_rows(payload: dict):

    result = payload.get("chart", {}).get("result", [])
    if not result:
        return []

    frame = result[0]

    timestamps = frame["timestamp"]
    quote = frame["indicators"]["quote"][0]

    rows = []

    for i, ts in enumerate(timestamps):

        o = quote["open"][i]
        h = quote["high"][i]
        l = quote["low"][i]
        c = quote["close"][i]
        v = quote["volume"][i]

        if None in (o, h, l, c, v):
            continue

        dt = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")

        rows.append({
            "timestamp": dt,
            "open": round(float(o), 4),
            "high": round(float(h), 4),
            "low": round(float(l), 4),
            "close": round(float(c), 4),
            "volume": int(v),
            "price": round(float(c), 4)
        })

    return rows


def write_csv(path: Path, rows):

    path.parent.mkdir(exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "price",
            ],
        )

        writer.writeheader()
        writer.writerows(rows)


def run_live_feed(symbol="^NSEI", interval="5m", period="1d"):

    output = Path("data/live_ohlcv.csv")

    while True:

        try:

            url = build_url(symbol, interval, period)

            payload = fetch_json(url)

            rows = convert_to_rows(payload)

            if rows:
                write_csv(output, rows)
                print(f"Updated {len(rows)} candles")

        except Exception as e:
            print("Feed error:", e)

        time.sleep(30)   # update every 30 seconds
def get_live_rows(symbol="^NSEI", interval="5m", period="1d"):

    url = build_url(symbol, interval, period)

    payload = fetch_json(url)

    rows = convert_to_rows(payload)

    if rows:
        write_csv(Path("data/live_ohlcv.csv"), rows)

    return rows