from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
import csv

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # type: ignore

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore


def _to_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert a Yahoo chart-style payload into OHLCV rows.

    This is intentionally dependency-free so unit tests can run without network
    calls or third-party packages.
    """

    chart = payload.get("chart") if isinstance(payload, dict) else None
    if not isinstance(chart, dict):
        return []

    results = chart.get("result")
    if not isinstance(results, list) or not results:
        return []

    result = results[0]
    if not isinstance(result, dict):
        return []

    timestamps = result.get("timestamp")
    if not isinstance(timestamps, list) or not timestamps:
        return []

    indicators = result.get("indicators")
    if not isinstance(indicators, dict):
        return []

    quotes = indicators.get("quote")
    if not isinstance(quotes, list) or not quotes:
        return []

    quote = quotes[0]
    if not isinstance(quote, dict):
        return []

    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    rows: list[dict[str, Any]] = []
    for i, ts in enumerate(timestamps):
        if i >= len(opens) or i >= len(highs) or i >= len(lows) or i >= len(closes) or i >= len(volumes):
            break

        o = opens[i]
        h = highs[i]
        l = lows[i]
        c = closes[i]
        v = volumes[i]
        if o is None or h is None or l is None or c is None:
            continue

        try:
            ts_int = int(ts)
        except Exception:
            continue

        rows.append(
            {
                "timestamp": datetime.fromtimestamp(ts_int, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "price": c,
            }
        )

    return rows


def fetch_live_ohlcv(symbol: str, interval: str, period: str) -> list[dict[str, Any]]:
    if yf is None:
        raise ModuleNotFoundError("yfinance is required for fetch_live_ohlcv (pip install yfinance)")

    df = yf.download(
        tickers=symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
    )

    if df is None or getattr(df, "empty", False):
        return []

    df = df.reset_index()

    if pd is not None and isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    rename_map = {
        "Datetime": "timestamp",
        "Date": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "timestamp" not in df.columns:
        df = df.rename(columns={df.columns[0]: "timestamp"})

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    existing = [c for c in required if c in df.columns]
    df = df[existing].copy()

    df["price"] = df.get("close")

    return df.to_dict("records")


def write_csv(rows: list[dict[str, Any]], path: str) -> None:
    if not rows:
        return

    keys = list(rows[0].keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)