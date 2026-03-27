from __future__ import annotations

from datetime import datetime

import pandas as pd


def build_signal_message(trade: dict[str, object], symbol: str = "NIFTY") -> str:
    return (
        f"Strategy: {trade.get('strategy', 'Unknown')}\n"
        f"Symbol: {trade.get('symbol', symbol)}\n"
        f"Side: {trade.get('side', '-')}\n"
        f"Entry: {trade.get('entry_price', '-')}\n"
        f"SL: {trade.get('stop_loss', '-')}\n"
        f"Target: {trade.get('target', '-')}\n"
        f"Time: {trade.get('timestamp', '-')}"
    )


def parse_timestamp(text: str) -> datetime:
    if not text:
        raise ValueError("Empty timestamp")

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {text}")


def prepare_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = df.copy().reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.columns = [str(c).strip().lower() for c in df.columns]

    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "timestamp"})
    elif "date" in df.columns:
        df = df.rename(columns={"date": "timestamp"})
    elif "timestamp" not in df.columns and len(df.columns) > 0:
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "timestamp"})

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.reset_index(drop=True)
    df["unix"] = df["timestamp"].astype("int64") // 10**9
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"])
    return df
