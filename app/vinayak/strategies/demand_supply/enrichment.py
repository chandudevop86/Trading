from __future__ import annotations

import pandas as pd

from vinayak.strategies.demand_supply.normalization import normalize_ohlcv_for_supply_demand


def session_label(text: str) -> str:
    if "09:15:00" <= text <= "09:45:00":
        return "OPENING"
    if "09:45:01" <= text <= "11:30:00":
        return "MORNING"
    if "11:30:01" <= text <= "13:30:00":
        return "MIDDAY"
    if "13:30:01" <= text <= "15:30:00":
        return "AFTERNOON"
    return "OFFHOURS"


def enrich_supply_demand_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["range"] = (out["high"] - out["low"]).clip(lower=0.0)
    out["body"] = (out["close"] - out["open"]).abs()
    out["body_fraction"] = (out["body"] / out["range"].replace(0.0, pd.NA)).fillna(0.0)
    out["avg_range_20"] = out["range"].rolling(20, min_periods=1).mean()
    out["avg_volume_20"] = out["volume"].rolling(20, min_periods=1).mean().replace(0.0, pd.NA)
    out["volume_ratio"] = (out["volume"] / out["avg_volume_20"]).fillna(1.0)
    prev_close = out["close"].shift(1)
    tr = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr_14"] = tr.rolling(14, min_periods=1).mean()
    out["atr_pct"] = ((out["atr_14"] / out["close"].replace(0.0, pd.NA)) * 100.0).fillna(0.0)
    if "ema_9" not in out.columns:
        out["ema_9"] = out["close"].ewm(span=9, adjust=False).mean()
    if "ema_20" not in out.columns:
        out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    if "ema_21" not in out.columns:
        out["ema_21"] = out["close"].ewm(span=21, adjust=False).mean()
    if "ema_50" not in out.columns:
        out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()
    if "ema_200" not in out.columns:
        out["ema_200"] = out["close"].ewm(span=200, adjust=False).mean()
    macd_fast = out["ema_9"]
    macd_slow = out["ema_21"]
    if "macd" not in out.columns:
        out["macd"] = macd_fast - macd_slow
    if "macd_signal" not in out.columns:
        out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    if "macd_hist" not in out.columns:
        out["macd_hist"] = out["macd"] - out["macd_signal"]
    if "rsi" not in out.columns:
        delta = out["close"].diff().fillna(0.0)
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean().replace(0.0, float("nan"))
        rs = avg_gain.div(avg_loss)
        out["rsi"] = (100.0 - (100.0 / (1.0 + rs))).fillna(50.0)
    if "session" not in out.columns:
        out["session"] = out["timestamp"].dt.strftime("%H:%M:%S").map(session_label)
    return out


def prepare_supply_demand_frame(candles: pd.DataFrame) -> pd.DataFrame:
    return enrich_supply_demand_frame(normalize_ohlcv_for_supply_demand(candles))


__all__ = ["enrich_supply_demand_frame", "prepare_supply_demand_frame", "session_label"]
