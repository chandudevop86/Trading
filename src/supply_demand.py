from __future__ import annotations

from collections.abc import Sequence
from math import floor
from typing import Any

import pandas as pd


def _coerce_candles(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, Sequence):
        rows: list[dict[str, Any]] = []
        for item in data:
            rows.append(
                {
                    "timestamp": getattr(item, "timestamp", None),
                    "open": getattr(item, "open", None),
                    "high": getattr(item, "high", None),
                    "low": getattr(item, "low", None),
                    "close": getattr(item, "close", None),
                    "volume": getattr(item, "volume", None),
                }
            )
        df = pd.DataFrame(rows)
    else:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df.columns = [str(col).strip().lower() for col in df.columns]
    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "timestamp" not in df.columns:
        df["timestamp"] = None

    return df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)


def _append_zone(
    zones: list[dict[str, Any]],
    seen: set[tuple[str, int, float, float, str]],
    *,
    zone_type: str,
    index: int,
    price: float,
    zone_low: float,
    zone_high: float,
    source: str,
    structure_level: float | None = None,
) -> None:
    key = (zone_type, index, round(price, 6), round(zone_low, 6), source)
    if key in seen:
        return
    seen.add(key)
    zone = {
        "type": zone_type,
        "price": round(float(price), 4),
        "index": int(index),
        "zone_low": round(float(zone_low), 4),
        "zone_high": round(float(zone_high), 4),
        "source": source,
    }
    if structure_level is not None:
        zone["structure_level"] = round(float(structure_level), 4)
    zones.append(zone)


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    if capital <= 0 or risk_pct <= 0:
        return 1
    return max(1, floor((capital * risk_pct) / risk_per_unit))


def _build_signal_from_zone(
    *,
    candle: pd.Series,
    zone: dict[str, Any],
    rr_ratio: float,
    capital: float,
    risk_pct: float,
) -> dict[str, Any] | None:
    open_price = float(candle["open"])
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])
    zone_low = float(zone["zone_low"])
    zone_high = float(zone["zone_high"])
    zone_mid = (zone_low + zone_high) / 2.0
    zone_type = str(zone["type"]).strip().lower()

    side = ""
    touched = False
    rejection_ok = False
    stop = 0.0

    touch_buffer = max(close * 0.003, 0.3)

    if zone_type == "demand":
        touched = low <= (zone_high + touch_buffer) and high >= (zone_low - touch_buffer)
        rejection_ok = close > open_price and close >= zone_mid
        stop = min(low, zone_low) - max(close * 0.001, 0.05)
        side = "BUY"
    elif zone_type == "supply":
        touched = high >= (zone_low - touch_buffer) and low <= (zone_high + touch_buffer)
        rejection_ok = close < open_price and close <= zone_mid
        stop = max(high, zone_high) + max(close * 0.001, 0.05)
        side = "SELL"
    else:
        return None

    if not touched or not rejection_ok:
        return None

    entry = close
    risk = abs(entry - stop)
    if risk <= 0:
        return None

    if side == "BUY":
        target = entry + (risk * rr_ratio)
    else:
        target = entry - (risk * rr_ratio)

    quantity = _calc_qty(capital, risk_pct, entry, stop)
    timestamp = candle.get("timestamp", "")

    return {
        "strategy": "DEMAND_SUPPLY",
        "timestamp": timestamp,
        "entry_time": timestamp,
        "side": side,
        "entry_price": round(entry, 4),
        "stop_loss": round(stop, 4),
        "trailing_stop_loss": round(stop, 4),
        "target_price": round(target, 4),
        "quantity": int(quantity),
        "signal": f"{zone_type.upper()}_RETEST",
        "zone_type": zone_type,
        "zone_low": round(zone_low, 4),
        "zone_high": round(zone_high, 4),
        "zone_source": zone.get("source", ""),
        "structure_level": zone.get("structure_level", ""),
    }


def generate_trades(
    df: Any,
    include_fvg: bool = True,
    include_bos: bool = True,
    capital: float = 100000.0,
    risk_pct: float = 0.01,
    rr_ratio: float = 2.0,
    **_: Any,
) -> list[dict[str, Any]]:
    candles = _coerce_candles(df)
    zones: list[dict[str, Any]] = []
    seen: set[tuple[str, int, float, float, str]] = set()
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []

    for i in range(2, len(candles) - 2):
        high = float(candles["high"].iloc[i])
        low = float(candles["low"].iloc[i])

        prev_high = float(candles["high"].iloc[i - 1])
        next_high = float(candles["high"].iloc[i + 1])

        prev_low = float(candles["low"].iloc[i - 1])
        next_low = float(candles["low"].iloc[i + 1])

        if high > prev_high and high > next_high:
            swing_highs.append((i, high))
            _append_zone(
                zones,
                seen,
                zone_type="supply",
                index=i,
                price=high,
                zone_low=high,
                zone_high=high,
                source="pivot",
            )

        if low < prev_low and low < next_low:
            swing_lows.append((i, low))
            _append_zone(
                zones,
                seen,
                zone_type="demand",
                index=i,
                price=low,
                zone_low=low,
                zone_high=low,
                source="pivot",
            )

    if include_fvg:
        for i in range(1, len(candles) - 1):
            prev_high = float(candles["high"].iloc[i - 1])
            prev_low = float(candles["low"].iloc[i - 1])
            next_high = float(candles["high"].iloc[i + 1])
            next_low = float(candles["low"].iloc[i + 1])

            if prev_high < next_low:
                zone_low = prev_high
                zone_high = next_low
                _append_zone(
                    zones,
                    seen,
                    zone_type="demand",
                    index=i,
                    price=(zone_low + zone_high) / 2.0,
                    zone_low=zone_low,
                    zone_high=zone_high,
                    source="fvg",
                )

            if prev_low > next_high:
                zone_low = next_high
                zone_high = prev_low
                _append_zone(
                    zones,
                    seen,
                    zone_type="supply",
                    index=i,
                    price=(zone_low + zone_high) / 2.0,
                    zone_low=zone_low,
                    zone_high=zone_high,
                    source="fvg",
                )

    if include_bos:
        for i in range(1, len(candles)):
            open_price = float(candles["open"].iloc[i])
            high = float(candles["high"].iloc[i])
            low = float(candles["low"].iloc[i])
            close = float(candles["close"].iloc[i])
            prev_close = float(candles["close"].iloc[i - 1])

            prior_swing_highs = [level for idx, level in swing_highs if idx < i]
            prior_swing_lows = [level for idx, level in swing_lows if idx < i]

            if prior_swing_highs:
                last_swing_high = prior_swing_highs[-1]
                if close > last_swing_high and prev_close <= last_swing_high:
                    zone_high = max(open_price, close)
                    _append_zone(
                        zones,
                        seen,
                        zone_type="demand",
                        index=i,
                        price=zone_high,
                        zone_low=low,
                        zone_high=zone_high,
                        source="bos",
                        structure_level=last_swing_high,
                    )

            if prior_swing_lows:
                last_swing_low = prior_swing_lows[-1]
                if close < last_swing_low and prev_close >= last_swing_low:
                    zone_low = min(open_price, close)
                    _append_zone(
                        zones,
                        seen,
                        zone_type="supply",
                        index=i,
                        price=zone_low,
                        zone_low=zone_low,
                        zone_high=high,
                        source="bos",
                        structure_level=last_swing_low,
                    )

    ordered_zones = sorted(zones, key=lambda zone: int(zone["index"]))
    signals: list[dict[str, Any]] = []

    for i in range(1, len(candles)):
        candle = candles.iloc[i]
        prior_zones = [zone for zone in ordered_zones if int(zone["index"]) < i]
        if not prior_zones:
            continue

        for zone in reversed(prior_zones[-6:]):
            signal = _build_signal_from_zone(
                candle=candle,
                zone=zone,
                rr_ratio=float(rr_ratio),
                capital=float(capital),
                risk_pct=float(risk_pct),
            )
            if signal is None:
                continue
            signal["zone_index"] = int(zone["index"])
            signal["zone_price"] = zone.get("price", "")
            signals.append(signal)
            break

    return signals


