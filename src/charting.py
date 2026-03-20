from __future__ import annotations

import altair as alt
import pandas as pd


UP_COLOR = "#16a34a"
DOWN_COLOR = "#dc2626"
BUY_COLOR = "#22c55e"
SELL_COLOR = "#ef4444"
CE_COLOR = "#22c55e"
PE_COLOR = "#f97316"


def add_vwap(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "vwap"])

    out = df.copy()
    typical_price = (out["high"] + out["low"] + out["close"]) / 3.0
    cumulative_volume = out["volume"].replace(0, pd.NA).ffill().fillna(1.0)
    out["vwap"] = ((typical_price * out["volume"]).cumsum() / cumulative_volume.cumsum()).astype(float)
    return out


def _prepare_price_frame(candles: pd.DataFrame) -> pd.DataFrame:
    df = candles.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp", "open", "high", "low", "close"]).reset_index(drop=True)


def compute_market_levels(candles: pd.DataFrame) -> dict[str, float]:
    if candles is None or candles.empty:
        return {
            "last_price": 0.0,
            "session_high": 0.0,
            "session_low": 0.0,
            "support_low": 0.0,
            "support_high": 0.0,
            "resistance_low": 0.0,
            "resistance_high": 0.0,
            "spread": 0.0,
            "opening_range_high": 0.0,
            "opening_range_low": 0.0,
            "prev_day_high": 0.0,
            "prev_day_low": 0.0,
            "cpr_pivot": 0.0,
            "cpr_top": 0.0,
            "cpr_bottom": 0.0,
        }

    df = _prepare_price_frame(candles)
    if df.empty:
        return {
            "last_price": 0.0,
            "session_high": 0.0,
            "session_low": 0.0,
            "support_low": 0.0,
            "support_high": 0.0,
            "resistance_low": 0.0,
            "resistance_high": 0.0,
            "spread": 0.0,
            "opening_range_high": 0.0,
            "opening_range_low": 0.0,
            "prev_day_high": 0.0,
            "prev_day_low": 0.0,
            "cpr_pivot": 0.0,
            "cpr_top": 0.0,
            "cpr_bottom": 0.0,
        }

    recent = df.tail(min(len(df), 12))
    support_low = float(recent["low"].nsmallest(min(len(recent), 3)).min())
    support_high = float(recent["low"].nsmallest(min(len(recent), 3)).max())
    resistance_low = float(recent["high"].nlargest(min(len(recent), 3)).min())
    resistance_high = float(recent["high"].nlargest(min(len(recent), 3)).max())
    last_price = float(df["close"].iloc[-1])

    opening = df.head(min(len(df), 3))
    opening_range_high = float(opening["high"].max())
    opening_range_low = float(opening["low"].min())

    session_day = df["timestamp"].dt.date.iloc[-1]
    prior = df[df["timestamp"].dt.date < session_day]
    prev_day_high = float(prior["high"].iloc[-1]) if not prior.empty else float(df["high"].iloc[0])
    prev_day_low = float(prior["low"].iloc[-1]) if not prior.empty else float(df["low"].iloc[0])

    day_high = float(df["high"].max())
    day_low = float(df["low"].min())
    day_close = float(df["close"].iloc[-1])
    pivot = (day_high + day_low + day_close) / 3.0
    bc = (day_high + day_low) / 2.0
    tc = (pivot - bc) + pivot
    cpr_bottom = min(bc, tc)
    cpr_top = max(bc, tc)

    return {
        "last_price": last_price,
        "session_high": day_high,
        "session_low": day_low,
        "support_low": support_low,
        "support_high": support_high,
        "resistance_low": resistance_low,
        "resistance_high": resistance_high,
        "spread": float(max(0.0, resistance_low - support_high)),
        "opening_range_high": opening_range_high,
        "opening_range_low": opening_range_low,
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
        "cpr_pivot": float(pivot),
        "cpr_top": float(cpr_top),
        "cpr_bottom": float(cpr_bottom),
    }


def build_market_depth_summary(candles: pd.DataFrame) -> pd.DataFrame:
    levels = compute_market_levels(candles)
    last_price = levels["last_price"]
    support_mid = (levels["support_low"] + levels["support_high"]) / 2.0 if levels["support_high"] else 0.0
    resistance_mid = (levels["resistance_low"] + levels["resistance_high"]) / 2.0 if levels["resistance_high"] else 0.0
    return pd.DataFrame(
        [
            {"level": "Resistance", "price": round(resistance_mid, 2), "distance": round(resistance_mid - last_price, 2)},
            {"level": "PDH", "price": round(levels["prev_day_high"], 2), "distance": round(levels["prev_day_high"] - last_price, 2)},
            {"level": "Last Price", "price": round(last_price, 2), "distance": 0.0},
            {"level": "Support", "price": round(support_mid, 2), "distance": round(last_price - support_mid, 2)},
            {"level": "PDL", "price": round(levels["prev_day_low"], 2), "distance": round(last_price - levels["prev_day_low"], 2)},
        ]
    )


def _signal_frame(output_rows: list[dict[str, object]] | None) -> pd.DataFrame:
    rows = output_rows or []
    points: list[dict[str, object]] = []
    for row in rows:
        side = str(row.get("side", "")).upper()
        ts = row.get("entry_time", row.get("timestamp", ""))
        price = row.get("entry_price", row.get("close", row.get("price", "")))
        option_type = str(row.get("option_type", "")).upper()
        option_strike = str(row.get("option_strike", "")).strip()
        if side not in {"BUY", "SELL"}:
            continue
        try:
            marker = option_type if option_type in {"CE", "PE"} else side
            points.append({
                "timestamp": pd.to_datetime(ts),
                "price": float(price),
                "side": side,
                "marker": marker,
                "option_strike": option_strike,
            })
        except Exception:
            continue
    if not points:
        return pd.DataFrame(columns=["timestamp", "price", "side", "marker", "option_strike"])
    return pd.DataFrame(points)


def build_live_market_chart(candles: pd.DataFrame, output_rows: list[dict[str, object]] | None = None) -> alt.Chart:
    if candles is None or candles.empty:
        return alt.Chart(pd.DataFrame({"timestamp": [], "close": []})).mark_line()

    df = _prepare_price_frame(candles)
    if df.empty:
        return alt.Chart(pd.DataFrame({"timestamp": [], "close": []})).mark_line()

    df["candle_color"] = df.apply(lambda row: UP_COLOR if row["close"] >= row["open"] else DOWN_COLOR, axis=1)
    signal_df = _signal_frame(output_rows)

    time_axis = alt.Axis(
        labelColor="#cbd5e1",
        titleColor="#cbd5e1",
        grid=False,
        format="%H:%M",
        tickCount=8,
        labelAngle=0,
        labelOverlap="greedy",
    )
    price_axis = alt.Axis(
        labelColor="#cbd5e1",
        titleColor="#cbd5e1",
        gridColor="#1e293b",
        format=",.2f",
        tickCount=8,
    )
    volume_axis = alt.Axis(
        labelColor="#94a3b8",
        titleColor="#94a3b8",
        gridColor="#172033",
        format=",.0f",
        tickCount=6,
    )

    base = alt.Chart(df).encode(
        x=alt.X("timestamp:T", title="Time", axis=time_axis)
    )

    wick = base.mark_rule(strokeWidth=1.2).encode(
        y=alt.Y("low:Q", title="Price", axis=price_axis, scale=alt.Scale(zero=False, nice=True)),
        y2="high:Q",
        color=alt.Color("candle_color:N", scale=None, legend=None),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("open:Q", format=".2f", title="Open"),
            alt.Tooltip("high:Q", format=".2f", title="High"),
            alt.Tooltip("low:Q", format=".2f", title="Low"),
            alt.Tooltip("close:Q", format=".2f", title="Close"),
            alt.Tooltip("volume:Q", format=",.0f", title="Volume"),
        ],
    )

    body = base.mark_bar(size=10).encode(
        y=alt.Y("open:Q", scale=alt.Scale(zero=False, nice=True)),
        y2="close:Q",
        color=alt.Color("candle_color:N", scale=None, legend=None),
    )

    layers: list[alt.Chart] = [wick, body]
    if not signal_df.empty:
        signal_points = alt.Chart(signal_df).mark_point(filled=True, size=100).encode(
            x=alt.X("timestamp:T", axis=time_axis),
            y=alt.Y("price:Q", axis=price_axis, scale=alt.Scale(zero=False, nice=True)),
            shape=alt.Shape("marker:N", scale=alt.Scale(domain=["BUY", "SELL", "CE", "PE"], range=["triangle-up", "triangle-down", "diamond", "square"]), legend=None),
            color=alt.Color("marker:N", scale=alt.Scale(domain=["BUY", "SELL", "CE", "PE"], range=[BUY_COLOR, SELL_COLOR, CE_COLOR, PE_COLOR]), legend=None),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Signal Time"),
                alt.Tooltip("side:N", title="Side"),
                alt.Tooltip("marker:N", title="Marker"),
                alt.Tooltip("option_strike:N", title="Option"),
                alt.Tooltip("price:Q", format=".2f", title="Price"),
            ],
        )
        layers.append(signal_points)

    price_panel = alt.layer(*layers).properties(height=430)
    volume = alt.Chart(df).mark_bar(opacity=0.78).encode(
        x=alt.X("timestamp:T", title="", axis=time_axis),
        y=alt.Y("volume:Q", title="Volume", axis=volume_axis, scale=alt.Scale(zero=True, nice=True)),
        color=alt.Color("candle_color:N", scale=None, legend=None),
        tooltip=[
            alt.Tooltip("timestamp:T", title="Time"),
            alt.Tooltip("volume:Q", format=",.0f", title="Volume"),
        ],
    ).properties(height=120)

    return alt.vconcat(price_panel, volume, spacing=10).resolve_scale(x="shared").configure_view(stroke=None).configure(background="#0b1220")
