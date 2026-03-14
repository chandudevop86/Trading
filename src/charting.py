import altair as alt
import pandas as pd


def plot_candles(df):

    base = alt.Chart(df).encode(
        x=alt.X("timestamp:T", title="Time")
    )

    candles = base.mark_rule().encode(
        y="low:Q",
        y2="high:Q"
    )

    bars = base.mark_bar().encode(
        y="open:Q",
        y2="close:Q",
        color=alt.condition(
            "datum.open <= datum.close",
            alt.value("#26a69a"),
            alt.value("#ef5350")
        )
    )

    chart = (candles + bars).properties(
        width=900,
        height=400
    )

    return chart
def add_vwap(df):

    tp = (df["high"] + df["low"] + df["close"]) / 3

    df["vwap"] = (tp * df["volume"]).cumsum() / df["volume"].cumsum()

    return df
def vwap_line(df):

    line = alt.Chart(df).mark_line(
        color="yellow",
        strokeWidth=2
    ).encode(
        x="timestamp:T",
        y="vwap:Q"
    )

    return line
def signal_points(df):

    if "signal" not in df.columns:
        return None

    points = alt.Chart(df).mark_point(
        size=100,
        filled=True
    ).encode(
        x="timestamp:T",
        y="close:Q",
        color="signal:N"
    )

    return points
chart = candle_chart + vwap_chart + signal_points(df)