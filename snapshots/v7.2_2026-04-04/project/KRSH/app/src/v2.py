from __future__ import annotations

import csv
import io
import inspect
import sys
from datetime import datetime, timedelta
from math import floor
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

from src.breakout_bot import Candle
from src.breakout_bot import generate_trades as generate_breakout_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes, pick_option_strike
from src.telegram_notifier import build_trade_summary, send_telegram_message

try:
    from src.supply_demand import generate_trades as generate_demand_supply_trades
except Exception:
    generate_demand_supply_trades = None

try:
    from src.nse_option_chain import (
        build_metrics_map,
        extract_option_records,
        fetch_option_chain,
        normalize_index_symbol,
    )
except Exception:
    build_metrics_map = None
    extract_option_records = None
    fetch_option_chain = None
    normalize_index_symbol = None


st.set_page_config(page_title="Trading Dashboard", page_icon="📈", layout="wide")

TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "")


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
    elif "timestamp" not in df.columns:
        df = df.rename(columns={df.columns[0]: "timestamp"})

    required = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required].copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df.drop_duplicates(subset=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)

    df["unix"] = df["timestamp"].astype("int64") // 10**9
    return df


def _df_to_candles(df: pd.DataFrame) -> list[Candle]:
    if df is None or df.empty:
        return []
    if "timestamp" not in df.columns:
        raise ValueError("Candles missing timestamp column")

    candles: list[Candle] = []
    for row in df.itertuples(index=False):
        ts = getattr(row, "timestamp", None)
        if ts is None:
            continue
        if isinstance(ts, pd.Timestamp):
            ts_dt = ts.to_pydatetime()
        else:
            ts_dt = pd.to_datetime(ts, errors="coerce")
            if pd.isna(ts_dt):
                continue
            ts_dt = ts_dt.to_pydatetime()

        candles.append(
            Candle(
                timestamp=ts_dt,
                open=float(getattr(row, "open", 0.0) or 0.0),
                high=float(getattr(row, "high", 0.0) or 0.0),
                low=float(getattr(row, "low", 0.0) or 0.0),
                close=float(getattr(row, "close", 0.0) or 0.0),
                volume=float(getattr(row, "volume", 0.0) or 0.0),
            )
        )

    candles.sort(key=lambda c: c.timestamp)
    return candles


def fetch_ohlcv_data(symbol: str, interval: str = "1m", period: str = "1d") -> pd.DataFrame:
    data = yf.download(
        tickers=symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
    )
    if data is None or data.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return prepare_trading_data(data)


def _to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _fmt_num(val: object) -> str:
    if val is None:
        return "-"
    text = str(val).strip()
    if text in {"", "-", "N/A"}:
        return "-"
    try:
        num = float(text)
    except Exception:
        return text
    out = f"{num:.2f}"
    return out[:-3] if out.endswith(".00") else out


def _format_expiry(expiry: object) -> str:
    if expiry is None:
        return ""
    text = str(expiry).strip()
    if not text or text in {"-", "N/A"}:
        return ""
    for fmt in ("%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


def _format_ts_ist(ts: object) -> str:
    if ts is None:
        return "-"
    try:
        dt = pd.to_datetime(ts, errors="coerce")
    except Exception:
        dt = None
    if dt is None or pd.isna(dt):
        return str(ts)

    if isinstance(dt, pd.Timestamp):
        py = dt.to_pydatetime()
    else:
        py = dt

    if getattr(py, "tzinfo", None) is None:
        py = py.replace(tzinfo=ZoneInfo("UTC"))

    ist = py.astimezone(ZoneInfo("Asia/Kolkata"))
    return ist.strftime("%Y-%m-%d %H:%M:%S IST")



def _estimate_weekly_expiry(symbol: str, now: datetime | None = None) -> str:
    s = (symbol or "").strip().upper()
    if s in {"^NSEI", "NIFTY", "NIFTY 50", "NIFTY50"}:
        tz = ZoneInfo("Asia/Kolkata")
        dt = now or datetime.now(tz)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        # Thursday = 3 (Mon=0)
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ""

def attach_lots(rows: list[dict[str, object]], lot_size: int, lots: int) -> list[dict[str, object]]:
    lot_size = int(lot_size) if lot_size and int(lot_size) > 0 else 0
    lots = int(lots) if lots and int(lots) > 0 else 0
    if lot_size <= 0 or lots <= 0:
        return rows

    qty = lot_size * lots
    out: list[dict[str, object]] = []
    for r in rows:
        row = dict(r)
        row["lots"] = lots
        row["quantity"] = qty
        try:
            ltp = float(row.get("option_ltp", 0) or 0)
        except Exception:
            ltp = 0.0
        if ltp > 0:
            row["order_value"] = round(ltp * qty, 2)
        out.append(row)
    return out


def send_signal_alert(
    trade: dict[str, object],
    strategy: str,
    symbol: str,
    refresh_seconds: int | None = None,
) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    side = str(trade.get("side", "-") or "-")
    entry = _fmt_num(trade.get("entry_price", trade.get("entry", "-")))
    sl = _fmt_num(trade.get("stop_loss", trade.get("sl", "-")))
    target = _fmt_num(trade.get("target_price", trade.get("target", "-")))
    option = str(trade.get("option_strike", "") or "").strip()

    opt_ltp = _fmt_num(trade.get("option_ltp"))
    opt_oi = _fmt_num(trade.get("option_oi"))
    opt_vol = _fmt_num(trade.get("option_vol"))
    opt_iv = _fmt_num(trade.get("option_iv"))
    opt_expiry = _format_expiry(trade.get("option_expiry"))
    opt_expiry_source = str(trade.get("option_expiry_source", "") or "").upper()
    if opt_expiry and opt_expiry_source == "EST":
        opt_expiry = opt_expiry + " (est)"

    lots = str(trade.get("lots", "") or "").strip()
    qty = str(trade.get("quantity", "") or "").strip()
    value = _fmt_num(trade.get("order_value"))

    ts = _format_ts_ist(trade.get("timestamp") or trade.get("entry_time"))

    extra = ""
    if refresh_seconds is not None:
        if refresh_seconds >= 60:
            extra = f" (next update in {refresh_seconds // 60} min)"
        else:
            extra = f" (next update in {refresh_seconds} sec)"

    parts: list[str] = [
        "🚨 Trade Signal",
        "",
        f"Strategy: {strategy}",
        f"Symbol: {symbol}",
        f"Side: {side}",
    ]
    if entry != "-":
        parts.append(f"Entry: {entry}")
    if sl != "-":
        parts.append(f"SL: {sl}")
    if target != "-":
        parts.append(f"Target: {target}")
    if option:
        parts.append(f"Option: {option}")
    if opt_expiry:
        parts.append(f"Expiry: {opt_expiry}")
    if opt_ltp != "-":
        parts.append(f"LTP: {opt_ltp}")
    if opt_oi != "-":
        parts.append(f"OI: {opt_oi}")
    if opt_vol != "-":
        parts.append(f"Vol: {opt_vol}")
    if opt_iv != "-":
        parts.append(f"IV: {opt_iv}")
    if lots and qty:
        parts.append(f"Lots: {lots} (Qty: {qty})")
    if value != "-":
        parts.append(f"Value: {value}")

    parts.append(f"Time: {ts}{extra}")

    msg = "\n".join(parts)

    try:
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, msg)
    except Exception as exc:
        st.warning(f"Telegram alert failed: {exc}")


def call_strategy_function(func, candles, **kwargs):
    sig = inspect.signature(func)
    accepted = sig.parameters
    filtered_kwargs = {k: v for k, v in kwargs.items() if k in accepted}
    try:
        return func(candles, **filtered_kwargs)
    except TypeError:
        return func(candles)


def _indicator_side(signal: str) -> str:
    s = (signal or "").upper()
    if s in {"BULLISH_TREND", "OVERSOLD", "BUY"}:
        return "BUY"
    if s in {"BEARISH_TREND", "OVERBOUGHT", "SELL"}:
        return "SELL"
    return "-"


def _attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
    for r in rows:
        row = dict(r)
        side = _indicator_side(str(row.get("market_signal", "")))
        row.setdefault("side", side)

        try:
            entry = float(row.get("close", 0.0) or 0.0)
        except Exception:
            entry = 0.0

        if entry > 0 and side in {"BUY", "SELL"}:
            row["entry_price"] = round(entry, 2)
            if sl_frac <= 0:
                sl_frac = 0.002
            if side == "BUY":
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)
            row["stop_loss"] = round(sl, 2)
            row["target_price"] = round(tp, 2)

        if "timestamp" in row:
            row["timestamp"] = str(row["timestamp"])
        out.append(row)

    return out


def run_strategy(
    strategy: str,
    candles: pd.DataFrame,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
    symbol: str,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    fetch_option_metrics: bool,
) -> list[dict[str, object]]:
    if candles.empty:
        return []

    strategy_kwargs = {
        "capital": float(capital),
        "risk_pct": float(risk_pct) / 100.0,
        "rr_ratio": float(rr_ratio),
        "trailing_sl_pct": float(trailing_sl_pct) / 100.0,
    }

    metrics_map: dict[tuple[int, str], dict[str, object]] = {}
    if fetch_option_metrics and fetch_option_chain and extract_option_records and build_metrics_map and normalize_index_symbol:
        try:
            sym = normalize_index_symbol(symbol)
            payload = fetch_option_chain(sym, timeout=10.0)
            records = extract_option_records(payload)
            metrics_map = build_metrics_map(records)  # type: ignore[assignment]
        except Exception:
            metrics_map = {}

    if strategy == "Demand Supply":
        if generate_demand_supply_trades is None:
            st.warning("Demand Supply module not available.")
            return []
        zones = call_strategy_function(generate_demand_supply_trades, candles, **strategy_kwargs)

        out_rows: list[dict[str, object]] = []
        sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
        if sl_frac <= 0:
            sl_frac = 0.002

        for z in zones or []:
            if not isinstance(z, dict):
                continue
            zone_type = str(z.get("type", "")).lower()
            side = "BUY" if zone_type == "demand" else "SELL" if zone_type == "supply" else "-"
            if side == "-":
                continue
            try:
                entry = float(z.get("price", 0.0) or 0.0)
            except Exception:
                entry = 0.0
            if entry <= 0:
                continue

            ts = ""
            try:
                idx = int(z.get("index", -1))
                if 0 <= idx < len(candles):
                    ts = str(candles["timestamp"].iloc[idx])
            except Exception:
                ts = ""

            if side == "BUY":
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)

            row: dict[str, object] = {
                "strategy": "Demand Supply",
                "symbol": symbol,
                "zone_type": zone_type,
                "side": side,
                "entry_price": round(entry, 2),
                "stop_loss": round(sl, 2),
                "target_price": round(tp, 2),
                "timestamp": ts,
            }

            try:
                strike, opt = pick_option_strike(
                    spot_price=float(row["entry_price"]),
                    side=str(row["side"]),
                    step=int(strike_step),
                    moneyness=str(moneyness),
                    steps=int(strike_steps),
                )
                row["option_type"] = opt
                row["strike_price"] = strike
                row["option_strike"] = f"{strike}{opt}"

                metrics = metrics_map.get((int(strike), str(opt)))

                if isinstance(metrics, dict):

                    row.update(metrics)

                    if metrics.get("option_expiry"):

                        row["option_expiry_source"] = "NSE"
            except Exception:
                pass

            if not row.get("option_expiry") and row.get("option_strike"):
                est = _estimate_weekly_expiry(symbol)
                if est:
                    row["option_expiry"] = est
                    row["option_expiry_source"] = "EST"

            if "option_expiry" in row:
                row["option_expiry"] = _format_expiry(row.get("option_expiry"))

            out_rows.append(row)

        return out_rows

    if strategy == "Indicator":
        candle_list = _df_to_candles(candles)
        rows = generate_indicator_rows(
            candle_list,
            config=IndicatorConfig(
                rsi_period=14,
                adx_period=14,
                macd_fast=12,
                macd_slow=26,
                macd_signal=9,
            ),
        )
        rows = _attach_indicator_trade_levels(rows, rr_ratio=rr_ratio, trailing_sl_pct=trailing_sl_pct)
        rows = [dict(r, strategy="Indicator", symbol=symbol) for r in rows]

        actionable = [r for r in rows if str(r.get("side")) in {"BUY", "SELL"} and r.get("entry_price")]
        if actionable:
            actionable = attach_option_strikes(actionable, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
            for r in actionable:
                strike = int(r.get("strike_price") or 0)
                opt = str(r.get("option_type") or "")
                metrics = metrics_map.get((strike, opt))
                if isinstance(metrics, dict):
                    r.update(metrics)
                    if metrics.get("option_expiry"):
                        r["option_expiry_source"] = "NSE"

                if not r.get("option_expiry") and r.get("option_strike"):
                    est = _estimate_weekly_expiry(symbol)
                    if est:
                        r["option_expiry"] = est
                        r["option_expiry_source"] = "EST"

                if "option_expiry" in r:
                    r["option_expiry"] = _format_expiry(r.get("option_expiry"))

            last_actionable = actionable[-1]
            for i in range(len(rows) - 1, -1, -1):
                if str(rows[i].get("timestamp")) == str(last_actionable.get("timestamp")):
                    rows[i].update(last_actionable)
                    break

        return rows[-200:]

    if strategy == "One Trade/Day":
        candle_list = _df_to_candles(candles)
        trades = call_strategy_function(generate_one_trade_day_trades, candle_list, **strategy_kwargs)
    elif strategy == "Breakout":
        candle_list = _df_to_candles(candles)
        trades = call_strategy_function(generate_breakout_trades, candle_list, **strategy_kwargs)
    else:
        trades = []

    out_rows: list[dict[str, object]] = []
    for t in trades or []:
        if not isinstance(t, dict):
            continue
        row = dict(t)
        row.setdefault("strategy", strategy)
        row.setdefault("symbol", symbol)

        if "timestamp" not in row:
            if "entry_time" in row:
                row["timestamp"] = str(row.get("entry_time"))
            elif "time" in row:
                row["timestamp"] = str(row.get("time"))

        if "target" in row and "target_price" not in row:
            row["target_price"] = row.get("target")

        if "entry_price" in row and "side" in row:
            try:
                annotated = attach_option_strikes([row], strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
                row = annotated[0]
                strike = int(row.get("strike_price") or 0)
                opt = str(row.get("option_type") or "")
                metrics = metrics_map.get((strike, opt))
                if isinstance(metrics, dict):
                    row.update(metrics)
                    if metrics.get("option_expiry"):
                        row["option_expiry_source"] = "NSE"

                if not row.get("option_expiry") and row.get("option_strike"):
                    est = _estimate_weekly_expiry(symbol)
                    if est:
                        row["option_expiry"] = est
                        row["option_expiry_source"] = "EST"

                if "option_expiry" in row:
                    row["option_expiry"] = _format_expiry(row.get("option_expiry"))
            except Exception:
                pass

        out_rows.append(row)

    return out_rows


def main() -> None:
    st.title("📈 Intratrade Algo Desk")

    st.sidebar.header("Market Data")
    symbol = st.sidebar.text_input("Symbol", "^NSEI")
    interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h"], index=0)
    period = st.sidebar.selectbox("Period", ["1d", "5d", "1mo", "3mo"], index=0)

    st.sidebar.header("⚙ Bot Settings")
    capital = st.sidebar.number_input("Capital (INR)", min_value=1000, value=100000, step=1000)
    risk_pct = st.sidebar.slider("Risk per trade (%)", 0.1, 10.0, 1.0)
    rr_ratio = st.sidebar.slider("Risk/Reward Ratio", 1.0, 10.0, 2.0)
    trailing_sl_pct = st.sidebar.slider("Trailing Stop Loss %", 0.1, 10.0, 1.0, 0.1)

    strategy = st.sidebar.selectbox("Strategy", ["Breakout", "Demand Supply", "Indicator", "One Trade/Day"])

    st.sidebar.header("🧾 Option Settings")
    strike_step = st.sidebar.selectbox("Strike step", [25, 50, 100], index=1)
    moneyness = st.sidebar.selectbox("Moneyness", ["ATM", "ITM", "OTM"], index=0)
    strike_steps = st.sidebar.slider("Steps (ITM/OTM)", 0, 5, 0)
    fetch_option_metrics = st.sidebar.checkbox("Fetch option LTP/OI/Vol/IV + Expiry", value=False)

    lot_size = st.sidebar.number_input("Lot size", min_value=1, value=65, step=1)
    lots = st.sidebar.slider("Lots (qty = lots × lot size)", 1, 10, 1)

    st.sidebar.header("Live Update")
    live_update = st.sidebar.checkbox("Auto refresh", value=False)
    refresh_seconds = st.sidebar.slider("Refresh every (seconds)", 2, 120, 10)

    send_telegram = st.sidebar.checkbox("Send Telegram alert (latest signal)", value=False)

    if live_update:
        components.html(
            f"""<script>
                const ms = {int(refresh_seconds)} * 1000;
                setTimeout(() => window.location.reload(), ms);
            </script>""",
            height=0,
        )

    st.info("Fetching live OHLCV data...")
    try:
        candles = fetch_ohlcv_data(symbol, interval=interval, period=period)
    except Exception as exc:
        st.error(f"Data fetch failed: {exc}")
        candles = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    st.caption(f"Total candles fetched: {len(candles)}")

    with st.expander("View raw candle data"):
        st.dataframe(candles, use_container_width=True)

    try:
        output_rows = run_strategy(
            strategy=strategy,
            candles=candles,
            capital=capital,
            risk_pct=risk_pct,
            rr_ratio=rr_ratio,
            trailing_sl_pct=trailing_sl_pct,
            symbol=symbol,
            strike_step=int(strike_step),
            moneyness=str(moneyness),
            strike_steps=int(strike_steps),
            fetch_option_metrics=bool(fetch_option_metrics),
        )
    except Exception as exc:
        st.error(f"Strategy execution failed: {exc}")
        output_rows = []

    if output_rows:
        output_rows = attach_lots(output_rows, lot_size=int(lot_size), lots=int(lots))

    if send_telegram and output_rows:
        latest = None
        for r in reversed(output_rows):
            if str(r.get("side")) in {"BUY", "SELL"}:
                latest = r
                break
        if latest is None:
            latest = output_rows[-1]
        send_signal_alert(latest, strategy=strategy, symbol=symbol, refresh_seconds=int(refresh_seconds))

    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 Charts", "📋 Trades"])

    with tab1:
        st.subheader("Market Overview")
        c1, c2, c3, c4 = st.columns(4)

        if not candles.empty:
            latest_close = float(candles["close"].iloc[-1])
            latest_high = float(candles["high"].iloc[-1])
            latest_low = float(candles["low"].iloc[-1])
            latest_volume = float(candles["volume"].iloc[-1]) if "volume" in candles.columns else 0.0
        else:
            latest_close = latest_high = latest_low = latest_volume = 0.0

        c1.metric("Close", round(latest_close, 2))
        c2.metric("High", round(latest_high, 2))
        c3.metric("Low", round(latest_low, 2))
        c4.metric("Volume", int(latest_volume))

        if not candles.empty:
            st.dataframe(candles.tail(10), use_container_width=True)
        else:
            st.warning("No candle data available.")

    with tab2:
        st.subheader("Live Price Chart")
        if not candles.empty:
            chart = (
                alt.Chart(candles)
                .mark_line()
                .encode(
                    x=alt.X("timestamp:T", title="Time"),
                    y=alt.Y("close:Q", title="Close Price"),
                    tooltip=["timestamp:T", "open:Q", "high:Q", "low:Q", "close:Q", "volume:Q"],
                )
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No chart data available.")

    with tab3:
        st.subheader("Execution Data")
        if output_rows:
            trades_df = pd.DataFrame(output_rows)
            st.dataframe(trades_df, use_container_width=True)

            try:
                summary = build_trade_summary(output_rows)
                st.text(summary)
            except Exception as exc:
                st.warning(f"Could not build trade summary: {exc}")

            csv_data = _to_csv(output_rows)
            st.download_button("Download CSV", data=csv_data, file_name="trades.csv", mime="text/csv")
        else:
            st.info("No trades generated yet.")

    with st.expander("Debug Output"):
        st.write("Strategy selected:", strategy)
        st.write("Output rows type:", type(output_rows))
        st.write("Output sample:", output_rows[:5] if isinstance(output_rows, list) else output_rows)


if __name__ == "__main__":
    main()






