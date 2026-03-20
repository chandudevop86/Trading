from __future__ import annotations

import csv
import io
import json
import os
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
from src.charting import build_live_market_chart, build_market_depth_summary, compute_market_levels
from src.breakout_bot import generate_trades as generate_breakout_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes, pick_option_strike
from src.telegram_notifier import build_trade_summary, send_telegram_message

try:
    from src.execution_engine import (
        build_analysis_queue,
        build_execution_candidates,
        execute_live_trades,
        execute_paper_trades,
    )
except Exception:
    build_analysis_queue = None
    build_execution_candidates = None
    execute_live_trades = None
    execute_paper_trades = None

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

try:
    from src.dhan_api import build_order_request_from_candidate, load_security_map
except Exception:
    build_order_request_from_candidate = None
    load_security_map = None




def _load_local_env(path: Path) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key:
                os.environ.setdefault(key, value)
    except Exception:
        return


def _bootstrap_env() -> None:
    env_candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent.parent / ".env",
        Path(__file__).resolve().parent / ".env",
    ]
    seen: set[Path] = set()
    for env_path in env_candidates:
        resolved = env_path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        _load_local_env(resolved)


_bootstrap_env()
st.set_page_config(page_title="KRSH SOLUTIONS", page_icon="chart", layout="wide")

st.markdown(
    """
    <script>
    (function () {
        const ensureViewport = () => {
            let meta = document.querySelector('meta[name="viewport"]');
            if (!meta) {
                meta = document.createElement('meta');
                meta.name = 'viewport';
                document.head.appendChild(meta);
            }
            meta.content = 'width=device-width, initial-scale=1.0, maximum-scale=1.0';
        };
        ensureViewport();
        setTimeout(ensureViewport, 0);
    })();
    </script>
    """,
    unsafe_allow_html=True,
)
TELEGRAM_TOKEN = st.secrets.get("TELEGRAM_TOKEN", "") or os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = st.secrets.get("TELEGRAM_CHAT_ID", "") or os.getenv("TELEGRAM_CHAT_ID", "")


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
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ""


def _attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    if not rows:
        return rows

    metrics_map: dict[tuple[int, str], dict[str, object]] = {}
    status = "DISABLED"
    if fetch_option_metrics and fetch_option_chain and extract_option_records and build_metrics_map and normalize_index_symbol:
        try:
            sym = normalize_index_symbol(symbol)
            payload = fetch_option_chain(sym, timeout=10.0)
            records = extract_option_records(payload)
            metrics_map = build_metrics_map(records)
            status = "FETCH_OK"
        except Exception:
            metrics_map = {}
            status = "FETCH_FAILED"

    enriched: list[dict[str, object]] = []
    any_nse_match = False
    any_estimated_expiry = False
    for item in rows:
        row = dict(item)
        strike_raw = row.get("strike_price", row.get("option_strike", ""))
        option_type = str(row.get("option_type", "") or "").upper()
        try:
            strike = int(float(strike_raw))
        except Exception:
            strike = 0

        metrics = metrics_map.get((strike, option_type), {}) if strike and option_type else {}
        if isinstance(metrics, dict) and metrics:
            row.update(metrics)
            any_nse_match = True
            if metrics.get("option_expiry"):
                row["option_expiry_source"] = "NSE"

        if not row.get("option_expiry") and row.get("option_strike"):
            est = _estimate_weekly_expiry(symbol)
            if est:
                row["option_expiry"] = est
                row["option_expiry_source"] = "EST"
                any_estimated_expiry = True

        if row.get("option_expiry"):
            row["option_expiry"] = _format_expiry(row.get("option_expiry"))
        enriched.append(row)

    final_status = status
    if status == "FETCH_OK" and not any_nse_match and any_estimated_expiry:
        final_status = "ESTIMATED_EXPIRY_ONLY"
    elif status == "FETCH_OK" and not any_nse_match:
        final_status = "NO_MATCH"
    elif status == "FETCH_OK" and any_nse_match:
        final_status = "NSE_OK"

    for row in enriched:
        row["_option_metrics_status"] = final_status
    return enriched


def _attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
    if sl_frac <= 0:
        sl_frac = 0.002
    for r in rows:
        row = dict(r)
        side = str(row.get("side", "") or "").upper()
        try:
            entry = float(row.get("close", row.get("price", row.get("entry_price", 0.0))) or 0.0)
        except Exception:
            entry = 0.0
        if entry > 0 and side in {"BUY", "SELL"}:
            row["entry_price"] = round(entry, 2)
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

def _order_trade_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    preferred = [
        "trade_label",
        "trade_no",
        "strategy",
        "symbol",
        "side",
        "entry_price",
        "spot_ltp",
        "target_1",
        "target_2",
        "target_3",
        "target_price",
        "stop_loss",
        "option_strike",
        "option_type",
        "option_ltp",
        "option_ltp_reason",
        "quantity",
        "lots",
        "order_value",
        "signal_time",
        "entry_time",
        "timestamp",
        "analysis_status",
        "execution_ready",
        "execution_type",
        "execution_status",
    ]
    ordered = [c for c in preferred if c in df.columns]
    ordered.extend([c for c in df.columns if c not in ordered])
    return df.loc[:, ordered]


def attach_futures_contracts(trades: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    annotated: list[dict[str, object]] = []
    future_symbol = f"{str(symbol).strip().upper()} FUT".strip()
    for trade in trades:
        row = dict(trade)
        row["instrument_mode"] = "Futures"
        row["trading_symbol"] = future_symbol
        row.setdefault("option_strike", future_symbol)
        annotated.append(row)
    return annotated

def send_signal_alert(row: dict[str, object], *, strategy: str, symbol: str, refresh_seconds: int) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    side = str(row.get("side", "-") or "-")
    entry = _fmt_num(row.get("entry_price", row.get("entry", "-")))
    sl = _fmt_num(row.get("stop_loss", row.get("sl", "-")))
    target = _fmt_num(row.get("target_price", row.get("target", "-")))
    option = str(row.get("option_strike", "") or "").strip()

    opt_ltp = _fmt_num(row.get("option_ltp"))
    opt_oi = _fmt_num(row.get("option_oi"))
    opt_vol = _fmt_num(row.get("option_vol"))
    opt_iv = _fmt_num(row.get("option_iv"))
    opt_expiry = _format_expiry(row.get("option_expiry"))
    opt_expiry_source = str(row.get("option_expiry_source", "") or "").upper()
    if opt_expiry and opt_expiry_source == "EST":
        opt_expiry = opt_expiry + " (est)"

    lots = str(row.get("lots", "") or "").strip()
    qty = str(row.get("quantity", "") or "").strip()
    value = _fmt_num(row.get("order_value"))
    ts = _format_ts_ist(row.get("timestamp") or row.get("entry_time"))

    extra = ""
    if refresh_seconds >= 60:
        extra = f" (next update in {refresh_seconds // 60} min)"
    elif refresh_seconds > 0:
        extra = f" (next update in {refresh_seconds} sec)"

    parts: list[str] = [
        "Trade Signal",
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

    try:
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, "`n".join(parts))
    except Exception:
        return

def _resolve_live_execution_kwargs(security_map_path: str) -> dict[str, object]:
    security_map: dict[str, dict[str, str]] = {}
    if load_security_map is not None:
        try:
            security_map = load_security_map(Path(str(security_map_path)))
        except Exception:
            security_map = {}
    return {"broker_name": "DHAN", "security_map": security_map}

def _build_dhan_preview_rows(candidates: list[dict[str, object]], security_map_path: str) -> list[dict[str, object]]:
    if build_order_request_from_candidate is None or load_security_map is None:
        return [{"status": "Broker payload builder unavailable"}]
    try:
        security_map = load_security_map(Path(str(security_map_path)))
    except Exception as exc:
        return [{"status": f"Security map load failed: {exc}"}]
    preview_rows: list[dict[str, object]] = []
    client_id = os.getenv("DHAN_CLIENT_ID", "")
    for candidate in candidates:
        try:
            order_request = build_order_request_from_candidate(candidate, client_id=client_id, security_map=security_map)
            preview_rows.append({"status": "READY", **order_request.to_payload()})
        except Exception as exc:
            preview_rows.append({"status": "ERROR", "symbol": candidate.get("symbol", ""), "side": candidate.get("side", ""), "message": str(exc)})
    return preview_rows

def _run_dhan_readiness_check(symbol: str, security_map_path: str) -> list[str]:
    notes: list[str] = []
    notes.append("PASS client id detected" if os.getenv("DHAN_CLIENT_ID", "").strip() else "FAIL missing DHAN_CLIENT_ID")
    notes.append("PASS access token detected" if os.getenv("DHAN_ACCESS_TOKEN", "").strip() else "FAIL missing DHAN_ACCESS_TOKEN")
    path = Path(str(security_map_path))
    notes.append(f"PASS security map found: {path}" if path.exists() else f"WARN security map missing: {path}")
    notes.append(f"INFO live symbol context: {symbol}")
    return notes


def _render_dhan_status_panel(symbol: str, security_map_path: str, execution_mode: str, reviewed_candidates: list[dict[str, object]]) -> None:
    client_id_present = bool(os.getenv("DHAN_CLIENT_ID", "").strip())
    access_token_present = bool(os.getenv("DHAN_ACCESS_TOKEN", "").strip())
    security_map_exists = Path(str(security_map_path)).exists()
    builder_ready = build_order_request_from_candidate is not None and load_security_map is not None
    reviewed_count = len(reviewed_candidates)
    live_ready = str(execution_mode).upper() == "LIVE" and client_id_present and access_token_present and security_map_exists and builder_ready and reviewed_count > 0

    st.caption("Dhan Status: credentials, security map, payload builder, and reviewed queue readiness.")
    cols = st.columns(5)
    cols[0].metric("Client ID", "Ready" if client_id_present else "Missing")
    cols[1].metric("Access Token", "Ready" if access_token_present else "Missing")
    cols[2].metric("Security Map", "Found" if security_map_exists else "Missing")
    cols[3].metric("Payload Builder", "Ready" if builder_ready else "Unavailable")
    cols[4].metric("Live Ready", "YES" if live_ready else "NO")

    status_rows = [
        {"check": "Broker mode", "status": str(execution_mode).upper(), "detail": "LIVE mode is required for Dhan order routing."},
        {"check": "Reviewed queue", "status": str(reviewed_count), "detail": "Only reviewed actionable trades are eligible for execution."},
        {"check": "Symbol", "status": str(symbol), "detail": "Current live symbol context."},
        {"check": "Security map path", "status": str(security_map_path), "detail": "CSV used to map option/future contracts to Dhan security IDs."},
    ]
    st.dataframe(pd.DataFrame(status_rows), width="stretch", hide_index=True)

def _render_live_execution_feedback(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    st.markdown('<div class="section-shell" style="margin-top:6px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">Live Execution Feedback</div><div class="section-copy">Latest broker-side execution rows from this run.</div>', unsafe_allow_html=True)
    st.dataframe(_order_trade_columns(pd.DataFrame(rows)), width="stretch")
    st.markdown('</div>', unsafe_allow_html=True)

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


def run_strategy(*, strategy: str, candles: pd.DataFrame, capital: float, risk_pct: float, rr_ratio: float, trailing_sl_pct: float, symbol: str, strike_step: int, moneyness: str, strike_steps: int, fetch_option_metrics: bool, mtf_ema_period: int, mtf_setup_mode: str, mtf_retest_strength: bool, mtf_max_trades_per_day: int) -> list[dict[str, object]]:
    candle_rows = _df_to_candles(candles)
    strategy_name = str(strategy or "Breakout").strip()
    risk_fraction = float(risk_pct) / 100.0
    rows: list[dict[str, object]] = []
    if strategy_name == "Breakout":
        rows = generate_breakout_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))
    elif strategy_name == "Demand Supply":
        rows = generate_demand_supply_trades(candles, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio)) if generate_demand_supply_trades is not None else []
    elif strategy_name == "Indicator":
        indicator_rows = generate_indicator_rows(candle_rows, config=IndicatorConfig())
        mapped: list[dict[str, object]] = []
        for row in indicator_rows:
            item = dict(row)
            signal = str(item.get("market_signal", "")).upper()
            item["side"] = "BUY" if signal in {"BULLISH_TREND", "OVERSOLD", "BUY", "LONG"} else "SELL" if signal in {"BEARISH_TREND", "OVERBOUGHT", "SELL", "SHORT"} else ""
            item.setdefault("entry_price", item.get("close", item.get("price", 0.0)))
            item.setdefault("timestamp", item.get("timestamp", ""))
            item.setdefault("strategy", "INDICATOR")
            mapped.append(item)
        rows = _attach_indicator_trade_levels(mapped, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))
    elif strategy_name == "One Trade/Day":
        rows = generate_one_trade_day_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), config=IndicatorConfig(), trailing_sl_pct=float(trailing_sl_pct))
    elif strategy_name == "MTF 5m":
        rows = generate_mtf_trade_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct), ema_period=int(mtf_ema_period), setup_mode=str(mtf_setup_mode), require_retest_strength=bool(mtf_retest_strength), max_trades_per_day=int(mtf_max_trades_per_day))
    else:
        rows = generate_breakout_trades(candle_rows, capital=float(capital), risk_pct=risk_fraction, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))

    normalized: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item.setdefault("strategy", strategy_name.upper().replace(" ", "_"))
        item.setdefault("symbol", symbol)
        item.setdefault("trade_no", idx)
        item.setdefault("trade_label", f"Trade {idx}")
        item.setdefault("entry_time", item.get("timestamp", ""))
        normalized.append(item)

    actionable = [dict(r) for r in normalized if str(r.get("side", "")).upper() in {"BUY", "SELL"}]
    if not actionable:
        return normalized

    actionable = attach_option_strikes(actionable, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
    actionable = _attach_option_metrics(actionable, symbol=str(symbol), fetch_option_metrics=bool(fetch_option_metrics))
    keyed_actionable = {
        f"{row.get('trade_no', '')}|{row.get('entry_time', row.get('timestamp', ''))}|{row.get('side', '')}": row
        for row in actionable
    }

    merged: list[dict[str, object]] = []
    for row in normalized:
        key = f"{row.get('trade_no', '')}|{row.get('entry_time', row.get('timestamp', ''))}|{row.get('side', '')}"
        enriched = keyed_actionable.get(key)
        if enriched is not None:
            updated = dict(row)
            updated.update(enriched)
            merged.append(updated)
        else:
            merged.append(row)
    return merged

def _render_sidebar_shell() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 20%),
                radial-gradient(circle at bottom left, rgba(34,197,94,0.10), transparent 22%),
                linear-gradient(180deg, #020617 0%, #07111f 44%, #0b1728 100%);
        }
        [data-testid="stHeader"] {
            background: rgba(2, 6, 23, 0.72);
        }
        [data-testid="stAppViewContainer"] .main .block-container {
            max-width: none;
            padding-top: 0.20rem;
            padding-left: 0.35rem;
            padding-right: 0.35rem;
        }
        [data-testid="stAppViewContainer"] [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(15,23,42,0.92), rgba(9,14,26,0.96));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 14px;
            padding: 7px 9px;
            box-shadow: 0 14px 30px rgba(2, 6, 23, 0.22);
        }
        [data-testid="stAppViewContainer"] [data-testid="stDataFrame"] {
            background: rgba(15, 23, 42, 0.7);
            border-radius: 16px;
            border: 1px solid rgba(148, 163, 184, 0.10);
            padding: 4px;
        }
        .hero-strip {
            border-radius: 24px;
            padding: 7px 9px;
            margin: 6px 0 10px 0;
            box-shadow: 0 24px 48px rgba(2, 6, 23, 0.32);
        }
        .hero-strip.hero-bull {
            background: linear-gradient(135deg, rgba(6,78,59,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(34,197,94,0.90) 100%);
            border: 1px solid rgba(74, 222, 128, 0.18);
        }
        .hero-strip.hero-bear {
            background: linear-gradient(135deg, rgba(127,29,29,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(239,68,68,0.90) 100%);
            border: 1px solid rgba(248, 113, 113, 0.18);
        }
        .hero-strip.hero-range {
            background: linear-gradient(135deg, rgba(120,53,15,0.96) 0%, rgba(15,23,42,0.94) 52%, rgba(245,158,11,0.88) 100%);
            border: 1px solid rgba(251, 191, 36, 0.18);
        }
        .hero-kicker {
            color: #93c5fd;
            font-size: 11px;
            letter-spacing: 0.2em;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        .hero-symbol {
            color: #f8fafc;
            font-size: 26px;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 4px;
        }
        .hero-price {
            color: #e0f2fe;
            font-size: 22px;
            font-weight: 700;
        }
        .hero-change {
            font-size: 14px;
            font-weight: 700;
            margin-top: 2px;
        }
        .hero-grid {
            display: grid;
            grid-template-columns: minmax(220px, 1.3fr) repeat(3, minmax(120px, 1fr));
            gap: 8px;
            align-items: stretch;
        }
        .hero-tile {
            background: rgba(15, 23, 42, 0.66);
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 14px;
            padding: 7px 9px;
        }
        .hero-label {
            color: #94a3b8;
            font-size: 11px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        .hero-value {
            color: #f8fafc;
            font-size: 16px;
            font-weight: 700;
        }
        @media (max-width: 980px) {
            .top-nav {
                grid-template-columns: 1fr;
                justify-items: start;
                gap: 14px;
            }
            .top-nav-menu {
                gap: 8px;
            }
            .top-nav-search {
                min-width: 0;
                width: 100%;
            }
            .dhan-hero-shell {
                width: 100%;
                min-height: 0;
                gap: 18px;
            }
            .dhan-hero-banner {
                width: min(560px, 100%);
            }
        }
        @media (max-width: 900px) {
            .hero-grid {
                grid-template-columns: 1fr;
            }
        }
        [data-testid="stTabs"] [role="tablist"] {
            gap: 8px;
            background: rgba(15, 23, 42, 0.56);
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 14px;
            padding: 8px;
            margin-bottom: 5px;
        }
        [data-testid="stTabs"] [role="tab"] {
            background: rgba(15, 23, 42, 0.82);
            color: #cbd5e1;
            border-radius: 14px;
            border: 1px solid rgba(148, 163, 184, 0.10);
            padding: 10px 16px;
            font-weight: 700;
        }
        [data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, rgba(14,165,233,0.95), rgba(34,197,94,0.90));
            color: #04111d;
            border-color: transparent;
            box-shadow: 0 12px 26px rgba(14, 165, 233, 0.20);
        }
        .stButton > button {
            background: linear-gradient(135deg, #0f172a 0%, #162338 100%);
            color: #e2e8f0;
            border: 1px solid rgba(125, 211, 252, 0.16);
            border-radius: 14px;
            font-weight: 700;
            padding: 0.62rem 1rem;
            box-shadow: 0 12px 24px rgba(2, 6, 23, 0.22);
        }
        .stButton > button:hover {
            border-color: rgba(74, 222, 128, 0.28);
            color: #f8fafc;
        }
        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #0ea5e9 0%, #22c55e 100%);
            color: #04111d;
            border-color: transparent;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: linear-gradient(180deg, rgba(14,165,233,0.12), rgba(15,23,42,0.92));
            color: #e2e8f0;
            font-weight: 700;
            border-bottom: 1px solid rgba(125, 211, 252, 0.14);
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            background: rgba(8, 15, 28, 0.78);
            color: #dbeafe;
            border-color: rgba(148, 163, 184, 0.08);
        }
        [data-testid="stExpander"] {
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 14px;
            background: rgba(15, 23, 42, 0.62);
        }
        [data-testid="stVerticalBlock"] [data-testid="stAltairChart"],
        [data-testid="stVerticalBlock"] [data-testid="stDataFrame"] {
            box-shadow: 0 18px 36px rgba(2, 6, 23, 0.22);
        }
        .chart-shell {
            background: linear-gradient(180deg, rgba(15,23,42,0.88), rgba(8,15,28,0.92));
            border: 1px solid rgba(148, 163, 184, 0.10);
            border-radius: 20px;
            padding: 10px 12px 4px 12px;
            margin-bottom: 5px;
            box-shadow: 0 20px 40px rgba(2, 6, 23, 0.24);
        }
        .section-shell {
            background: linear-gradient(180deg, rgba(15,23,42,0.82), rgba(8,15,28,0.90));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 24px;
            padding: 9px 11px;
            margin-bottom: 5px;
            box-shadow: 0 20px 38px rgba(2, 6, 23, 0.20);
        }
        .live-panel {
            background: linear-gradient(180deg, rgba(15,23,42,0.92), rgba(8,15,28,0.96));
            border: 1px solid rgba(56, 189, 248, 0.18);
            border-radius: 24px;
            padding: 9px 11px;
            margin-bottom: 5px;
            box-shadow: 0 20px 42px rgba(2, 6, 23, 0.24);
        }
        .live-kicker {
            color: #7dd3fc;
            font-size: 11px;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            margin-bottom: 5px;
            font-weight: 700;
        }
        .live-title {
            color: #f8fafc;
            font-size: 24px;
            font-weight: 800;
            line-height: 1.05;
            margin-bottom: 5px;
        }
        .live-sub {
            color: #94a3b8;
            font-size: 16px;
            line-height: 1.5;
            max-width: 760px;
        }
        .control-ribbon {
            background: linear-gradient(180deg, rgba(12,32,57,0.90), rgba(9,24,44,0.96));
            border: 1px solid rgba(118, 164, 210, 0.16);
            border-radius: 20px;
            padding: 10px 12px 6px 12px;
            margin-bottom: 5px;
            box-shadow: 0 18px 34px rgba(2, 12, 27, 0.18);
        }
        .control-ribbon-title {
            color: #f8fafc;
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        .control-ribbon-copy {
            color: #94a3b8;
            font-size: 14px;
            margin-bottom: 6px;
        }
        [data-testid="stSidebar"] {
            display: none;
            background:
                radial-gradient(circle at top left, rgba(34,197,94,0.14), transparent 24%),
                linear-gradient(180deg, #08111f 0%, #0b1220 48%, #0f172a 100%);
            border-right: 1px solid rgba(148, 163, 184, 0.18);
        }
        [data-testid="stSidebar"] .block-container {
            padding-top: 1rem;
        }
        [data-testid="stSidebar"] .live-panel {
            background: linear-gradient(180deg, rgba(15,23,42,0.96), rgba(8,15,28,0.98));
            border: 1px solid rgba(56, 189, 248, 0.2);
            border-radius: 14px;
            padding: 7px 9px;
            margin-bottom: 5px;
            box-shadow: 0 16px 40px rgba(2, 6, 23, 0.35);
        }
        [data-testid="stSidebar"] .live-kicker {
            color: #7dd3fc;
            font-size: 11px;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            margin-bottom: 4px;
        }
        [data-testid="stSidebar"] .live-title {
            color: #e2e8f0;
            font-size: 24px;
            font-weight: 700;
            line-height: 1.1;
            margin-bottom: 5px;
        }
        [data-testid="stSidebar"] .live-sub {
            color: #94a3b8;
            font-size: 12px;
            line-height: 1.4;
        }
        [data-testid="stSidebar"] .live-section {
            color: #f8fafc;
            font-size: 12px;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            margin: 14px 0 6px 0;
        }
        [data-testid="stSidebar"] .stSegmentedControl,
        [data-testid="stSidebar"] .stPills {
            background: rgba(15, 23, 42, 0.72);
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 16px;
            padding: 6px;
        }
        [data-testid="stSidebar"] .stSegmentedControl [role="radiogroup"],
        [data-testid="stSidebar"] .stPills [role="radiogroup"] {
            gap: 8px;
        }
        [data-testid="stSidebar"] .stSegmentedControl label,
        [data-testid="stSidebar"] .stPills label {
            border-radius: 12px !important;
            border: 1px solid rgba(148, 163, 184, 0.14) !important;
            background: rgba(15, 23, 42, 0.95) !important;
            color: #cbd5e1 !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebar"] .stSegmentedControl label[data-selected="true"],
        [data-testid="stSidebar"] .stPills label[data-selected="true"] {
            background: linear-gradient(135deg, #0ea5e9, #22c55e) !important;
            color: #04111d !important;
            border-color: transparent !important;
            box-shadow: 0 10px 24px rgba(14, 165, 233, 0.22);
        }
        [data-testid="stSidebar"] .status-card {
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 14px;
            padding: 7px 9px;
        }
        [data-testid="stSidebar"] .status-label {
            color: #94a3b8;
            font-size: 10px;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            margin-bottom: 4px;
        }
        [data-testid="stSidebar"] .status-value {
            color: #f8fafc;
            font-size: 14px;
            font-weight: 700;
            line-height: 1.2;
        }
        [data-testid="stSidebar"] .status-price {
            color: #4ade80;
        }
        [data-testid="stSidebar"] .stButton button,
        [data-testid="stSidebar"] .stDownloadButton button {
            border-radius: 14px;
        }
        </style>
        """,

        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top right, rgba(14,165,233,0.10), transparent 20%),
                radial-gradient(circle at bottom left, rgba(34,197,94,0.10), transparent 22%),
                radial-gradient(circle at top right, rgba(255,184,77,0.16), transparent 24%),
                radial-gradient(circle at bottom left, rgba(111,188,255,0.14), transparent 26%),
                linear-gradient(180deg, #04101d 0%, #08192c 44%, #0b1f36 100%);
        }
        [data-testid="stHeader"] {
            background: rgba(4, 17, 31, 0.74);
            border-bottom: 1px solid rgba(118, 164, 210, 0.10);
        }
        [data-testid="stAppViewContainer"] .main .block-container {
            max-width: none;
            padding-top: 0.20rem;
            padding-left: 0.35rem;
            padding-right: 0.35rem;
        }
        h1, h2, h3, h4, h5, h6, p, label, span, div {
            color: #e5eef8;
        }
        [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(12,32,57,0.94), rgba(9,24,44,0.96)) !important;
            border: 1px solid rgba(118, 164, 210, 0.14) !important;
            border-radius: 18px !important;
            box-shadow: 0 14px 30px rgba(2, 12, 27, 0.22) !important;
        }
        [data-testid="stMetricLabel"] {
            color: #89a7c7 !important;
        }
        [data-testid="stMetricValue"] {
            color: #ffffff !important;
        }
        [data-testid="stTabs"] [role="tablist"] {
            background: rgba(12, 32, 57, 0.82);
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 14px 28px rgba(2, 12, 27, 0.18);
        }
        [data-testid="stTabs"] [role="tab"] {
            background: rgba(255, 255, 255, 0.05);
            color: #d8e6f5;
            border: 1px solid rgba(148, 196, 232, 0.12);
        }
        [data-testid="stTabs"] [aria-selected="true"] {
            background: linear-gradient(135deg, #ffb84d 0%, #ff8a2a 100%);
            color: #ffffff;
            box-shadow: none;
        }
        .stButton > button {
            background: linear-gradient(135deg, #0c2039 0%, #12365b 100%);
            color: #eaf4ff;
            border: 1px solid rgba(148, 196, 232, 0.14);
            box-shadow: 0 12px 24px rgba(2, 12, 27, 0.18);
        }
        .stButton > button:hover {
            border-color: rgba(255, 122, 47, 0.36);
            color: #ffffff;
        }
        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, #ffb84d 0%, #ff8a2a 100%);
            color: #ffffff;
            border-color: transparent;
        }
        [data-testid="stDataFrame"] {
            background: rgba(12, 32, 57, 0.92) !important;
            border: 1px solid rgba(118, 164, 210, 0.14) !important;
            border-radius: 18px !important;
            box-shadow: 0 16px 30px rgba(2, 12, 27, 0.18) !important;
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            background: rgba(255, 255, 255, 0.06);
            color: #eaf4ff;
            border-bottom: 1px solid rgba(148, 196, 232, 0.12);
        }
        [data-testid="stDataFrame"] [role="gridcell"] {
            background: rgba(9, 24, 44, 0.92);
            color: #dbeafe;
        }
        [data-testid="stExpander"] {
            background: rgba(12, 32, 57, 0.88);
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 16px 30px rgba(2, 12, 27, 0.16);
        }
        .section-shell, .chart-shell {
            background: linear-gradient(180deg, rgba(12,32,57,0.94), rgba(9,24,44,0.98));
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 18px 32px rgba(2, 12, 27, 0.20);
        }
        [data-testid="stSidebar"] {
            display: none;
            background: radial-gradient(circle at top left, rgba(255,184,77,0.12), transparent 26%), linear-gradient(180deg, #071524 0%, #0b1d33 100%);
            border-right: 1px solid rgba(118, 164, 210, 0.16);
        }
        [data-testid="stSidebar"] .live-panel,
        [data-testid="stSidebar"] .status-card {
            background: linear-gradient(180deg, rgba(12,32,57,0.96), rgba(9,24,44,0.98));
            border: 1px solid rgba(118, 164, 210, 0.14);
            box-shadow: 0 14px 28px rgba(2, 12, 27, 0.22);
        }
        [data-testid="stSidebar"] .live-kicker,
        [data-testid="stSidebar"] .live-sub,
        [data-testid="stSidebar"] .status-label,
        [data-testid="stSidebar"] .live-section {
            color: #89a7c7;
        }
        [data-testid="stSidebar"] .live-title,
        [data-testid="stSidebar"] .status-value {
            color: #ffffff;
        }
        [data-testid="stSidebar"] .status-price {
            color: #7dd3fc;
        }
        [data-testid="stSidebar"] .stSegmentedControl,
        [data-testid="stSidebar"] .stPills {
            background: rgba(12, 32, 57, 0.82);
            border: 1px solid rgba(118, 164, 210, 0.14);
        }
        [data-testid="stSidebar"] .stSegmentedControl label,
        [data-testid="stSidebar"] .stPills label {
            background: rgba(255, 255, 255, 0.05) !important;
            color: #d8e6f5 !important;
            border: 1px solid rgba(148, 196, 232, 0.12) !important;
        }
        [data-testid="stSidebar"] .stSegmentedControl label[data-selected="true"],
        [data-testid="stSidebar"] .stPills label[data-selected="true"] {
            background: linear-gradient(135deg, #ff7a2f 0%, #ff5b21 100%) !important;
            color: #ffffff !important;
            box-shadow: none;
        }
                .page-masthead {
            display: block;
            background: linear-gradient(180deg, #050505 0%, #090909 100%);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 0;
            padding: 8px 14px 10px 14px;
            margin-bottom: 5px;
            box-shadow: 0 24px 50px rgba(0, 0, 0, 0.34);
            overflow: hidden;
            position: relative;
        }
        .page-masthead::before,
        .page-masthead::after {
            display: none;
        }
        .top-nav {
            position: relative;
            z-index: 3;
            display: grid;
            grid-template-columns: auto 1fr;
            align-items: center;
            gap: 10px;
            border: none;
            border-radius: 0;
            padding: 4px 0 8px 0;
            margin-bottom: 4px;
            backdrop-filter: none;
        }
        .top-nav-brand {
            display: inline-flex;
            align-items: center;
            justify-content: flex-start;
            gap: 8px;
            color: #ffffff;
            font-size: 24px;
            font-weight: 800;
            letter-spacing: 0;
            white-space: nowrap;
        }
        .top-nav-logo {
            width: 36px;
            height: 36px;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: flex-start;
            background: #1fba81;
            color: #ffffff;
            font-size: 14px;
            font-weight: 900;
            box-shadow: none;
        }
        .top-nav-brand span {
            color: #ffffff;
        }
        .mega-panel {
            position: absolute;
            z-index: 8;
            top: calc(100% + 14px);
            left: -8px;
            width: min(820px, 70vw);
            max-height: 68vh;
            overflow-y: auto;
            overflow-x: hidden;
            background: #ffffff;
            color: #111827;
            border-radius: 24px;
            padding: 28px 30px;
            box-shadow: 0 28px 50px rgba(0,0,0,0.28);
            opacity: 0;
            visibility: hidden;
            transform: translateY(12px);
            transition: opacity 180ms ease, transform 180ms ease, visibility 180ms ease;
            pointer-events: none;
            scrollbar-width: thin;
            scrollbar-color: rgba(17,24,39,0.35) transparent;
        }
        .mega-panel::before {
            content: "";
            position: absolute;
            top: -14px;
            left: 190px;
            width: 36px;
            height: 36px;
            background: #ffffff;
            transform: rotate(45deg);
            border-radius: 4px;
        }
        .mega-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 28px 34px;
        }
        .mega-item {
            display: grid;
            grid-template-columns: 36px 1fr;
            gap: 16px;
            align-items: start;
        }
        .mega-icon {
            width: 36px;
            height: 36px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: flex-start;
            font-size: 14px;
            font-weight: 800;
            color: #ffffff;
            background: linear-gradient(135deg, #14b87a 0%, #0f9f6c 100%);
        }
        .mega-icon.alt {
            background: linear-gradient(135deg, #7c4dff 0%, #a46dff 100%);
        }
        .mega-icon.dark {
            background: linear-gradient(135deg, #2d3748 0%, #111827 100%);
        }
        .mega-icon.blue {
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        }
        .mega-title {
            color: #121826;
            font-size: 16px;
            font-weight: 800;
            margin-bottom: 4px;
        }
        .markets-panel {
            width: min(1000px, 78vw);
            left: -120px;
            padding: 0;
            overflow: hidden;
        }
        .markets-panel::before {
            left: 320px;
        }
        .markets-tabs {
            display: flex;
            gap: 14px;
            flex-wrap: nowrap;
            padding: 16px 24px;
            border-bottom: 1px solid #e5e7eb;
            background: #ffffff;
        }
        .markets-tab {
            padding: 12px 22px;
            border-radius: 10px;
            border: 1px solid #d1d5db;
            color: #4b5563;
            font-size: 16px;
            font-weight: 700;
            background: #ffffff;
        }
        .markets-tab.active {
            background: #f7a600;
            border-color: #f7a600;
            color: #ffffff;
        }
        .markets-body {
            display: grid;
            grid-template-columns: 260px 1fr;
            min-height: 320px;
        }
        .markets-side {
            border-right: 1px solid #e5e7eb;
            background: #fbfbfb;
            padding: 0;
        }
        .markets-side-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 24px;
            font-size: 14px;
            font-weight: 700;
            color: #9ca3af;
        }
        .markets-side-item.active {
            color: #f59e0b;
            background: #fff7ed;
        }
        .markets-arrow {
            color: #f59e0b;
            font-size: 16px;
        }
        .markets-list-wrap {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 26px 48px;
            padding: 26px 30px;
            align-content: start;
        }
        .market-link {
            display: flex;
            align-items: center;
            gap: 8px;
            color: #1f2937;
            font-size: 14px;
            font-weight: 700;
        }
        .market-dot {
            width: 22px;
            height: 22px;
            border-radius: 999px;
            display: inline-flex;
            align-items: center;
            justify-content: flex-start;
            color: #ffffff;
            font-size: 11px;
            font-weight: 800;
            background: linear-gradient(135deg, #ef4444 0%, #f59e0b 100%);
        }
        .market-dot.blue {
            background: linear-gradient(135deg, #38bdf8 0%, #2563eb 100%);
        }
        .more-panel {
            width: min(860px, 74vw);
            left: -180px;
        }
        .more-panel::before {
            left: 210px;
        }
        .investments-panel {
            width: min(940px, 76vw);
            left: -80px;
        }
        .investments-panel::before {
            left: 250px;
        }
        .mega-copy {
            color: #6b7280;
            font-size: 16px;
            line-height: 1.55;
        }
        .masthead-grid {
            position: relative;
            z-index: 2;
            display: block;
            padding: 10px 6px 4px 6px;
        }
        .dhan-hero-shell {
            display: flex;
            width: min(640px, 100%);
            margin: 0 auto;
            flex-direction: column;
            align-items: flex-start;
            justify-content: flex-start;
            text-align: left;
            min-height: 0;
            gap: 14px;
        }
        .dhan-hero-icon {
            color: #f7a600;
            font-size: clamp(56px, 5vw, 84px);
            line-height: 1;
            margin-bottom: 2px;
        }
        .dhan-hero-title {
            color: #f3f4f6;
            max-width: 1100px;
            font-size: clamp(38px, 4.4vw, 68px);
            font-weight: 900;
            line-height: 0.98;
            margin: 0;
            letter-spacing: -0.03em;
        }
        .dhan-hero-title .hero-line {
            display: block;
            white-space: nowrap;
        }
        .dhan-hero-title .hero-line + .hero-line {
            margin-top: 2px;
        }
        .dhan-hero-title .hero-line.accent {
            color: #2f66ff;
        }
        .dhan-hero-subtitle {
            color: #ffffff;
            font-size: clamp(20px, 2vw, 28px);
            line-height: 1.35;
            margin-top: 2px;
            max-width: 760px;
        }
        .dhan-hero-subtitle .hero-sub-line {
            display: block;
            white-space: nowrap;
        }
        .dhan-hero-banner {
            margin-top: 14px;
            width: min(640px, 100%);
            border-radius: 24px;
            border: 1px solid rgba(111, 155, 255, 0.6);
            background: linear-gradient(180deg, rgba(18,25,38,0.94), rgba(16,22,34,0.94));
            padding: 16px 18px 16px 18px;
            box-shadow: 0 22px 40px rgba(0,0,0,0.28);
        }
        .dhan-hero-badge {
            display: inline-flex;
            align-items: center;
            justify-content: flex-start;
            justify-content: flex-start;
            border-radius: 999px;
            background: #f7a600;
            color: #111827;
            font-size: 16px;
            font-weight: 800;
            margin-bottom: 5px;
        }
        .dhan-hero-banner-copy {
            color: #ffffff;
            font-size: clamp(18px, 1.8vw, 24px);
            line-height: 1.5;
        }
        .dhan-hero-banner-copy .hero-banner-line {
            display: block;
        }
        .breadcrumb-list {
            display: flex;
            flex-wrap: nowrap;
            gap: 8px;
            align-items: center;
            margin-bottom: 5px;
            color: rgba(255,255,255,0.72);
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .breadcrumb-sep {
            color: rgba(255,255,255,0.34);
        }
        .breadcrumb-current {
            color: #ffffff;
        }
        .page-eyebrow {
            color: #88bfff;
            font-size: 11px;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }
        .page-title {
            color: #ffffff;
            font-size: 74px;
            font-weight: 900;
            line-height: 0.94;
            margin: 0;
            max-width: 620px;
        }
        .page-title .accent {
            color: #ffffff;
        }
        .page-subtitle {
            color: #dbe7f8;
            font-size: 19px;
            line-height: 1.65;
            margin-top: 22px;
            max-width: 560px;
        }
        .page-badge {
            position: absolute;
            right: 22px;
            bottom: 22px;
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 14px;
            color: #111827;
            font-size: 14px;
            font-weight: 700;
            padding: 9px 11px;
            box-shadow: none;
        }
        .masthead-pills {
            display: none;
        }
        .hero-search {
            margin-top: 24px;
            max-width: 520px;
            border-radius: 14px;
            background: #ffffff;
            color: #4b5563;
            padding: 16px 20px;
            font-size: 24px;
            font-weight: 700;
            box-shadow: none;
            border: 1px solid rgba(0,0,0,0.08);
        }
        .hero-chip-row {
            display: flex;
            gap: 14px;
            flex-wrap: nowrap;
            margin-top: 22px;
        }
        .hero-chip {
            min-width: 132px;
            border-radius: 16px;
            padding: 14px 16px;
            background: rgba(255, 255, 255, 0.06);
            border: 1px solid rgba(255,255,255,0.10);
            box-shadow: none;
        }
        .hero-chip-label {
            color: #ffffff;
            font-size: 14px;
            font-weight: 800;
        }
        .hero-chip-meta {
            color: #89a7c7;
            font-size: 12px;
            margin-top: 4px;
        }
        .hero-visual {
            position: relative;
            min-height: 300px;
            border-radius: 24px;
            background: radial-gradient(circle at 18% 18%, rgba(76, 180, 255, 0.20), transparent 22%), linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
            border: 1px solid rgba(255,255,255,0.08);
            overflow: hidden;
            padding: 18px;
        }
        .hero-visual::before {
            content: "";
            position: absolute;
            inset: auto 22px 26px 22px;
            height: 92px;
            border-radius: 14px;
            background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
            border: 1px solid rgba(255,255,255,0.06);
        }
        .hero-visual::after {
            content: "KRSH SOLUTIONS";
            position: absolute;
            top: 28px;
            left: 28px;
            color: rgba(255,255,255,0.92);
            font-size: 24px;
            font-weight: 800;
            letter-spacing: 0.02em;
        }
        .hero-orb {
            position: absolute;
            border-radius: 999px;
            filter: blur(10px);
            opacity: 0.85;
        }
        .hero-orb-one {
            width: 96px;
            height: 120px;
            right: 40px;
            top: 54px;
            background: radial-gradient(circle, rgba(255,193,92,0.85) 0%, rgba(255,193,92,0.08) 72%);
        }
        .hero-orb-two {
            width: 140px;
            height: 140px;
            right: 110px;
            bottom: 90px;
            background: radial-gradient(circle, rgba(53,168,255,0.55) 0%, rgba(53,168,255,0.06) 72%);
        }
        .hero-curve {
            position: absolute;
            border-top: 3px solid;
            border-radius: 999px;
            opacity: 0.95;
        }
        .hero-curve-one {
            right: -8px;
            top: 110px;
            width: 340px;
            height: 130px;
            border-color: rgba(43, 170, 255, 0.72);
            transform: rotate(-4deg);
        }
        .hero-curve-two {
            right: 34px;
            top: 150px;
            width: 300px;
            height: 120px;
            border-color: rgba(102, 222, 255, 0.44);
            transform: rotate(8deg);
        }
        .hero-curve-three {
            right: 76px;
            top: 208px;
            width: 240px;
            height: 96px;
            border-color: rgba(110, 103, 255, 0.45);
            transform: rotate(-12deg);
        }
        .hero-preview-card {
            position: absolute;
            left: 28px;
            right: 28px;
            bottom: 28px;
            z-index: 2;
            display: grid;
            grid-template-columns: 1.2fr 0.8fr;
            gap: 16px;
            align-items: center;
            padding: 9px 11px;
            background: linear-gradient(180deg, rgba(19,19,19,0.90), rgba(19,19,19,0.82));
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
        }
        .hero-preview-copy {
            color: rgba(255,255,255,0.78);
            font-size: 16px;
            line-height: 1.6;
            max-width: 290px;
        }
        .hero-preview-title {
            color: #ffffff;
            font-size: 16px;
            font-weight: 800;
            margin-bottom: 5px;
        }
        .hero-preview-metrics {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 8px;
        }
        .hero-preview-metric {
            border-radius: 14px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.08);
            padding: 9px 11px;
        }
        .hero-preview-label {
            color: rgba(255,255,255,0.62);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }
        .hero-preview-value {
            color: #ffffff;
            font-size: 14px;
            font-weight: 800;
            margin-top: 4px;
        }
        .hero-strip,
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.08);
            border-radius: 20px;
            box-shadow: none;
        }
        .hero-strip.hero-bull,
        .hero-strip.hero-bear,
        .hero-strip.hero-range {
            border-left: 4px solid #f7a600;
        }
        .hero-kicker, .hero-label {
            color: #6b7280;
        }
        .hero-symbol, .hero-value, .hero-price {
            color: #111827;
        }
        .hero-tile {
            background: #f8fafc;
            border: 1px solid rgba(0,0,0,0.06);
        }
        .hero-kicker, .hero-label {
            color: #89a7c7;
        }
        .hero-symbol, .hero-value, .hero-price {
            color: #ffffff;
        }
        .hero-tile {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid rgba(148, 196, 232, 0.14);
        }
        .section-heading {
            color: #ffffff;
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 2px;
        }
        .section-copy {
            color: #89a7c7;
            font-size: 14px;
            margin-bottom: 5px;
        }
        @media (max-width: 980px) {
            .top-nav {
                grid-template-columns: 1fr;
                justify-items: start;
                gap: 14px;
            }
            .top-nav-menu {
                gap: 8px;
            }
            .top-nav-search {
                min-width: 0;
                width: 100%;
            }
            .dhan-hero-shell {
                width: 100%;
                min-height: 0;
                gap: 18px;
            }
            .dhan-hero-banner {
                width: min(560px, 100%);
            }
        }
        @media (max-width: 900px) {
            .masthead-grid {
                grid-template-columns: 1fr;
            }
            .mega-panel {
                position: static;
                width: 100%;
                opacity: 1;
                visibility: visible;
                transform: none;
                pointer-events: auto;
                margin-top: 16px;
            }
            .masthead-pills {
                grid-template-columns: repeat(2, minmax(120px, 1fr));
            }
            .page-title {
                font-size: 40px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
def _render_page_masthead(
    symbol: str,
    strategy: str,
    execution_mode: str,
    auto_execute: bool,
    *,
    workspace: str,
    content_view: str,
    interval: str,
    period: str,
    instrument_mode: str,
    lots: int,
    lot_size: int,
    risk_pct: float,
    rr_ratio: float,
    last_signal_side: str,
    open_trades: int,
    account_status: str,
) -> None:
    section_tabs = ["Home", "Live Signals", "Market", "Strategy", "Trades", "Desk Controls", "Downloads"]

    st.markdown(
        """
        <div class="page-masthead">
            <div class="top-nav" style="display:block;">
                <div class="top-nav-brand"><div class="top-nav-logo">K</div><div>KRSH<span> Solutions</span></div></div>
                <div style="margin-top:10px;color:#89a7c7;font-size:12px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;white-space:nowrap;">Open section</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    selected_section = st.segmented_control(
        "Open section",
        section_tabs,
        default=content_view if content_view in section_tabs else section_tabs[0],
        key="masthead_open_section",
        width="stretch",

        label_visibility="collapsed",
    )
    if selected_section and selected_section != content_view:
        st.session_state["content_view"] = str(selected_section)
        st.rerun()

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


def _format_volume_metric(symbol: str, candles: pd.DataFrame) -> tuple[str, str]:
    if candles is None or candles.empty or "volume" not in candles.columns:
        return "-", "No volume data"
    volume_series = pd.to_numeric(candles["volume"], errors="coerce").fillna(0)
    latest_volume = float(volume_series.iloc[-1]) if not volume_series.empty else 0.0
    if latest_volume > 0:
        return f"{int(latest_volume):,}", "Latest candle"
    symbol_text = str(symbol or "").upper()
    if symbol_text.startswith("^"):
        return "-", "Index volume unavailable"
    return "0", "Latest candle"


def _build_watchlist_candidates(candles: pd.DataFrame, strategy: str, symbol: str, limit: int = 3) -> list[dict[str, object]]:
    if candles is None or candles.empty:
        return []

    df = candles.copy().tail(max(limit * 6, 12)).reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if df.empty:
        return []

    df["range"] = (df["high"] - df["low"]).abs()
    df["body"] = (df["close"] - df["open"]).abs()
    df["score"] = (df["body"] * 1.4) + df["range"]
    ranked = df.sort_values(["score", "timestamp"], ascending=[False, False]).head(limit)

    watchlist: list[dict[str, object]] = []
    for idx, row in enumerate(ranked.itertuples(index=False), start=1):
        close_price = float(getattr(row, "close", 0.0) or 0.0)
        open_price = float(getattr(row, "open", 0.0) or 0.0)
        high_price = float(getattr(row, "high", 0.0) or 0.0)
        low_price = float(getattr(row, "low", 0.0) or 0.0)
        side = "BUY" if close_price >= open_price else "SELL"
        stop_loss = low_price if side == "BUY" else high_price
        risk = abs(close_price - stop_loss)
        if risk <= 0:
            risk = max(close_price * 0.002, 1.0)
            stop_loss = close_price - risk if side == "BUY" else close_price + risk
        target_price = close_price + (risk * 2.0) if side == "BUY" else close_price - (risk * 2.0)
        watchlist.append(
            {
                "trade_label": f"Setup {idx}",
                "strategy": str(strategy).upper().replace(" ", "_"),
                "symbol": symbol,
                "entry_time": str(getattr(row, "timestamp", "")),
                "side": side,
                "entry_price": round(close_price, 2),
                "stop_loss": round(float(stop_loss), 2),
                "target_price": round(float(target_price), 2),
                "setup_status": "WATCHLIST",
                "setup_reason": "Recent high-momentum candle from current market data.",
                "execution_ready": "NO",
            }
        )
    return watchlist
def _render_hero_strip(
    symbol: str,
    last_price: object,
    day_change: float,
    strategy: str,
    execution_mode: str,
    open_trades: int,
    support_band: str,
    resistance_band: str,
    option_bias: str,
    market_status: str,
) -> None:
    price_text = _fmt_num(last_price)
    change_color = "#4ade80" if day_change >= 0 else "#f87171"
    change_prefix = "+" if day_change > 0 else ""
    status_text = "ACTIVE" if open_trades > 0 else "STANDBY"
    market_upper = str(market_status or "").upper()
    if "BREAKOUT" in market_upper or "LIVE BUY" in market_upper:
        hero_tone = "hero-bull"
    elif "BREAKDOWN" in market_upper or "LIVE SELL" in market_upper:
        hero_tone = "hero-bear"
    else:
        hero_tone = "hero-range"
    st.markdown(
        f"""
        <div class="hero-strip {hero_tone}">
            <div class="hero-grid">
                <div>
                    <div class="hero-kicker">Market Snapshot</div>
                    <div class="hero-symbol">{symbol}</div>
                    <div class="hero-price">Spot LTP {price_text}</div>
                    <div class="hero-change" style="color:{change_color};">{change_prefix}{day_change:.2f} vs previous close</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Strategy</div>
                    <div class="hero-value">{strategy}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Execution</div>
                    <div class="hero-value">{execution_mode}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Status</div>
                    <div class="hero-value">{market_status}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Support</div>
                    <div class="hero-value">{support_band}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Resistance</div>
                    <div class="hero-value">{resistance_band}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">CE / PE Bias</div>
                    <div class="hero-value">{option_bias}</div>
                </div>
                <div class="hero-tile">
                    <div class="hero-label">Open Trades</div>
                    <div class="hero-value">{status_text} / {open_trades}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )



def _sidebar_section(title: str, subtitle: str = "") -> None:
    text = f'<div class="live-section">{title}</div>'
    if subtitle:
        text += f'<div class="live-sub" style="margin-bottom:8px;">{subtitle}</div>'
    st.markdown(text, unsafe_allow_html=True)




def _render_page_footer() -> None:
    st.caption("KRSH Solutions desk workspace")


def _render_capability_band(
    execution_mode: str,
    account_status: str,
    signal_count: int,
    market_status: str,
    refresh_seconds: int,
    auto_execute: bool,
) -> None:
    auto_text = "Enabled" if auto_execute else "Manual review"
    paper_status = "Ready" if execution_mode == "PAPER" else "Standby"
    broker_status = account_status if execution_mode == "LIVE" else "Available in LIVE mode"
    st.markdown('<div class="section-shell" style="margin-bottom:14px;">', unsafe_allow_html=True)
    st.markdown('<div class="section-heading">Live Status & Quick Actions</div><div class="section-copy">This section is now a working status strip, not a decorative capability banner.</div>', unsafe_allow_html=True)
    status_cols = st.columns([1, 1, 1, 1, 0.9])
    status_cols[0].metric("Market", str(market_status))
    status_cols[1].metric("Signals", int(signal_count))
    status_cols[2].metric("Execution", str(execution_mode), auto_text)
    status_cols[3].metric("Broker", str(broker_status), paper_status)
    with status_cols[4]:
        st.caption(f"Refresh every {int(refresh_seconds)}s")
        if st.button("Refresh Now", width="stretch"):
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)



def _render_dhan_feature_sections(
    symbol: str,
    strategy: str,
    execution_mode: str,
    account_status: str,
    signal_count: int,
    auto_execute: bool,
) -> None:
    auto_text = "Auto execution enabled" if auto_execute else "Manual review before send"
    st.caption("Quick Sections")

    actions = [
        ("Live Signals", f"{int(signal_count)} setups from {strategy}", "Live Signals"),
        ("Paper & Live", f"Current mode: {execution_mode}", "Paper & Live"),
        ("Routing Status", f"{account_status}", "Routing Status"),
        ("Execution Style", auto_text, "Execution Style"),
        ("Instrument Focus", f"{symbol}", "Instrument Focus"),
    ]
    action_cols = st.columns(5)
    for col, (label, meta, section_name) in zip(action_cols, actions):
        with col:
            if st.button(label, key=f"home_tile_{section_name.lower().replace(' ', '_')}", width="stretch"):
                st.session_state["content_view"] = section_name
                st.rerun()
            st.caption(meta)




def _render_prepare_desk_page() -> None:
    st.caption("Prepare Desk: choose symbol, strategy, timeframe, and execution mode.")


def _render_review_signals_page() -> None:
    st.caption("Review Signals: validate setups on charts before execution.")


def _render_authorize_execute_page() -> None:
    st.caption("Authorize Execute: send only reviewed trades when ready.")


def _render_desk_summary_page(workspace: str, strategy: str, content_view: str, risk_text: str, account_status: str) -> None:
    st.caption("Desk Summary: workspace, strategy, section, risk, and account state.")
    cols = st.columns(5)
    cols[0].metric("Workspace", str(workspace))
    cols[1].metric("Strategy", str(strategy))
    cols[2].metric("Section", str(content_view))
    cols[3].metric("Risk", str(risk_text))
    cols[4].metric("Account", str(account_status))


def _render_live_signals_page(signal_rows: list[dict[str, object]], strategy: str, last_signal_side: str, watchlist_rows: list[dict[str, object]] | None = None) -> None:
    st.caption("Live Signals: current setup count, latest signal, and contract details.")
    signal_count = len(signal_rows)
    latest_signal = dict(signal_rows[-1]) if signal_rows else {}

    top_cols = st.columns(3)
    top_cols[0].metric("Setups", int(signal_count))
    top_cols[1].metric("Latest Signal", str(last_signal_side))
    top_cols[2].metric("Strategy", str(strategy))

    if not latest_signal:
        st.info("No actionable BUY/SELL signal is available yet for the selected strategy.")
        if watchlist_rows:
            st.caption("Top setup ideas from current market data")
            st.dataframe(pd.DataFrame(watchlist_rows), width="stretch", hide_index=True)
        return

    detail_cols = st.columns(4)
    detail_cols[0].metric("Strike", str(latest_signal.get("option_strike", "-")))
    detail_cols[1].metric("Expiry", str(latest_signal.get("option_expiry", "-")))
    detail_cols[2].metric("Lots / Qty", f"{latest_signal.get('lots', '-')} / {latest_signal.get('quantity', '-')}")
    detail_cols[3].metric("Option LTP", _fmt_num(latest_signal.get("option_ltp")), str(latest_signal.get("option_ltp_reason", "")))

    trade_cols = st.columns(4)
    trade_cols[0].metric("Entry", _fmt_num(latest_signal.get("entry_price")))
    trade_cols[1].metric("Stop Loss", _fmt_num(latest_signal.get("stop_loss")))
    trade_cols[2].metric("Target", _fmt_num(latest_signal.get("target_price")))
    trade_cols[3].metric("Order Value", _fmt_num(latest_signal.get("order_value")))

    latest_view = {
        "time": latest_signal.get("entry_time", latest_signal.get("timestamp", "-")),
        "side": latest_signal.get("side", "-"),
        "entry_price": latest_signal.get("entry_price", "-"),
        "stop_loss": latest_signal.get("stop_loss", "-"),
        "target_price": latest_signal.get("target_price", "-"),
        "option_strike": latest_signal.get("option_strike", "-"),
        "option_expiry": latest_signal.get("option_expiry", "-"),
        "option_ltp": latest_signal.get("option_ltp", "-"),
        "option_ltp_reason": latest_signal.get("option_ltp_reason", "-"),
        "lots": latest_signal.get("lots", "-"),
        "quantity": latest_signal.get("quantity", "-"),
        "order_value": latest_signal.get("order_value", "-"),
    }
    st.caption("Latest actionable signal")
    st.dataframe(pd.DataFrame([latest_view]), width="stretch", hide_index=True)

    history_cols = [
        "trade_label",
        "side",
        "entry_price",
        "stop_loss",
        "target_price",
        "option_strike",
        "option_expiry",
        "option_ltp",
        "option_ltp_reason",
        "lots",
        "quantity",
        "order_value",
        "entry_time",
    ]
    history_rows = [{k: row.get(k, "") for k in history_cols} for row in signal_rows[-8:]]
    st.caption("Recent actionable signals")
    st.dataframe(pd.DataFrame(history_rows), width="stretch", hide_index=True)

def _render_paper_live_page(execution_mode: str, account_status: str) -> None:
    st.caption("Paper & Live: current execution mode and desk readiness.")
    cols = st.columns(2)
    cols[0].metric("Current Mode", str(execution_mode))
    cols[1].metric("Account Status", str(account_status))


def _render_routing_status_page(account_status: str, execution_mode: str, broker_label: str) -> None:
    st.caption("Routing Status: broker connectivity and reviewed trade routing.")
    cols = st.columns(3)
    cols[0].metric("Routing", str(account_status))
    cols[1].metric("Mode", str(execution_mode))
    cols[2].metric("Broker", str(broker_label))


def _render_execution_style_page(auto_execute: bool, refresh_seconds: int, send_telegram: bool) -> None:
    auto_text = "Auto execution enabled" if auto_execute else "Manual review before send"
    telegram_text = "Telegram alerts ON" if send_telegram else "Telegram alerts OFF"
    st.caption("Execution Style: manual or auto, refresh cadence, and alerts.")
    cols = st.columns(3)
    cols[0].metric("Style", auto_text)
    cols[1].metric("Refresh", f"Every {int(refresh_seconds)}s")
    cols[2].metric("Alerts", telegram_text)


def _render_instrument_focus_page(symbol: str, instrument_mode: str, interval: str, period: str) -> None:
    st.caption("Instrument Focus: symbol, instrument, and scan window.")
    cols = st.columns(3)
    cols[0].metric("Symbol", str(symbol))
    cols[1].metric("Instrument", str(instrument_mode))
    cols[2].metric("Scan Window", f"{interval} / {period}")



def _render_strategy_page(strategy: str, workspace: str, symbol: str, interval: str, period: str, execution_mode: str) -> None:
    strategy_options = ["Breakout", "Demand Supply", "Indicator", "One Trade/Day", "MTF 5m"]
    st.caption("Strategy")
    selected_strategy = st.segmented_control(
        "Strategy",
        strategy_options,
        default=strategy if strategy in strategy_options else strategy_options[0],
        label_visibility="collapsed",
        width="stretch",
        key="strategy_page_selector",
    )
    if selected_strategy and str(selected_strategy) != str(strategy):
        st.session_state["strategy"] = str(selected_strategy)
        st.rerun()
    strategy_copy = {
        "Breakout": "Focuses on breakout moves after price clears structure and momentum confirms continuation.",
        "Demand Supply": "Tracks reaction zones and setup quality around demand and supply behavior.",
        "Indicator": "Uses indicator-led market context to classify bullish, bearish, and reversal-style setups.",
        "One Trade/Day": "Keeps execution disciplined by limiting the flow to a single high-conviction setup each day.",
        "MTF 5m": "Uses 5m execution with higher timeframe filters to improve structure and confirmation quality.",
    }.get(str(strategy), "Review the currently selected strategy before running the desk.")
    st.caption("Strategy: active model and current market context.")
    st.markdown(
        f"""
        <div class="section-shell" style="margin-bottom:14px;">
            <div class="hero-chip-row">
                <div class="hero-chip"><div class="hero-chip-label">Active Strategy</div><div class="hero-chip-meta">{strategy}</div></div>
                <div class="hero-chip"><div class="hero-chip-label">Workspace</div><div class="hero-chip-meta">{workspace}</div></div>
                <div class="hero-chip"><div class="hero-chip-label">Symbol</div><div class="hero-chip-meta">{symbol}</div></div>
                <div class="hero-chip"><div class="hero-chip-label">Timeframe</div><div class="hero-chip-meta">{interval} / {period}</div></div>
                <div class="hero-chip"><div class="hero-chip-label">Mode</div><div class="hero-chip-meta">{execution_mode}</div></div>
            </div>
        </div>
        <div class="section-shell" style="margin-bottom:14px;">
            <div class="section-heading" style="font-size:18px;">Strategy Focus</div>
            <div class="section-copy" style="font-size:14px; line-height:1.7; margin-bottom:0;">{strategy_copy}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def main() -> None:
    _render_sidebar_shell()
    masthead_slot = st.empty()

    workspace = "Desk"
    strategy_options = ["Breakout", "Demand Supply", "Indicator", "One Trade/Day", "MTF 5m"]
    strategy = str(st.session_state.get("strategy", "Breakout"))

    content_options = ["Home", "Live Signals", "Market", "Strategy", "Trades", "Desk Controls", "Downloads"]
    if st.session_state.get("content_view") not in content_options:
        st.session_state["content_view"] = "Home"
    content_view = str(st.session_state["content_view"])
    show_controls = content_view == "Desk Controls"


    if content_view == "Home":
        st.caption("Simple Main Page")
    symbol = str(st.session_state.get("symbol", "^NSEI"))
    interval = str(st.session_state.get("interval", "5m"))
    period = str(st.session_state.get("period", "1d"))

    show_chart_review = content_view not in {"Desk Controls", "Live Signals", "Market"}
    if show_chart_review:
        st.caption("Chart Review")
        review_col1, review_col2, review_col3 = st.columns([1.2, 1.2, 1.0])
        with review_col1:
            symbol = st.text_input("Symbol", symbol, key="global_symbol")
        with review_col2:
            interval = st.segmented_control(
                "Timeframe",
                ["1m", "5m", "15m", "30m", "1h"],
                default="5m" if strategy == "MTF 5m" else interval,
                key="global_interval",
                disabled=(strategy == "MTF 5m"),
                width="stretch",
            )
        with review_col3:
            period = st.segmented_control(
                "Period",
                ["1d", "5d", "1mo", "3mo"],
                default=period,
                key="global_period",
                width="stretch",
            )
    execution_mode = "PAPER"
    instrument_mode = "Options"
    lot_size = 65
    lots = 2
    capital = 100000
    risk_pct = 1.0
    rr_ratio = 2.0
    trailing_sl_pct = 1.0
    auto_execute_generated = False
    live_update = False
    refresh_seconds = 10
    send_telegram = False
    paper_log_output = "data/paper_trading_logs_all.csv"
    live_log_output = "data/live_trading_logs_all.csv"
    dhan_client_id = ""
    dhan_token_present = False
    dhan_security_map_path = "data/dhan_security_map.csv"
    strike_step = 50
    moneyness = "ATM"
    strike_steps = 0
    fetch_option_metrics = False
    mtf_ema_period = 3
    mtf_setup_mode = "either"
    mtf_retest_strength = True
    mtf_max_trades_per_day = 3

    if show_controls:
        st.caption("Desk Controls")
        market_col, position_col, access_col = st.columns([1.15, 1.15, 1.1])

        with market_col:
            st.caption("Market Setup")
            st.caption("Choose what to scan and how the strategy should read the market.")
            symbol = st.text_input("Symbol", symbol)
            interval = st.segmented_control("Interval", ["1m", "5m", "15m", "30m", "1h"], default=interval, disabled=(strategy == "MTF 5m"))
            period = st.segmented_control("Period", ["1d", "5d", "1mo", "3mo"], default=period)
            execution_mode = st.segmented_control("Execution mode", ["PAPER", "LIVE"], default=execution_mode)
            instrument_mode = st.segmented_control("Instrument", ["Options", "Futures"], default=instrument_mode)


        with position_col:
            st.caption("Position & Risk")
            st.caption("Define capital, sizing, and protection logic before trades are generated.")
            lot_size = st.number_input("Lot size", min_value=1, value=lot_size, step=1)
            lots = st.slider("Lots", 1, 10, lots)
            capital = st.number_input("Capital (INR)", min_value=1000, value=capital, step=1000)
            risk_pct = st.slider("Risk per trade (%)", 0.1, 10.0, risk_pct)
            rr_ratio = st.slider("Risk / Reward", 1.0, 10.0, rr_ratio)
            trailing_sl_pct = st.slider("Trailing stop loss %", 0.1, 10.0, trailing_sl_pct, 0.1)
            auto_execute_generated = st.toggle("Auto execute", value=auto_execute_generated)


        with access_col:
            st.caption("Execution Access")
            st.caption("Switch between paper and live routing, set refresh behavior, and verify broker readiness.")
            live_update = st.checkbox("Auto refresh", value=live_update)
            refresh_seconds = st.slider("Refresh every (seconds)", 2, 120, refresh_seconds)
            send_telegram = st.checkbox("Send Telegram alert", value=send_telegram)
            st.caption("Use Auto execute only after reviewing generated trades and payload previews.")
            st.markdown('<div class="section-copy" style="margin-top:8px; margin-bottom:8px;">Trade integration</div>', unsafe_allow_html=True)
            if execution_mode == "PAPER":
                paper_log_output = st.text_input("Paper trade log path", value=paper_log_output)
                st.info("Execution type: simulated")
                st.info("Broker: disabled")
            else:
                live_log_output = st.text_input("Live trade log path", value=live_log_output)
                dhan_security_map_path = st.text_input("Security map path", value=dhan_security_map_path)
                st.info("Broker: Dhan")
                dhan_client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
                dhan_token_present = bool(os.getenv("DHAN_ACCESS_TOKEN", "").strip())
                if dhan_client_id and dhan_token_present:
                    st.success("Dhan credentials detected")
                else:
                    st.warning("Add Dhan credentials to .env")
                if st.button("Check Dhan Live Ready", width="stretch"):
                    readiness_notes = _run_dhan_readiness_check(symbol, dhan_security_map_path)
                    for note in readiness_notes:
                        if note.startswith("FAIL"):
                            st.error(note)
                        elif note.startswith("WARN"):
                            st.warning(note)
                        elif note.startswith("PASS"):
                            st.success(note)
                        else:
                            st.info(note)


        if instrument_mode == "Options":
            st.markdown('<div class="section-shell" style="margin-bottom:14px;">', unsafe_allow_html=True)
            st.markdown('<div class="section-heading">Option Contract Controls</div><div class="section-copy">Fine-tune strike selection only when options are the selected instrument.</div>', unsafe_allow_html=True)
            option_cols = st.columns([1, 1, 1, 1])
            with option_cols[0]:
                strike_step = int(st.segmented_control("Strike step", [25, 50, 100], default=strike_step))
            with option_cols[1]:
                moneyness = st.segmented_control("Moneyness", ["ATM", "ITM", "OTM"], default=moneyness)
            with option_cols[2]:
                strike_steps = st.slider("ITM / OTM steps", 0, 5, strike_steps)
            with option_cols[3]:
                fetch_option_metrics = st.checkbox("Fetch option chain metrics", value=fetch_option_metrics)

        else:
            st.caption("Futures mode uses the monthly futures contract automatically.")

        if strategy == "MTF 5m":
            st.markdown('<div class="section-shell" style="margin-bottom:14px;">', unsafe_allow_html=True)
            st.markdown('<div class="section-heading">MTF 5m Controls</div><div class="section-copy">Extra higher-timeframe filters appear only for the MTF strategy workspace.</div>', unsafe_allow_html=True)
            mtf_cols = st.columns([1, 1, 1, 1])
            with mtf_cols[0]:
                mtf_ema_period = int(st.number_input("EMA period (1h)", min_value=2, max_value=20, value=mtf_ema_period, step=1))
            with mtf_cols[1]:
                mtf_setup_label = st.segmented_control("15m setup filter", ["Either", "BOS only", "FVG only"], default="Either")
                mtf_setup_mode = {"Either": "either", "BOS only": "bos", "FVG only": "fvg"}[str(mtf_setup_label)]
            with mtf_cols[2]:
                mtf_retest_strength = st.checkbox("Require strong 5m retest candle", value=mtf_retest_strength)
            with mtf_cols[3]:
                mtf_max_trades_per_day = int(st.segmented_control("Max trades/day", [1, 2, 3], default=mtf_max_trades_per_day))

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

    try:
        if strategy == "MTF 5m" and interval != "5m":
            st.warning("MTF 5m strategy requires the base interval to be 5m so it can derive 15m and 1h candles.")
            output_rows = []
        else:
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
                mtf_ema_period=int(mtf_ema_period),
                mtf_setup_mode=str(mtf_setup_mode),
                mtf_retest_strength=bool(mtf_retest_strength),
                mtf_max_trades_per_day=int(mtf_max_trades_per_day),
            )
    except Exception as exc:
        st.error(f"Strategy execution failed: {exc}")
        output_rows = []

    if output_rows:
        if instrument_mode == "Futures":
            output_rows = attach_futures_contracts(output_rows, symbol)
        output_rows = attach_lots(output_rows, lot_size=int(lot_size), lots=int(lots))


    latest_sidebar_price = candles["close"].iloc[-1] if not candles.empty else "-"
    signal_rows = [r for r in output_rows if str(r.get("side", "")).upper() in {"BUY", "SELL"}]
    watchlist_rows = _build_watchlist_candidates(candles, str(strategy), str(symbol), limit=3) if not signal_rows else []
    last_signal_side = str(signal_rows[-1].get("side", "-")) if signal_rows else "-"
    if send_telegram and output_rows and not auto_execute_generated:
        latest = None
        for r in reversed(output_rows):
            if str(r.get("side")) in {"BUY", "SELL"}:
                latest = r
                break
        if latest is None:
            latest = output_rows[-1]
        send_signal_alert(latest, strategy=strategy, symbol=symbol, refresh_seconds=int(refresh_seconds))

    execution_candidates: list[dict[str, object]] = []
    analyzed_candidates: list[dict[str, object]] = []
    if build_execution_candidates is not None:
        try:
            execution_candidates = build_execution_candidates(strategy, output_rows, symbol)
        except Exception as exc:
            st.warning(f"Could not build execution candidates: {exc}")
            execution_candidates = []
    if build_analysis_queue is not None:
        analyzed_candidates = build_analysis_queue(execution_candidates)

    auto_executed_rows: list[dict[str, object]] = []
    reviewed_auto_candidates = st.session_state.get("analyzed_trade_queue", [])
    if auto_execute_generated:
        if not reviewed_auto_candidates:
            st.info("Auto execute is enabled, but it only runs after you analyze trades and stage a reviewed BUY/SELL queue.")
        else:
            try:
                if execution_mode == "LIVE":
                    if execute_live_trades is None:
                        st.error("Live execution module is not available.")
                    else:
                        auto_executed_rows = execute_live_trades(reviewed_auto_candidates, Path(live_log_output), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
                else:
                    if execute_paper_trades is None:
                        st.error("Paper execution module is not available.")
                    else:
                        auto_executed_rows = execute_paper_trades(reviewed_auto_candidates, Path(paper_log_output), deduplicate=True)
            except Exception as exc:
                st.error(f"Auto execution failed: {exc}")
                auto_executed_rows = []

            if auto_executed_rows:
                st.success(f"Auto executed {len(auto_executed_rows)} reviewed trade(s) in {execution_mode} mode.")
                st.session_state["analyzed_trade_queue"] = []
                if execution_mode == "LIVE":
                    _render_live_execution_feedback(auto_executed_rows)
                if send_telegram:
                    signal_map = {
                        f"{row.get('strategy','')}|{row.get('symbol','')}|{row.get('entry_time', row.get('timestamp',''))}|{row.get('side','')}": row
                        for row in output_rows
                        if isinstance(row, dict)
                    }
                    for executed in auto_executed_rows:
                        exec_key = f"{executed.get('strategy','')}|{executed.get('symbol','')}|{executed.get('signal_time','')}|{executed.get('side','')}"
                        alert_row = dict(signal_map.get(exec_key, {}))
                        alert_row.update(executed)
                        send_signal_alert(alert_row, strategy=strategy, symbol=symbol, refresh_seconds=int(refresh_seconds))
            else:
                st.info("Reviewed queue is staged for auto execution, but no new trades were executed on this run.")

    account_status = "Paper"
    if str(execution_mode).upper() == "LIVE":
        account_status = "Dhan Ready" if dhan_client_id and dhan_token_present else "Dhan Missing"

    risk_text = f"{float(risk_pct):.1f}% risk / {float(rr_ratio):.1f}R"
    with masthead_slot.container():
        _render_page_masthead(
            symbol=str(symbol),
            strategy=str(strategy),
            execution_mode=str(execution_mode),
            auto_execute=bool(auto_execute_generated),
            workspace=str(workspace),
            content_view=str(content_view),
            interval=str(interval),
            period=str(period),
            instrument_mode=str(instrument_mode),
            lots=int(lots),
            lot_size=int(lot_size),
            risk_pct=float(risk_pct),
            rr_ratio=float(rr_ratio),
            last_signal_side=str(last_signal_side),
            open_trades=len(signal_rows),
            account_status=account_status,
        )

    hero_last_price = float(candles["close"].iloc[-1]) if not candles.empty else 0.0
    if not candles.empty and len(candles) >= 2:
        hero_day_change = float(candles["close"].iloc[-1]) - float(candles["close"].iloc[-2])
    else:
        hero_day_change = 0.0
    hero_levels = compute_market_levels(candles) if not candles.empty else {"support_low": 0.0, "support_high": 0.0, "resistance_low": 0.0, "resistance_high": 0.0}
    support_band = f"{hero_levels['support_low']:.2f}-{hero_levels['support_high']:.2f}" if hero_levels['support_high'] else "-"
    resistance_band = f"{hero_levels['resistance_low']:.2f}-{hero_levels['resistance_high']:.2f}" if hero_levels['resistance_high'] else "-"
    ce_count = sum(1 for r in signal_rows if str(r.get("option_type", "")).upper() == "CE")
    pe_count = sum(1 for r in signal_rows if str(r.get("option_type", "")).upper() == "PE")
    if ce_count > pe_count:
        option_bias = f"CE {ce_count}:{pe_count}"
    elif pe_count > ce_count:
        option_bias = f"PE {pe_count}:{ce_count}"
    elif ce_count == pe_count and ce_count > 0:
        option_bias = f"BAL {ce_count}:{pe_count}"
    else:
        option_bias = "NEUTRAL"
    if hero_last_price and hero_levels["resistance_high"] and hero_last_price >= hero_levels["resistance_high"]:
        market_status = "BREAKOUT"
    elif hero_last_price and hero_levels["support_low"] and hero_last_price <= hero_levels["support_low"]:
        market_status = "BREAKDOWN"
    else:
        market_status = "RANGE" if not signal_rows else f"LIVE {last_signal_side}"
    if content_view == "Home":
        _render_hero_strip(
            symbol=str(symbol),
            last_price=hero_last_price if not candles.empty else "-",
            day_change=float(hero_day_change),
            strategy=str(strategy),
            execution_mode=str(execution_mode),
            open_trades=len(signal_rows),
            support_band=support_band,
            resistance_band=resistance_band,
            option_bias=option_bias,
            market_status=market_status,
        )
    if content_view == "Live Signals":
        _render_live_signals_page(signal_rows, str(strategy), str(last_signal_side), watchlist_rows)
    if content_view == "Paper & Live":
        _render_paper_live_page(str(execution_mode), account_status)
    if content_view == "Routing Status":
        _render_routing_status_page(account_status, str(execution_mode), "Dhan" if str(execution_mode).upper() == "LIVE" else "Paper")
    if content_view == "Execution Style":
        _render_execution_style_page(bool(auto_execute_generated), int(refresh_seconds), bool(send_telegram))
    if content_view == "Instrument Focus":
        _render_instrument_focus_page(str(symbol), str(instrument_mode), str(interval), str(period))
    if content_view == "Strategy":
        _render_strategy_page(str(strategy), str(workspace), str(symbol), str(interval), str(period), str(execution_mode))
    if content_view == "Desk Summary":
        _render_desk_summary_page(str(workspace), str(strategy), str(content_view), risk_text, account_status)
    if content_view == "Prepare Desk":
        _render_prepare_desk_page()
    if content_view == "Review Signals":
        _render_review_signals_page()
    if content_view == "Authorize Execute":
        _render_authorize_execute_page()
    if content_view == "Live Status":
        _render_capability_band(
            execution_mode=str(execution_mode),
            account_status=account_status,
            signal_count=len(signal_rows),
            market_status=market_status,
            refresh_seconds=int(refresh_seconds),
            auto_execute=bool(auto_execute_generated),
        )
    if content_view == "Home":
        _render_dhan_feature_sections(
            symbol=str(symbol),
            strategy=str(strategy),
            execution_mode=str(execution_mode),
            account_status=account_status,
            signal_count=len(signal_rows),
            auto_execute=bool(auto_execute_generated),
        )
    if content_view == "Market":
        st.markdown('<div class="section-shell">', unsafe_allow_html=True)
        st.caption("Market Overview: latest price, range, and recent candles.")
        c1, c2, c3, c4 = st.columns(4)
    
        if not candles.empty:
            latest_close = float(candles["close"].iloc[-1])
            latest_high = float(candles["high"].iloc[-1])
            latest_low = float(candles["low"].iloc[-1])
            latest_volume_text, latest_volume_note = _format_volume_metric(str(symbol), candles)
        else:
            latest_close = latest_high = latest_low = 0.0
            latest_volume_text, latest_volume_note = "-", "No volume data"
    
        c1.metric("Close", round(latest_close, 2))
        c2.metric("High", round(latest_high, 2))
        c3.metric("Low", round(latest_low, 2))
        c4.metric("Volume", latest_volume_text, latest_volume_note)
    
        if not candles.empty:
            st.dataframe(candles.tail(6), width="stretch")
        else:
            st.warning("No candle data available.")
        st.markdown("</div>", unsafe_allow_html=True)
    
        st.markdown('<div class="chart-shell">', unsafe_allow_html=True)
        st.caption("Market Chart: intraday candles with levels and depth.")
        if not candles.empty:
            latest_move = 0.0
            if len(candles) >= 2:
                try:
                    latest_move = float(candles["close"].iloc[-1]) - float(candles["close"].iloc[-2])
                except Exception:
                    latest_move = 0.0
    
            levels = compute_market_levels(candles)
            move_color = "#16a34a" if latest_move >= 0 else "#dc2626"
            move_prefix = "+" if latest_move > 0 else ""
            st.markdown(
                f"""
                <div style=\"background:#f8fafc;border:1px solid #dbe4ee;border-radius:18px;padding:6px 8px;margin-bottom:4px;box-shadow:0 10px 24px rgba(15,23,42,0.05);\"> 
                    <div style=\"display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap;\">
                        <div>
                            <div style=\"color:#64748b;font-size:12px;letter-spacing:0.12em;text-transform:uppercase;\">Live Price</div>
                            <div style=\"color:#0f172a;font-size:14px;font-weight:700;line-height:1.05;\">{levels['last_price']:.2f}</div>
                        </div>
                        <div style=\"text-align:right;\">
                            <div style=\"color:{move_color};font-size:14px;font-weight:700;\">{move_prefix}{latest_move:.2f}</div>
                            <div style=\"color:#64748b;font-size:12px;\">vs previous candle close</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    
            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Session High", round(levels["session_high"], 2))
            h2.metric("Session Low", round(levels["session_low"], 2))
            h3.metric("Support Band", f"{levels['support_low']:.2f}-{levels['support_high']:.2f}")
            h4.metric("Resistance Band", f"{levels['resistance_low']:.2f}-{levels['resistance_high']:.2f}")
    
            left, right = st.columns([5.6, 1.0])
            with left:
                chart = build_live_market_chart(candles, output_rows=output_rows)
                st.altair_chart(chart, width="stretch", height=240)
                st.caption("Standard candlestick chart with volume and optional BUY/SELL or CE/PE trade markers.")
            with right:
                st.markdown("**Market Depth View**")
                depth_df = build_market_depth_summary(candles)
                st.dataframe(depth_df, width="stretch", hide_index=True)
                st.caption(f"Price spread between support and resistance bands: {levels['spread']:.2f}")
        else:
            st.info("No chart data available.")
        st.markdown("</div>", unsafe_allow_html=True)
    
    if content_view == "Trades":
        st.markdown('<div class="section-shell">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">Trade Workspace</div><div class="section-copy">Review live-ready setups, broker readiness, and only the reviewed orders you actually want routed.</div>', unsafe_allow_html=True)
        _render_dhan_status_panel(str(symbol), dhan_security_map_path, str(execution_mode), st.session_state.get("analyzed_trade_queue", []))

        if auto_executed_rows:
            with st.expander(f"Auto-executed trades ({len(auto_executed_rows)})", expanded=False):
                st.dataframe(_order_trade_columns(pd.DataFrame(auto_executed_rows)), width="stretch")

        if output_rows:
            trades_df = pd.DataFrame(output_rows)
            with st.expander(f"Generated Trades ({len(trades_df)})", expanded=False):
                st.dataframe(trades_df.tail(12), width="stretch")

            with st.expander("Trade Summary", expanded=False):
                try:
                    summary = build_trade_summary(output_rows)
                    st.text(summary)
                except Exception as exc:
                    st.warning(f"Could not build trade summary: {exc}")

            csv_data = _to_csv(output_rows)
            st.download_button("Download CSV", data=csv_data, file_name="trades.csv", mime="text/csv")
        else:
            st.caption("No trades generated yet.")

        st.markdown('<div class="section-copy" style="margin-top:8px; margin-bottom:8px;">Analyze First, Execute Later</div>', unsafe_allow_html=True)
        if execution_candidates:
            st.caption("Current executable candidates generated from the latest strategy run.")
            with st.expander(f"Execution Candidates ({len(execution_candidates)})", expanded=False):
                st.dataframe(_order_trade_columns(pd.DataFrame(execution_candidates)), width="stretch")
            if execution_mode == "LIVE":
                with st.expander("Dhan Live Payload Preview"):
                    if st.button("Preview Live Payloads", width="stretch"):
                        st.session_state["dhan_payload_preview"] = _build_dhan_preview_rows(
                            execution_candidates,
                            dhan_security_map_path,
                        )
                    preview_rows = st.session_state.get("dhan_payload_preview", [])
                    if preview_rows:
                        st.dataframe(pd.DataFrame(preview_rows), width="stretch")
                    else:
                        st.caption("Preview the exact Dhan live-order payloads here before sending them to the broker.")
        else:
            st.info("No execution candidates are available for the current strategy output.")
            if watchlist_rows:
                st.caption("Top setup ideas from current market data")
                st.dataframe(pd.DataFrame(watchlist_rows), width="stretch", hide_index=True)
    
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Analyze Current Trades", width="stretch"):
                st.session_state["analyzed_trade_queue"] = analyzed_candidates
                if analyzed_candidates:
                    st.success(f"Analyzed {len(analyzed_candidates)} executable trade(s). Review them below before execution.")
                else:
                    st.info("No actionable BUY/SELL signal is available yet. Current output contains analysis/setup rows only.")
        with c2:
            if st.button("Clear Analyzed Queue", width="stretch"):
                st.session_state["analyzed_trade_queue"] = []
                st.info("Cleared the analyzed trade queue.")
        with c3:
            st.caption(f"Execution mode: {execution_mode}")
    
        staged_candidates = st.session_state.get("analyzed_trade_queue", [])
        if staged_candidates:
            st.success(f"Reviewed queue ready: {len(staged_candidates)} actionable trade(s) available for execution.")
            st.caption("Only this staged reviewed queue can be executed manually or by auto execute.")
            with st.expander(f"Reviewed Queue ({len(staged_candidates)})", expanded=True):
                st.dataframe(_order_trade_columns(pd.DataFrame(staged_candidates)), width="stretch")

            executed_rows: list[dict[str, object]] = []
            if st.button("Execute Reviewed Trades", type="primary", width="stretch"):
                st.session_state["confirm_execute_reviewed"] = True

            if st.session_state.get("confirm_execute_reviewed", False):
                st.warning(f"Confirm execution of {len(staged_candidates)} reviewed trade(s) in {execution_mode} mode.")
                confirm_cols = st.columns(2)
                with confirm_cols[0]:
                    confirm_execute = st.button("Confirm Execute", type="primary", width="stretch")
                with confirm_cols[1]:
                    cancel_execute = st.button("Cancel", width="stretch")

                if cancel_execute:
                    st.session_state["confirm_execute_reviewed"] = False
                    st.info("Execution cancelled.")

                if confirm_execute:
                    st.session_state["confirm_execute_reviewed"] = False
                    if execution_mode == "LIVE":
                        if execute_live_trades is None:
                            st.error("Live execution module is not available.")
                        else:
                            executed_rows = execute_live_trades(staged_candidates, Path(live_log_output), deduplicate=True, **_resolve_live_execution_kwargs(dhan_security_map_path))
                    else:
                        if execute_paper_trades is None:
                            st.error("Paper execution module is not available.")
                        else:
                            executed_rows = execute_paper_trades(staged_candidates, Path(paper_log_output), deduplicate=True)

                    if executed_rows:
                        st.success(f"Executed {len(executed_rows)} reviewed trade(s) in {execution_mode} mode.")
                        st.dataframe(_order_trade_columns(pd.DataFrame(executed_rows)), width="stretch")
                        if execution_mode == "LIVE":
                            _render_live_execution_feedback(executed_rows)
                    else:
                        st.warning("No new reviewed trades were executed. They may already be logged.")
        else:
            st.info("Analyze trades first to build a review queue, then execute that reviewed batch later.")
        st.markdown("</div>", unsafe_allow_html=True)
    raw_candles_csv = candles.to_csv(index=False) if not candles.empty else "timestamp,open,high,low,close,volume`n"
    debug_payload = {
        "strategy": strategy,
        "workspace": workspace,
        "symbol": symbol,
        "execution_mode": execution_mode,
        "instrument_mode": instrument_mode,
        "output_rows_count": len(output_rows) if isinstance(output_rows, list) else 0,
        "execution_candidates_count": len(execution_candidates),
        "reviewed_queue_count": len(st.session_state.get("analyzed_trade_queue", [])),
    }

    if content_view == "Downloads":
        st.markdown('<div class="section-shell">', unsafe_allow_html=True)
        st.markdown('<div class="section-heading">Downloads</div><div class="section-copy">Raw data and debug output are hidden from the page and available only as file downloads.</div>', unsafe_allow_html=True)
        download_cols = st.columns(3)
        with download_cols[0]:
            st.download_button("Download Raw Candles CSV", data=raw_candles_csv, file_name="krsh_raw_candles.csv", mime="text/csv", width="stretch")
        with download_cols[1]:
            st.download_button("Download Trades CSV", data=_to_csv(output_rows) if output_rows else "", file_name="krsh_trades.csv", mime="text/csv", width="stretch")
        with download_cols[2]:
            st.download_button("Download Debug JSON", data=json.dumps(debug_payload, indent=2), file_name="krsh_debug.json", mime="application/json", width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    _render_page_footer()


if __name__ == "__main__":
    main()
