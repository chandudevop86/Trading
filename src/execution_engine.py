from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from src.csv_io import read_csv_rows
DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
}


def default_quantity_for_symbol(symbol: str) -> int:
    normalized = symbol.strip().upper()
    return int(DEFAULT_LOT_SIZES.get(normalized, 1))


def normalize_order_quantity(symbol: str, quantity: object) -> int:
    lot = default_quantity_for_symbol(symbol)
    try:
        qty = int(float(quantity))
    except (TypeError, ValueError):
        qty = lot

    if qty <= 0:
        return lot
    if lot <= 1:
        return qty

    return max(lot, (qty // lot) * lot)


def _extract_share_and_strike(row: dict[str, object]) -> tuple[object, object]:
    share_price = row.get("entry_price", row.get("close", row.get("price", "")))
    strike_price = row.get("strike_price", row.get("option_strike", row.get("strike", "")))
    return share_price, strike_price


def _parse_dt(text: object) -> Optional[datetime]:
    raw = str(text or "").strip()
    if not raw:
        return None

    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue

    return None


def build_execution_candidates(strategy: str, output_rows: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    symbol = symbol.strip() or "UNKNOWN"
    candidates: list[dict[str, object]] = []

    if not output_rows:
        return candidates

    if strategy == "Indicator (RSI/ADX/MACD+VWAP)":
        last = output_rows[-1]
        signal = str(last.get("market_signal", "NEUTRAL")).strip().upper()

        side = "HOLD"
        if signal in {"BULLISH_TREND", "OVERSOLD", "LONG", "BUY"}:
            side = "BUY"
        elif signal in {"BEARISH_TREND", "OVERBOUGHT", "SHORT", "SELL"}:
            side = "SELL"

        close_val = last.get("close", last.get("price", 0.0))
        try:
            close_price = float(close_val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            close_price = 0.0

        trade = {
            "strategy": "INDICATOR",
            "symbol": symbol,
            "signal_time": str(last.get("timestamp", "")),
            "side": side,
            "price": close_price,
            "share_price": close_price,
            "strike_price": last.get("strike_price"),
            "quantity": default_quantity_for_symbol(symbol),
            "reason": signal,
        }

        candidates.append(trade)
        return candidates

    for row in output_rows:
        share_price, strike_price = _extract_share_and_strike(row)

        option_type = str(row.get("option_type", "")).strip().upper()
        if not option_type:
            side_val = str(row.get("side", "")).strip().upper()
            if side_val == "BUY":
                option_type = "CE"
            elif side_val == "SELL":
                option_type = "PE"

        option_strike = str(row.get("option_strike", "")).strip()
        if not option_strike and option_type and strike_price not in {None, ""}:
            try:
                strike_int = int(float(str(strike_price)))
                option_strike = f"{strike_int}{option_type}"
            except Exception:
                option_strike = f"{strike_price}{option_type}"

        candidates.append(
            {
                "strategy": str(row.get("strategy", "TRADE_BOT")),
                "symbol": symbol,
                "signal_time": str(row.get("entry_time", row.get("timestamp", ""))),
                "side": str(row.get("side", "HOLD")),
                "price": row.get("entry_price", row.get("close", "")),
                "share_price": share_price,
                "strike_price": strike_price,
                "option_type": option_type,
                "option_strike": option_strike,
                "trade_no": row.get("trade_no", ""),
                "trade_label": row.get("trade_label", ""),
                "target_1": row.get("target_1", ""),
                "target_2": row.get("target_2", ""),
                "target_3": row.get("target_3", ""),
                "spot_ltp": row.get("spot_ltp", row.get("close", row.get("share_price", ""))),
                "option_ltp": row.get("option_ltp", ""),
                "lots": row.get("lots", ""),
                "order_value": row.get("order_value", ""),
                "stop_loss": row.get("stop_loss", ""),
                "trailing_stop_loss": row.get("trailing_stop_loss", ""),
                "target_price": row.get("target_price", ""),
                "quantity": normalize_order_quantity(symbol, row.get("quantity", default_quantity_for_symbol(symbol))),
                "reason": f"SL:{row.get('stop_loss', '')} TSL:{row.get('trailing_stop_loss', '')} TP:{row.get('target_price', '')}",
            }
        )

    return candidates


def build_analysis_queue(
    candidates: list[dict[str, object]],
    analyzed_at_utc: Optional[str] = None,
) -> list[dict[str, object]]:
    actionable = [c for c in candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"}]
    if not actionable:
        return []

    stamp = analyzed_at_utc or datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    analyzed: list[dict[str, object]] = []
    for candidate in actionable:
        row = dict(candidate)
        row["analysis_status"] = "ANALYZED"
        row["analyzed_at_utc"] = stamp
        row["execution_ready"] = "YES"
        analyzed.append(row)
    return analyzed


def _existing_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    keys: set[str] = set()
    for row in read_csv_rows(path):
        keys.add(f"{row.get('strategy','')}|{row.get('symbol','')}|{row.get('signal_time','')}|{row.get('side','')}")
    return keys


def _first_execution_time(path: Path, execution_type: str) -> Optional[datetime]:
    if not path.exists():
        return None

    earliest: Optional[datetime] = None
    for row in read_csv_rows(path):
            if str(row.get("execution_type", "")).upper() != execution_type.upper():
                continue
            raw = str(row.get("executed_at_utc", "")).strip()
            if not raw:
                continue
            try:
                ts = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            except ValueError:
                continue
            earliest = ts if earliest is None else min(earliest, ts)
    return earliest


def live_trading_unlock_status(
    paper_log_path: Path,
    min_days: int = 30,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, int, str]:
    start = _first_execution_time(paper_log_path, "PAPER")
    if start is None:
        return False, 0, ""

    now = now_utc or datetime.now(UTC)
    days = max(0, (now - start).days)
    unlock_date = start.replace(microsecond=0) + timedelta(days=min_days)
    return days >= min_days, days, unlock_date.strftime("%Y-%m-%d %H:%M:%S UTC")


def execute_paper_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True) -> list[dict[str, object]]:
    executable = [c for c in candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"}]
    if not executable:
        return []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_write: list[dict[str, object]] = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    seen = _existing_keys(output_path) if deduplicate else set()

    for c in executable:
        key = f"{c.get('strategy','')}|{c.get('symbol','')}|{c.get('signal_time','')}|{c.get('side','')}"
        if deduplicate and key in seen:
            continue

        record = dict(c)
        record.setdefault("share_price", record.get("price", ""))
        record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
        record["quantity"] = normalize_order_quantity(str(record.get("symbol", "")), record.get("quantity"))
        record["execution_type"] = "PAPER"
        # "EXECUTED" means opened; it becomes "CLOSED" when we later apply exit rules.
        record["execution_status"] = "EXECUTED"
        record["executed_at_utc"] = now
        rows_to_write.append(record)
        seen.add(key)

    if not rows_to_write:
        return []

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_to_write[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)

    return rows_to_write


def execute_live_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True) -> list[dict[str, object]]:
    executable = [c for c in candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"}]
    if not executable:
        return []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_write: list[dict[str, object]] = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    seen = _existing_keys(output_path) if deduplicate else set()

    for c in executable:
        key = f"{c.get('strategy','')}|{c.get('symbol','')}|{c.get('signal_time','')}|{c.get('side','')}"
        if deduplicate and key in seen:
            continue

        record = dict(c)
        record.setdefault("share_price", record.get("price", ""))
        record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
        record["quantity"] = normalize_order_quantity(str(record.get("symbol", "")), record.get("quantity"))
        record["execution_type"] = "LIVE"
        record["execution_status"] = "SENT"
        record["executed_at_utc"] = now
        rows_to_write.append(record)
        seen.add(key)

    if not rows_to_write:
        return []

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows_to_write[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)

    return rows_to_write


def close_paper_trades(
    paper_log_path: Path,
    candles: list[dict[str, object]],
    *,
    max_hold_minutes: int = 60,
) -> list[dict[str, object]]:
    """Close open PAPER trades using SL/TP, trailing SL, time-exit, then EOD."""

    if not paper_log_path.exists():
        return []

    candle_rows: list[dict[str, object]] = []
    for c in candles or []:
        ts = _parse_dt(c.get("timestamp"))
        if ts is None:
            continue
        try:
            candle_rows.append(
                {
                    "timestamp": ts,
                    "open": float(c.get("open", 0) or 0),
                    "high": float(c.get("high", 0) or 0),
                    "low": float(c.get("low", 0) or 0),
                    "close": float(c.get("close", 0) or 0),
                }
            )
        except (TypeError, ValueError):
            continue

    candle_rows.sort(key=lambda r: r["timestamp"])
    if not candle_rows:
        return []

    existing_rows = read_csv_rows(paper_log_path)

    def _to_float(val: object) -> Optional[float]:
        try:
            if val is None or str(val).strip() == "":
                return None
            return float(val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    closed_now: list[dict[str, object]] = []
    updated_rows: list[dict[str, object]] = []

    for r in existing_rows:
        execution_type = str(r.get("execution_type", "")).upper()
        status = str(r.get("execution_status", "")).upper()
        if execution_type != "PAPER" or status in {"CLOSED", "EXITED"}:
            updated_rows.append(r)
            continue

        side = str(r.get("side", "")).upper()
        entry_time = _parse_dt(r.get("signal_time")) or _parse_dt(r.get("entry_time")) or _parse_dt(r.get("timestamp"))
        if entry_time is None or side not in {"BUY", "SELL"}:
            updated_rows.append(r)
            continue

        entry_price = _to_float(r.get("price", r.get("entry_price"))) or 0.0
        stop_loss = _to_float(r.get("stop_loss"))
        target_price = _to_float(r.get("target_price"))
        trail_stop = _to_float(r.get("trailing_stop_loss")) or stop_loss
        trailing_sl_pct = _to_float(r.get("trailing_sl_pct")) or 0.0

        exit_price: Optional[float] = None
        exit_time: Optional[datetime] = None
        exit_reason: str = ""

        time_exit_at = entry_time + timedelta(minutes=int(max_hold_minutes or 0)) if max_hold_minutes and max_hold_minutes > 0 else None

        for c in candle_rows:
            ts = c["timestamp"]
            if ts < entry_time:
                continue

            if trailing_sl_pct and trailing_sl_pct > 0 and trail_stop is not None:
                if side == "BUY":
                    trail_stop = max(float(trail_stop), float(c["high"]) * (1.0 - trailing_sl_pct))
                else:
                    trail_stop = min(float(trail_stop), float(c["low"]) * (1.0 + trailing_sl_pct))

            # Worst-case if both hit same candle -> stop first.
            if side == "BUY":
                if trail_stop is not None and float(c["low"]) <= float(trail_stop):
                    exit_price, exit_time, exit_reason = float(trail_stop), ts, "TRAILING_STOP" if stop_loss is not None and float(trail_stop) > float(stop_loss) else "STOP_LOSS"
                    break
                if target_price is not None and float(c["high"]) >= float(target_price):
                    exit_price, exit_time, exit_reason = float(target_price), ts, "TARGET"
                    break
            else:
                if trail_stop is not None and float(c["high"]) >= float(trail_stop):
                    exit_price, exit_time, exit_reason = float(trail_stop), ts, "TRAILING_STOP" if stop_loss is not None and float(trail_stop) < float(stop_loss) else "STOP_LOSS"
                    break
                if target_price is not None and float(c["low"]) <= float(target_price):
                    exit_price, exit_time, exit_reason = float(target_price), ts, "TARGET"
                    break

            if time_exit_at is not None and ts >= time_exit_at:
                exit_price, exit_time, exit_reason = float(c["close"]), ts, "TIME_EXIT"
                break

        if exit_price is None or exit_time is None:
            last = candle_rows[-1]
            exit_price, exit_time, exit_reason = float(last["close"]), last["timestamp"], "EOD"

        try:
            qty = int(float(r.get("quantity", 0) or 0))
        except (TypeError, ValueError):
            qty = 0

        pnl = 0.0
        if qty > 0:
            pnl = (float(exit_price) - float(entry_price)) * qty if side == "BUY" else (float(entry_price) - float(exit_price)) * qty

        r2 = dict(r)
        r2["execution_status"] = "CLOSED"
        r2["exit_time"] = exit_time.isoformat(sep=" ")
        r2["exit_price"] = round(float(exit_price), 4)
        r2["exit_reason"] = exit_reason
        r2["pnl"] = round(float(pnl), 2)
        if trail_stop is not None:
            r2["trailing_stop_loss"] = round(float(trail_stop), 4)

        updated_rows.append(r2)
        closed_now.append(r2)

    if not closed_now:
        return []

    fieldnames: list[str] = []
    for r in updated_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with paper_log_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return closed_now
