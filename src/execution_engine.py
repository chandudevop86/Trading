from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
}
time_str = "2026-03-15 10:30:00"
dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

ts = dt.timestamp()
print(ts)

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


def build_execution_candidates(strategy: str, output_rows: list[dict[str, object]], symbol: str) -> list[dict[str, object]]:
    symbol = symbol.strip() or "UNKNOWN"
    candidates: list[dict[str, object]] = []

    if strategy == "Indicator (RSI/ADX/MACD+VWAP)":
        if not output_rows:
            return candidates
        last = output_rows[-1]
        signal = str(last.get("market_signal", "NEUTRAL"))
        side = "HOLD"
        if signal in {"BULLISH_TREND", "OVERSOLD"}:
            side = "BUY"
        elif signal in {"BEARISH_TREND", "OVERBOUGHT"}:
            side = "SELL"
        side = "BUY" if signal == "LONG" else "SELL"
    trade = {
        "strategy": "INDICATOR",
        "symbol": symbol,
        "signal_time": str(last.get("timestamp", "")),
        "side": side,
        "price": float(last.get("close", 0)),
        "share_price": float(last.get("close", 0)),
        "strike_price": last.get("strike_price"),
        "quantity": default_quantity_for_symbol(symbol),
        "reason": signal,
    }

candidates.append(trade)

return candidates   
        
for row in output_rows:
        share_price, strike_price = _extract_share_and_strike(row)
        candidates.append(
            {
                "strategy": str(row.get("strategy", "TRADE_BOT")),
                "symbol": symbol,
                "signal_time": str(row.get("entry_time", row.get("timestamp", ""))),
                "side": str(row.get("side", "HOLD")),
                "price": row.get("entry_price", row.get("close", "")),
                "share_price": share_price,
                "strike_price": strike_price,
                "quantity": normalize_order_quantity(symbol, row.get("quantity", default_quantity_for_symbol(symbol))),
                "reason": f"SL:{row.get('stop_loss', '')} TSL:{row.get('trailing_stop_loss', '')} TP:{row.get('target_price', '')}",
            }
        )

    return candidates


def _existing_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    keys: set[str] = set()
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add(f"{row.get('strategy','')}|{row.get('symbol','')}|{row.get('signal_time','')}|{row.get('side','')}")
    return keys


def _first_execution_time(path: Path, execution_type: str) -> Optional[datetime]:
    if not path.exists():
        return None

    earliest: Optional[datetime] = None
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
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