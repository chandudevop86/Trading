from __future__ import annotations

import csv
import hashlib
import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Tuple

from src.csv_io import read_csv_rows
try:
    from src.dhan_api import DhanClient, DhanExecutionError, build_order_request_from_candidate, load_security_map, resolve_security
except Exception:
    DhanClient = None  # type: ignore
    DhanExecutionError = ValueError  # type: ignore
    build_order_request_from_candidate = None  # type: ignore
    load_security_map = None  # type: ignore
    resolve_security = None  # type: ignore

DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
    "NIFTY FUT": 65,
    "NIFTY FUTURES": 65,
    "NIFTYFUT": 65,
}

TRADE_STATUS_NEW = "NEW"
TRADE_STATUS_REVIEWED = "REVIEWED"
TRADE_STATUS_PENDING_EXECUTION = "PENDING_EXECUTION"
TRADE_STATUS_EXECUTED = "EXECUTED"
TRADE_STATUS_BLOCKED = "BLOCKED"
TRADE_STATUS_OPEN = "OPEN"
TRADE_STATUS_CLOSED = "CLOSED"
TRADE_STATUS_ERROR = "ERROR"

SKIP_REASON_DUPLICATE_ACTIVE_TRADE = "DUPLICATE_ACTIVE_TRADE"
SKIP_REASON_DUPLICATE_EXECUTED_TRADE = "DUPLICATE_EXECUTED_TRADE"
SKIP_REASON_INVALID_SIDE = "INVALID_SIDE"
SKIP_REASON_MAX_TRADES_PER_DAY = "MAX_TRADES_PER_DAY"
SKIP_REASON_MAX_DAILY_LOSS = "MAX_DAILY_LOSS"
SKIP_REASON_MISSING_PRICE = "MISSING_PRICE"
SKIP_REASON_MISSING_QUANTITY = "MISSING_QUANTITY"
SKIP_REASON_BROKER_ERROR = "BROKER_ERROR"
SKIP_REASON_KILL_SWITCH = "KILL_SWITCH_ENABLED"

ACTIVE_TRADE_STATUSES = {
    TRADE_STATUS_REVIEWED,
    TRADE_STATUS_PENDING_EXECUTION,
    TRADE_STATUS_EXECUTED,
    TRADE_STATUS_OPEN,
}
EXECUTION_SUCCESS_STATUSES = {"EXECUTED", "SENT", "FILLED"}
EXECUTION_SCHEMA = [
    "trade_id",
    "trade_key",
    "trade_status",
    "position_status",
    "duplicate_reason",
    "blocked_reason",
    "validation_error",
    "strategy",
    "symbol",
    "data_symbol",
    "trade_symbol",
    "trading_symbol",
    "security_id",
    "exchange_segment",
    "instrument_type",
    "signal_time",
    "side",
    "price",
    "share_price",
    "strike_price",
    "option_expiry",
    "option_type",
    "option_strike",
    "quantity",
    "execution_type",
    "execution_status",
    "executed_at_utc",
    "reviewed_at_utc",
    "analyzed_at_utc",
    "broker_name",
    "broker_order_id",
    "broker_status",
    "broker_message",
    "broker_response_json",
    "risk_limit_reason",
    "pnl",
    "exit_time",
    "exit_price",
    "exit_reason",
]


@dataclass
class ExecutionResult:
    rows: list[dict[str, object]] = field(default_factory=list)
    executed_rows: list[dict[str, object]] = field(default_factory=list)
    blocked_rows: list[dict[str, object]] = field(default_factory=list)
    skipped_rows: list[dict[str, object]] = field(default_factory=list)
    error_rows: list[dict[str, object]] = field(default_factory=list)
    executed_count: int = 0
    blocked_count: int = 0
    skipped_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0

    def __iter__(self):
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, index: int) -> dict[str, object]:
        return self.rows[index]

    def __bool__(self) -> bool:
        return bool(self.rows)


def default_quantity_for_symbol(symbol: str) -> int:
    normalized = symbol.strip().upper()
    return int(DEFAULT_LOT_SIZES.get(normalized, 1))


def normalize_order_quantity(symbol: str, quantity: object) -> int:
    lot = default_quantity_for_symbol(symbol)
    try:
        qty = int(float(quantity))
    except (TypeError, ValueError):
        qty = 0

    if qty <= 0:
        return 0
    if lot <= 1:
        return qty

    return max(lot, (qty // lot) * lot)


def _extract_share_and_strike(row: dict[str, object]) -> tuple[object, object]:
    share_price = row.get("entry_price", row.get("close", row.get("price", "")))
    strike_price = row.get("strike_price", row.get("option_strike", row.get("strike", "")))
    return share_price, strike_price


def _parse_dt(text: object) -> Optional[datetime]:
    def _normalize(dt: datetime) -> datetime:
        # Normalize offset-aware values to naive UTC so CSV/log timestamps and
        # market-data timestamps remain directly comparable.
        if dt.tzinfo is not None:
            return dt.astimezone(UTC).replace(tzinfo=None)
        return dt

    raw = str(text or "").strip()
    if not raw:
        return None

    try:
        return _normalize(datetime.fromisoformat(raw))
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return _normalize(datetime.strptime(raw, fmt))
        except ValueError:
            continue

    return None


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _normalize_text(value: object) -> str:
    return str(value or "").strip().upper()


def _price_value(record: dict[str, object]) -> float:
    for key in ("entry_price", "price", "share_price", "close", "spot_ltp"):
        value = _safe_float(record.get(key))
        if value > 0:
            return value
    return 0.0


def make_trade_key(record: dict[str, object]) -> str:
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    side = _normalize_text(record.get("side"))
    instrument = _normalize_text(
        record.get("trading_symbol")
        or record.get("contract_symbol")
        or record.get("option_strike")
        or record.get("strike_price")
        or symbol
    )
    payload = "|".join([strategy, symbol, side, instrument])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def make_trade_id(record: dict[str, object]) -> str:
    existing = str(record.get("trade_id", "") or "").strip()
    if existing:
        return existing
    strategy = _normalize_text(record.get("strategy", "TRADE_BOT"))
    symbol = _normalize_text(record.get("symbol", "UNKNOWN"))
    signal_time = str(record.get("signal_time", record.get("entry_time", record.get("timestamp", ""))) or "").strip()
    side = _normalize_text(record.get("side"))
    entry_price = f"{_price_value(record):.6f}"
    option_key = _normalize_text(record.get("option_strike") or record.get("trading_symbol") or record.get("strike_price"))
    payload = "|".join([strategy, symbol, signal_time, side, entry_price, option_key])
    return str(uuid.uuid5(uuid.NAMESPACE_URL, payload))


def execution_candidate_key(record: dict[str, object]) -> str:
    return make_trade_key(record)


def _ensure_trade_identity(record: dict[str, object], *, default_status: str | None = None) -> dict[str, object]:
    normalized = dict(record)
    normalized.setdefault("strategy", str(normalized.get("strategy", "TRADE_BOT") or "TRADE_BOT"))
    normalized.setdefault("symbol", str(normalized.get("symbol", "UNKNOWN") or "UNKNOWN"))
    normalized["trade_key"] = str(normalized.get("trade_key", "") or make_trade_key(normalized))
    normalized["trade_id"] = make_trade_id(normalized)
    if default_status and not str(normalized.get("trade_status", "") or "").strip():
        normalized["trade_status"] = default_status
    return normalized


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
        close_price = _price_value(last)
        candidates.append(_ensure_trade_identity({
            "strategy": "INDICATOR",
            "symbol": symbol,
            "signal_time": str(last.get("timestamp", "")),
            "side": side,
            "price": close_price,
            "share_price": close_price,
            "strike_price": last.get("strike_price"),
            "quantity": default_quantity_for_symbol(symbol),
            "reason": signal,
        }, default_status=TRADE_STATUS_NEW))
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
                option_strike = f"{int(float(str(strike_price)))}{option_type}"
            except Exception:
                option_strike = f"{strike_price}{option_type}"
        candidates.append(_ensure_trade_identity({
            "strategy": str(row.get("strategy", "TRADE_BOT")),
            "symbol": symbol,
            "signal_time": str(row.get("entry_time", row.get("timestamp", ""))),
            "side": str(row.get("side", "HOLD")),
            "price": row.get("entry_price", row.get("close", "")),
            "share_price": share_price,
            "strike_price": strike_price,
            "option_expiry": row.get("option_expiry", row.get("expiry_date", "")),
            "option_expiry_source": row.get("option_expiry_source", ""),
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
            "quantity": row.get("quantity", default_quantity_for_symbol(symbol)),
            "reason": f"SL:{row.get('stop_loss', '')} TSL:{row.get('trailing_stop_loss', '')} TP:{row.get('target_price', '')}",
        }, default_status=TRADE_STATUS_NEW))
    return candidates


def build_analysis_queue(candidates: list[dict[str, object]], analyzed_at_utc: Optional[str] = None) -> list[dict[str, object]]:
    actionable = [c for c in candidates if _normalize_text(c.get("side")) in {"BUY", "SELL"}]
    if not actionable:
        return []
    stamp = analyzed_at_utc or datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    analyzed: list[dict[str, object]] = []
    for candidate in actionable:
        row = _ensure_trade_identity(candidate, default_status=TRADE_STATUS_REVIEWED)
        row["analysis_status"] = "ANALYZED"
        row["analyzed_at_utc"] = stamp
        row["reviewed_at_utc"] = stamp
        row["execution_ready"] = "YES"
        row["trade_status"] = TRADE_STATUS_REVIEWED
        analyzed.append(row)
    return analyzed


def _read_trade_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [dict(row) for row in read_csv_rows(path)]


def _row_is_closed(row: dict[str, object]) -> bool:
    return _normalize_text(row.get("trade_status")) == TRADE_STATUS_CLOSED or _normalize_text(row.get("position_status")) == TRADE_STATUS_CLOSED or _normalize_text(row.get("execution_status")) in {"CLOSED", "EXITED"}


def _row_is_active(row: dict[str, object]) -> bool:
    if _row_is_closed(row):
        return False
    if _normalize_text(row.get("trade_status")) in ACTIVE_TRADE_STATUSES:
        return True
    return _normalize_text(row.get("execution_status")) in {"EXECUTED", "SENT", "FILLED"}


def load_active_trade_keys(path: Path, execution_type: str | None = None) -> set[str]:
    keys: set[str] = set()
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if not _row_is_active(row):
            continue
        keys.add(str(row.get("trade_key", "") or make_trade_key(row)))
    return keys


def _load_historical_trade_ids(path: Path, execution_type: str | None = None) -> set[str]:
    ids: set[str] = set()
    for row in _read_trade_rows(path):
        if execution_type and _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        ids.add(str(row.get("trade_id", "") or make_trade_id(row)))
    return ids


def filter_unlogged_candidates(candidates: list[dict[str, object]], output_path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    active_keys = load_active_trade_keys(output_path)
    fresh: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    for candidate in candidates:
        row = _ensure_trade_identity(candidate)
        if str(row.get("trade_key", "")) in active_keys:
            flagged = dict(row)
            flagged["duplicate_reason"] = SKIP_REASON_DUPLICATE_ACTIVE_TRADE
            skipped.append(flagged)
        else:
            fresh.append(row)
    return fresh, skipped


def _trade_day_key(record: dict[str, object], fallback_day: str) -> str:
    ts = _parse_dt(record.get("signal_time")) or _parse_dt(record.get("entry_time")) or _parse_dt(record.get("timestamp"))
    if ts is None:
        return fallback_day
    return ts.strftime("%Y-%m-%d")


def _load_daily_execution_state(path: Path, execution_type: str) -> dict[str, dict[str, float]]:
    state: dict[str, dict[str, float]] = {}
    for row in _read_trade_rows(path):
        if _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
            continue
        if _row_is_closed(row):
            continue
        if _normalize_text(row.get("execution_status")) not in EXECUTION_SUCCESS_STATUSES and _normalize_text(row.get("trade_status")) not in {TRADE_STATUS_EXECUTED, TRADE_STATUS_OPEN, TRADE_STATUS_PENDING_EXECUTION}:
            continue
        day_key = _trade_day_key(row, str(row.get("executed_at_utc", ""))[:10])
        bucket = state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        bucket["count"] += 1.0
        bucket["realized_pnl"] += _safe_float(row.get("pnl"))
    return state


def _risk_limit_message(max_trades_per_day: int | None, max_daily_loss: float | None) -> str:
    parts: list[str] = []
    if max_trades_per_day is not None and int(max_trades_per_day) > 0:
        parts.append(f"max trades/day={int(max_trades_per_day)}")
    if max_daily_loss is not None and float(max_daily_loss) > 0:
        parts.append(f"max daily loss={float(max_daily_loss):.2f}")
    return ", ".join(parts) if parts else "risk limit"


def live_kill_switch_enabled() -> bool:
    raw = str(os.getenv("LIVE_TRADING_KILL_SWITCH", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


def _default_live_broker_client() -> tuple[object | None, str]:
    broker_name = str(os.getenv("LIVE_BROKER", "DHAN") or "DHAN").strip().upper()
    if broker_name != "DHAN" or DhanClient is None:
        return None, broker_name
    try:
        client = DhanClient.from_env()
    except Exception:
        client = None
    return client, broker_name


def _default_security_map() -> dict[str, dict[str, str]]:
    if load_security_map is None:
        return {}
    raw_path = str(os.getenv("DHAN_SECURITY_MAP", "data/dhan_security_map.csv") or "data/dhan_security_map.csv").strip()
    try:
        return load_security_map(Path(raw_path))
    except Exception:
        return {}


def _apply_live_broker_result(record: dict[str, object], broker_name: str, result: object) -> None:
    record["broker_name"] = broker_name
    if isinstance(result, dict):
        record["broker_order_id"] = result.get("orderId", result.get("order_id", ""))
        record["broker_status"] = result.get("orderStatus", result.get("status", "SENT"))
        record["broker_message"] = result.get("message", result.get("remarks", result.get("omsErrorDescription", "")))
        try:
            record["broker_response_json"] = json.dumps(result, ensure_ascii=True)
        except Exception:
            record["broker_response_json"] = str(result)
    else:
        record["broker_status"] = "SENT"
        record["broker_message"] = ""


def _first_execution_time(path: Path, execution_type: str) -> Optional[datetime]:
    earliest: Optional[datetime] = None
    for row in _read_trade_rows(path):
        if _normalize_text(row.get("execution_type")) != _normalize_text(execution_type):
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


def live_trading_unlock_status(paper_log_path: Path, min_days: int = 30, now_utc: Optional[datetime] = None) -> Tuple[bool, int, str]:
    start = _first_execution_time(paper_log_path, "PAPER")
    if start is None:
        return False, 0, ""
    now = now_utc or datetime.now(UTC)
    days = max(0, (now - start).days)
    unlock_date = start.replace(microsecond=0) + timedelta(days=min_days)
    return days >= min_days, days, unlock_date.strftime("%Y-%m-%d %H:%M:%S UTC")


def validate_candidate(candidate: dict[str, object]) -> tuple[bool, str, dict[str, object]]:
    record = _ensure_trade_identity(candidate)
    side = _normalize_text(record.get("side"))
    if side not in {"BUY", "SELL"}:
        record["validation_error"] = SKIP_REASON_INVALID_SIDE
        return False, SKIP_REASON_INVALID_SIDE, record
    price = _price_value(record)
    if price <= 0:
        record["validation_error"] = SKIP_REASON_MISSING_PRICE
        return False, SKIP_REASON_MISSING_PRICE, record
    record["price"] = price
    record.setdefault("share_price", price)
    record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
    raw_quantity = record.get("quantity")
    if raw_quantity is None or str(raw_quantity).strip() == "":
        record["validation_error"] = SKIP_REASON_MISSING_QUANTITY
        return False, SKIP_REASON_MISSING_QUANTITY, record
    quantity = normalize_order_quantity(str(record.get("symbol", "")), raw_quantity)
    if quantity <= 0:
        record["validation_error"] = SKIP_REASON_MISSING_QUANTITY
        return False, SKIP_REASON_MISSING_QUANTITY, record
    record["quantity"] = quantity
    return True, "", record


def _make_result_row(candidate: dict[str, object], *, execution_type: str, processed_at_utc: str) -> dict[str, object]:
    row = _ensure_trade_identity(candidate)
    row.setdefault("execution_type", execution_type)
    row.setdefault("executed_at_utc", processed_at_utc)
    row.setdefault("position_status", "")
    return row


def _mark_skipped(result: ExecutionResult, row: dict[str, object], reason: str) -> None:
    skipped = dict(row)
    if reason.startswith("DUPLICATE_"):
        skipped["duplicate_reason"] = reason
        result.duplicate_count += 1
    else:
        skipped["validation_error"] = reason
    result.skipped_rows.append(skipped)
    result.skipped_count += 1


def _append_written_row(result: ExecutionResult, row: dict[str, object]) -> None:
    result.rows.append(row)
    execution_status = _normalize_text(row.get("execution_status"))
    if execution_status in {"EXECUTED", "SENT", "FILLED"}:
        result.executed_rows.append(row)
        result.executed_count += 1
    elif execution_status == "BLOCKED":
        result.blocked_rows.append(row)
        result.blocked_count += 1
    elif execution_status == "ERROR":
        result.error_rows.append(row)
        result.error_count += 1


def _stable_fieldnames(rows: list[dict[str, object]], existing_rows: list[dict[str, object]] | None = None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for key in EXECUTION_SCHEMA:
        if key not in seen:
            ordered.append(key)
            seen.add(key)
    for source in (existing_rows or []) + rows:
        for key in source.keys():
            if key not in seen:
                ordered.append(key)
                seen.add(key)
    return ordered


def _write_execution_rows(path: Path, rows_to_write: list[dict[str, object]]) -> None:
    if not rows_to_write:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows = _read_trade_rows(path)
    fieldnames = _stable_fieldnames(rows_to_write, existing_rows)
    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)


def execution_result_summary(result: ExecutionResult) -> list[tuple[str, str]]:
    messages: list[tuple[str, str]] = []
    if result.executed_count:
        label = "trade" if result.executed_count == 1 else "trades"
        messages.append(("success", f"{result.executed_count} {label} executed"))
    if result.blocked_count:
        reason_counts: dict[str, int] = {}
        for row in result.blocked_rows:
            reason = str(row.get("blocked_reason") or row.get("risk_limit_reason") or "BLOCKED").strip()
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in reason_counts.items():
            label = "trade" if count == 1 else "trades"
            messages.append(("warning", f"{count} {label} blocked by {reason.replace('_', ' ').lower()}"))
    if result.skipped_count:
        reason_counts: dict[str, int] = {}
        for row in result.skipped_rows:
            reason = str(row.get("duplicate_reason") or row.get("validation_error") or "SKIPPED").strip()
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in reason_counts.items():
            label = "trade" if count == 1 else "trades"
            messages.append(("info", f"{count} {label} skipped because {reason.replace('_', ' ').lower()}"))
    if result.error_count:
        label = "trade" if result.error_count == 1 else "trades"
        messages.append(("error", f"{result.error_count} {label} failed with broker or execution errors"))
    return messages


def _execute_candidates(candidates: list[dict[str, object]], output_path: Path, *, execution_type: str, deduplicate: bool, max_trades_per_day: int | None, max_daily_loss: float | None, broker_client: object | None = None, broker_name: str | None = None, security_map: dict[str, dict[str, str]] | None = None) -> ExecutionResult:
    result = ExecutionResult()
    if not candidates:
        return result
    output_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    today_key = now[:10]
    historical_trade_ids = _load_historical_trade_ids(output_path, execution_type)
    active_trade_keys = load_active_trade_keys(output_path, execution_type)
    daily_state = _load_daily_execution_state(output_path, execution_type)
    rows_to_write: list[dict[str, object]] = []

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    resolved_security_map = security_map if security_map is not None else {}
    if execution_type == "LIVE":
        if resolved_broker_client is None:
            resolved_broker_client, inferred_name = _default_live_broker_client()
            if not resolved_broker_name:
                resolved_broker_name = inferred_name
        if not resolved_broker_name:
            resolved_broker_name = "LIVE"
        if not resolved_security_map:
            resolved_security_map = _default_security_map()

    for candidate in candidates:
        ok, validation_reason, validated = validate_candidate(candidate)
        base_row = _make_result_row(validated, execution_type=execution_type, processed_at_utc=now)
        trade_id = str(base_row.get("trade_id", ""))
        trade_key = str(base_row.get("trade_key", ""))
        if not ok:
            _mark_skipped(result, base_row, validation_reason)
            continue
        if deduplicate and trade_id in historical_trade_ids:
            _mark_skipped(result, base_row, SKIP_REASON_DUPLICATE_EXECUTED_TRADE)
            continue
        if deduplicate and trade_key in active_trade_keys:
            _mark_skipped(result, base_row, SKIP_REASON_DUPLICATE_ACTIVE_TRADE)
            continue

        day_key = _trade_day_key(base_row, today_key)
        state = daily_state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        trade_limit_hit = max_trades_per_day is not None and int(max_trades_per_day) > 0 and int(state["count"]) >= int(max_trades_per_day)
        loss_limit_hit = max_daily_loss is not None and float(max_daily_loss) > 0 and float(state["realized_pnl"]) <= -abs(float(max_daily_loss))
        if trade_limit_hit or loss_limit_hit:
            blocked = dict(base_row)
            blocked["trade_status"] = TRADE_STATUS_BLOCKED
            blocked["execution_status"] = "BLOCKED"
            blocked["blocked_reason"] = SKIP_REASON_MAX_TRADES_PER_DAY if trade_limit_hit else SKIP_REASON_MAX_DAILY_LOSS
            blocked["risk_limit_reason"] = _risk_limit_message(max_trades_per_day, max_daily_loss)
            if execution_type == "LIVE":
                blocked["broker_name"] = resolved_broker_name
                blocked["broker_status"] = "RISK_LIMIT"
                blocked["broker_message"] = f"Live execution blocked by {_risk_limit_message(max_trades_per_day, max_daily_loss)}"
            rows_to_write.append(blocked)
            _append_written_row(result, blocked)
            continue

        if execution_type == "PAPER":
            executed = dict(base_row)
            executed["trade_status"] = TRADE_STATUS_EXECUTED
            executed["position_status"] = TRADE_STATUS_OPEN
            executed["execution_status"] = "EXECUTED"
            rows_to_write.append(executed)
            _append_written_row(result, executed)
            state["count"] += 1.0
            state["realized_pnl"] += _safe_float(executed.get("pnl"))
            historical_trade_ids.add(trade_id)
            active_trade_keys.add(trade_key)
            continue

        live_row = dict(base_row)
        if live_kill_switch_enabled():
            live_row["trade_status"] = TRADE_STATUS_BLOCKED
            live_row["execution_status"] = "BLOCKED"
            live_row["blocked_reason"] = SKIP_REASON_KILL_SWITCH
            live_row["broker_name"] = resolved_broker_name
            live_row["broker_status"] = "KILL_SWITCH"
            live_row["broker_message"] = "Live execution blocked by LIVE_TRADING_KILL_SWITCH."
            rows_to_write.append(live_row)
            _append_written_row(result, live_row)
            continue

        live_row.setdefault("data_symbol", live_row.get("symbol", ""))
        live_row.setdefault("trade_symbol", live_row.get("trading_symbol", live_row.get("symbol", "")))
        if resolved_broker_client is None:
            live_row["trade_status"] = TRADE_STATUS_ERROR
            live_row["execution_status"] = "ERROR"
            live_row["validation_error"] = "BROKER_CLIENT_NOT_CONFIGURED"
            live_row["broker_name"] = resolved_broker_name
            live_row["broker_status"] = "NOT_CONFIGURED"
            live_row["broker_message"] = "Live broker client not configured; row logged only."
        else:
            try:
                if resolve_security is None or build_order_request_from_candidate is None:
                    raise RuntimeError("Broker payload builder unavailable")
                resolution = resolve_security(
                    live_row,
                    resolved_security_map,
                    broker_client=resolved_broker_client,
                    validate_with_option_chain=True,
                )
                for key in (
                    "data_symbol",
                    "trade_symbol",
                    "trading_symbol",
                    "security_id",
                    "exchange_segment",
                    "instrument_type",
                    "option_expiry",
                    "option_type",
                    "strike_price",
                ):
                    value = resolution.get(key)
                    if value not in {None, ""}:
                        live_row[key] = value
                client_id = getattr(resolved_broker_client, "client_id", "")
                order_request = build_order_request_from_candidate(
                    live_row,
                    client_id=str(client_id),
                    security_map=resolved_security_map,
                    resolved_security=resolution,
                    broker_client=resolved_broker_client,
                )
                broker_result = resolved_broker_client.place_order(order_request)
                _apply_live_broker_result(live_row, resolved_broker_name, broker_result)
                if _normalize_text(live_row.get("broker_status", "SENT")) in {"REJECTED", "FAILED", "ERROR"}:
                    live_row["trade_status"] = TRADE_STATUS_ERROR
                    live_row["execution_status"] = "ERROR"
                    live_row["validation_error"] = SKIP_REASON_BROKER_ERROR
                else:
                    live_row["trade_status"] = TRADE_STATUS_PENDING_EXECUTION
                    live_row["execution_status"] = "SENT"
            except DhanExecutionError as exc:
                live_row["trade_status"] = TRADE_STATUS_ERROR
                live_row["execution_status"] = "ERROR"
                live_row["validation_error"] = getattr(exc, "code", SKIP_REASON_BROKER_ERROR)
                live_row["broker_name"] = resolved_broker_name
                live_row["broker_status"] = getattr(exc, "code", "ERROR")
                live_row["broker_message"] = str(exc)
                metadata = getattr(exc, "metadata", {})
                if isinstance(metadata, dict):
                    for key, value in metadata.items():
                        if value not in {None, ""}:
                            live_row[key] = value
            except Exception as exc:
                live_row["trade_status"] = TRADE_STATUS_ERROR
                live_row["execution_status"] = "ERROR"
                live_row["validation_error"] = SKIP_REASON_BROKER_ERROR
                live_row["broker_name"] = resolved_broker_name
                live_row["broker_status"] = "ERROR"
                live_row["broker_message"] = str(exc)

        rows_to_write.append(live_row)
        _append_written_row(result, live_row)
        if _normalize_text(live_row.get("execution_status")) == "SENT":
            state["count"] += 1.0
            state["realized_pnl"] += _safe_float(live_row.get("pnl"))
            historical_trade_ids.add(trade_id)
            active_trade_keys.add(trade_key)

    _write_execution_rows(output_path, rows_to_write)
    return result


def execute_paper_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True, *, max_trades_per_day: int | None = None, max_daily_loss: float | None = None) -> ExecutionResult:
    return _execute_candidates(candidates, output_path, execution_type="PAPER", deduplicate=deduplicate, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss)


def execute_live_trades(candidates: list[dict[str, object]], output_path: Path, deduplicate: bool = True, *, broker_client: object | None = None, broker_name: str | None = None, security_map: dict[str, dict[str, str]] | None = None, max_trades_per_day: int | None = None, max_daily_loss: float | None = None) -> ExecutionResult:
    return _execute_candidates(candidates, output_path, execution_type="LIVE", deduplicate=deduplicate, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss, broker_client=broker_client, broker_name=broker_name, security_map=security_map)


def load_open_trades(log_path: Path, execution_type: str | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for raw in _read_trade_rows(log_path):
        if execution_type and _normalize_text(raw.get("execution_type")) != _normalize_text(execution_type):
            continue
        if not _row_is_active(raw):
            continue
        rows.append(_ensure_trade_identity(raw))
    return rows


def manual_close_paper_trades(
    paper_log_path: Path,
    trade_ids: list[str],
    *,
    exit_price: float | None = None,
    exit_reason: str = "MANUAL_EXIT",
    exited_at_utc: str | None = None,
) -> list[dict[str, object]]:
    if not paper_log_path.exists() or not trade_ids:
        return []

    target_ids = {str(trade_id or "").strip() for trade_id in trade_ids if str(trade_id or "").strip()}
    if not target_ids:
        return []

    existing_rows = _read_trade_rows(paper_log_path)
    updated_rows: list[dict[str, object]] = []
    closed_rows: list[dict[str, object]] = []
    closed_at = exited_at_utc or datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    for raw in existing_rows:
        row = _ensure_trade_identity(raw)
        if _normalize_text(row.get("execution_type")) != "PAPER" or _row_is_closed(row):
            updated_rows.append(row)
            continue

        if str(row.get("trade_id", "")) not in target_ids:
            updated_rows.append(row)
            continue

        side = _normalize_text(row.get("side"))
        price = float(exit_price) if exit_price is not None and float(exit_price) > 0 else _price_value(row)
        entry_price = _price_value(row)
        qty = int(_safe_float(row.get("quantity")))
        pnl = 0.0
        if qty > 0 and price > 0 and side in {"BUY", "SELL"}:
            pnl = (price - entry_price) * qty if side == "BUY" else (entry_price - price) * qty

        closed = dict(row)
        closed["execution_status"] = "CLOSED"
        closed["trade_status"] = TRADE_STATUS_CLOSED
        closed["position_status"] = TRADE_STATUS_CLOSED
        closed["exit_time"] = closed_at
        closed["exit_price"] = round(price, 4) if price > 0 else ""
        closed["exit_reason"] = exit_reason
        closed["pnl"] = round(float(pnl), 2)
        updated_rows.append(closed)
        closed_rows.append(closed)

    if not closed_rows:
        return []

    fieldnames = _stable_fieldnames(updated_rows)
    with paper_log_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return closed_rows
def _reconcile_execution_status(broker_status: str) -> str:
    normalized = str(broker_status or "").strip().upper()
    if normalized in {"TRADED", "FILLED", "COMPLETED", "EXECUTED", "SUCCESS"}:
        return "FILLED"
    if normalized in {"REJECTED", "FAILED", "ERROR", "CANCELLED", "CANCELED", "EXPIRED"}:
        return "ERROR"
    return "SENT"


def _symbol_key(record: dict[str, object]) -> str:
    for key in ("trading_symbol", "tradingSymbol", "contract_symbol", "contractSymbol", "option_strike", "optionStrike", "symbol"):
        raw = str(record.get(key, "") or "").strip().upper()
        if raw:
            return raw
    return "UNKNOWN"


def _signed_quantity(side: object, quantity: object) -> int:
    qty = int(_safe_float(quantity))
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        return -qty
    return qty


def _broker_position_map(positions: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    mapped: dict[str, dict[str, object]] = {}
    for raw in positions:
        record = dict(raw)
        symbol = _symbol_key(record)
        if symbol == "UNKNOWN":
            continue
        net_qty = int(_safe_float(record.get("netQty") or record.get("net_qty") or record.get("quantity") or record.get("qty") or 0))
        record["net_qty"] = net_qty
        mapped[symbol] = record
    return mapped


def reconcile_live_trades(
    live_log_path: Path,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
) -> list[dict[str, object]]:
    if not live_log_path.exists():
        return []

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    if resolved_broker_client is None:
        resolved_broker_client, inferred_name = _default_live_broker_client()
        if not resolved_broker_name:
            resolved_broker_name = inferred_name
    if not resolved_broker_name:
        resolved_broker_name = "LIVE"

    existing_rows = read_csv_rows(live_log_path)
    if not existing_rows:
        return []

    reconciled_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    updated_rows: list[dict[str, object]] = []
    reconciled_rows: list[dict[str, object]] = []

    for row in existing_rows:
        record = dict(row)
        if str(record.get("execution_type", "")).upper() != "LIVE":
            updated_rows.append(record)
            continue

        current_status = str(record.get("execution_status", "")).upper()
        if current_status in {"BLOCKED", "ERROR", "FILLED"}:
            updated_rows.append(record)
            continue

        order_id = str(record.get("broker_order_id", "") or "").strip()
        if not order_id:
            record["reconciliation_status"] = "SKIPPED"
            record["reconciliation_note"] = "Missing broker_order_id"
            record["reconciled_at_utc"] = reconciled_at
            updated_rows.append(record)
            continue

        if resolved_broker_client is None or not hasattr(resolved_broker_client, "get_order_by_id"):
            record["reconciliation_status"] = "UNAVAILABLE"
            record["reconciliation_note"] = "Broker client does not support order reconciliation"
            record["reconciled_at_utc"] = reconciled_at
            updated_rows.append(record)
            continue

        try:
            result = resolved_broker_client.get_order_by_id(order_id)
            _apply_live_broker_result(record, resolved_broker_name, result)
            record["execution_status"] = _reconcile_execution_status(str(record.get("broker_status", "")))
            record["trade_status"] = TRADE_STATUS_OPEN if str(record.get("execution_status", "")).upper() == "FILLED" else TRADE_STATUS_ERROR
            record["position_status"] = TRADE_STATUS_OPEN if str(record.get("execution_status", "")).upper() == "FILLED" else record.get("position_status", "")
            record["reconciliation_status"] = "RECONCILED"
            record["reconciled_at_utc"] = reconciled_at
            reconciled_rows.append(record)
        except Exception as exc:
            record["reconciliation_status"] = "ERROR"
            record["reconciliation_note"] = str(exc)
            record["reconciled_at_utc"] = reconciled_at

        updated_rows.append(record)

    fieldnames: list[str] = []
    for r in updated_rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    with live_log_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return reconciled_rows


def reconcile_live_positions(
    live_log_path: Path,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
) -> list[dict[str, object]]:
    if not live_log_path.exists():
        return []

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    if resolved_broker_client is None:
        resolved_broker_client, inferred_name = _default_live_broker_client()
        if not resolved_broker_name:
            resolved_broker_name = inferred_name
    if not resolved_broker_name:
        resolved_broker_name = "LIVE"

    if resolved_broker_client is None or not hasattr(resolved_broker_client, "get_positions"):
        return []

    existing_rows = read_csv_rows(live_log_path)
    if not existing_rows:
        return []

    expected: dict[str, int] = {}
    for row in existing_rows:
        if str(row.get("execution_type", "")).upper() != "LIVE":
            continue
        if str(row.get("execution_status", "")).upper() != "FILLED":
            continue
        symbol = _symbol_key(row)
        expected[symbol] = expected.get(symbol, 0) + _signed_quantity(row.get("side"), row.get("quantity"))

    positions_raw = resolved_broker_client.get_positions()
    actual_map = _broker_position_map(positions_raw if isinstance(positions_raw, list) else [])
    reconciled_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    position_rows: list[dict[str, object]] = []
    all_symbols = sorted(set(expected.keys()) | set(actual_map.keys()))
    for symbol in all_symbols:
        expected_qty = int(expected.get(symbol, 0))
        actual_qty = int(_safe_float(actual_map.get(symbol, {}).get("net_qty", 0)))
        delta = actual_qty - expected_qty
        position_rows.append(
            {
                "symbol": symbol,
                "broker_name": resolved_broker_name,
                "expected_net_qty": expected_qty,
                "broker_net_qty": actual_qty,
                "qty_delta": delta,
                "position_match": "YES" if delta == 0 else "NO",
                "reconciled_at_utc": reconciled_at,
            }
        )

    return position_rows

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
        r2["trade_status"] = TRADE_STATUS_CLOSED
        r2["position_status"] = TRADE_STATUS_CLOSED
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











def _order_update_value(event: object, key: str, default: object = '') -> object:
    if isinstance(event, dict):
        return event.get(key, default)
    return getattr(event, key, default)


def _apply_live_order_update_row(record: dict[str, object], event: object) -> dict[str, object]:
    updated = dict(record)
    status = _normalize_text(_order_update_value(event, 'status'))
    filled_qty = int(_safe_float(_order_update_value(event, 'filled_qty', 0)))
    remaining_qty = int(_safe_float(_order_update_value(event, 'remaining_qty', 0)))
    average_price = _safe_float(_order_update_value(event, 'average_price', 0.0))
    traded_price = _safe_float(_order_update_value(event, 'traded_price', 0.0))
    update_time = str(_order_update_value(event, 'update_time', '') or '')
    message = str(_order_update_value(event, 'message', '') or '')
    order_id = str(_order_update_value(event, 'order_id', '') or '')
    correlation_id = str(_order_update_value(event, 'correlation_id', '') or '')

    if order_id:
        updated['broker_order_id'] = order_id
    if correlation_id:
        updated['correlation_id'] = correlation_id
    if status:
        updated['broker_status'] = status
    if message:
        updated['broker_message'] = message
    if update_time:
        updated['reconciled_at_utc'] = update_time
    if average_price > 0:
        updated['average_price'] = round(average_price, 4)
        updated['price'] = round(average_price, 4)
    if traded_price > 0:
        updated['traded_price'] = round(traded_price, 4)
    updated['filled_qty'] = filled_qty
    updated['remaining_qty'] = remaining_qty

    if status in {'TRADED', 'FILLED', 'COMPLETED', 'EXECUTED', 'SUCCESS'} or (filled_qty > 0 and remaining_qty == 0):
        updated['execution_status'] = 'FILLED'
        updated['trade_status'] = TRADE_STATUS_OPEN
        updated['position_status'] = TRADE_STATUS_OPEN
    elif status in {'PARTIAL', 'PARTIALLY_FILLED', 'PARTTRADED'} or (filled_qty > 0 and remaining_qty > 0):
        updated['execution_status'] = 'PARTIAL'
        updated['trade_status'] = TRADE_STATUS_PENDING_EXECUTION
        updated['position_status'] = updated.get('position_status', '') or ''
    elif status in {'REJECTED', 'FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'EXPIRED'}:
        updated['execution_status'] = 'ERROR'
        updated['trade_status'] = TRADE_STATUS_ERROR
    else:
        updated['execution_status'] = updated.get('execution_status', 'SENT')
        if not str(updated.get('trade_status', '')).strip():
            updated['trade_status'] = TRADE_STATUS_PENDING_EXECUTION

    return updated


def apply_live_order_updates_to_log(live_log_path: str | Path, order_updates: list[object]) -> list[dict[str, object]]:
    path = Path(live_log_path)
    if not path.exists() or not order_updates:
        return []

    existing_rows = _read_trade_rows(path)
    if not existing_rows:
        return []

    updated_rows: list[dict[str, object]] = []
    changed_rows: list[dict[str, object]] = []

    for raw in existing_rows:
        record = dict(raw)
        if _normalize_text(record.get('execution_type')) != 'LIVE':
            updated_rows.append(record)
            continue

        matched = record
        applied = False
        for event in order_updates:
            event_order_id = str(_order_update_value(event, 'order_id', '') or '').strip()
            event_trade_id = str(_order_update_value(event, 'trade_id', '') or '').strip()
            event_correlation_id = str(_order_update_value(event, 'correlation_id', '') or '').strip()
            if event_order_id and event_order_id == str(record.get('broker_order_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break
            if event_trade_id and event_trade_id == str(record.get('trade_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break
            if event_correlation_id and event_correlation_id == str(record.get('correlation_id', '') or '').strip():
                matched = _apply_live_order_update_row(record, event)
                applied = True
                break

        updated_rows.append(matched)
        if applied:
            changed_rows.append(matched)

    if not changed_rows:
        return []

    fieldnames = _stable_fieldnames(updated_rows)
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    return changed_rows
