from __future__ import annotations

import csv
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from src.csv_io import read_csv_rows
try:
    from src.dhan_api import DhanClient, build_order_request_from_candidate, load_security_map
except Exception:
    DhanClient = None  # type: ignore
    build_order_request_from_candidate = None  # type: ignore
    load_security_map = None  # type: ignore
DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
    "NIFTY FUT": 65,
    "NIFTY FUTURES": 65,
    "NIFTYFUT": 65,
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
        keys.add(execution_candidate_key(row))
    return keys


def execution_candidate_key(record: dict[str, object]) -> str:
    return f"{record.get('strategy','')}|{record.get('symbol','')}|{record.get('signal_time','')}|{record.get('side','')}"


def filter_unlogged_candidates(
    candidates: list[dict[str, object]],
    output_path: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    seen = _existing_keys(output_path)
    fresh: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    for candidate in candidates:
        if execution_candidate_key(candidate) in seen:
            skipped.append(candidate)
        else:
            fresh.append(candidate)
    return fresh, skipped


def _trade_day_key(record: dict[str, object], fallback_day: str) -> str:
    ts = _parse_dt(record.get("signal_time")) or _parse_dt(record.get("entry_time")) or _parse_dt(record.get("timestamp"))
    if ts is None:
        return fallback_day
    return ts.strftime("%Y-%m-%d")


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == "":
            return 0.0
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _load_daily_execution_state(path: Path, execution_type: str) -> dict[str, dict[str, float]]:
    state: dict[str, dict[str, float]] = {}
    if not path.exists():
        return state

    for row in read_csv_rows(path):
        if str(row.get("execution_type", "")).upper() != execution_type.upper():
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
        record["broker_message"] = result.get("message", result.get("remarks", ""))
        try:
            record["broker_response_json"] = json.dumps(result, ensure_ascii=True)
        except Exception:
            record["broker_response_json"] = str(result)
    else:
        record["broker_status"] = "SENT"
        record["broker_message"] = ""

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


def execute_paper_trades(
    candidates: list[dict[str, object]],
    output_path: Path,
    deduplicate: bool = True,
    *,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
) -> list[dict[str, object]]:
    executable = [c for c in candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"}]
    if not executable:
        return []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_write: list[dict[str, object]] = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    today_key = now[:10]
    seen = _existing_keys(output_path) if deduplicate else set()
    daily_state = _load_daily_execution_state(output_path, "PAPER")

    for c in executable:
        key = execution_candidate_key(c)
        if deduplicate and key in seen:
            continue

        record = dict(c)
        record.setdefault("share_price", record.get("price", ""))
        record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
        record["quantity"] = normalize_order_quantity(str(record.get("symbol", "")), record.get("quantity"))
        record["execution_type"] = "PAPER"
        record["executed_at_utc"] = now

        day_key = _trade_day_key(record, today_key)
        state = daily_state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        trade_limit_hit = max_trades_per_day is not None and int(max_trades_per_day) > 0 and int(state["count"]) >= int(max_trades_per_day)
        loss_limit_hit = max_daily_loss is not None and float(max_daily_loss) > 0 and float(state["realized_pnl"]) <= -abs(float(max_daily_loss))
        if trade_limit_hit or loss_limit_hit:
            record["execution_status"] = "BLOCKED"
            record["risk_limit_reason"] = _risk_limit_message(max_trades_per_day, max_daily_loss)
            rows_to_write.append(record)
            continue

        record["execution_status"] = "EXECUTED"
        rows_to_write.append(record)
        state["count"] += 1.0
        state["realized_pnl"] += _safe_float(record.get("pnl"))
        seen.add(key)

    if not rows_to_write:
        return []

    fieldnames: list[str] = []
    for row in rows_to_write:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)

    return rows_to_write


def execute_live_trades(
    candidates: list[dict[str, object]],
    output_path: Path,
    deduplicate: bool = True,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
) -> list[dict[str, object]]:
    executable = [c for c in candidates if str(c.get("side", "")).upper() in {"BUY", "SELL"}]
    if not executable:
        return []

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_to_write: list[dict[str, object]] = []
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    today_key = now[:10]
    seen = _existing_keys(output_path) if deduplicate else set()
    daily_state = _load_daily_execution_state(output_path, "LIVE")

    resolved_broker_client = broker_client
    resolved_broker_name = str(broker_name or "").strip().upper()
    if resolved_broker_client is None:
        resolved_broker_client, inferred_name = _default_live_broker_client()
        if not resolved_broker_name:
            resolved_broker_name = inferred_name
    if not resolved_broker_name:
        resolved_broker_name = "LIVE"

    resolved_security_map = security_map if security_map is not None else _default_security_map()

    for c in executable:
        key = execution_candidate_key(c)
        if deduplicate and key in seen:
            continue

        record = dict(c)
        record.setdefault("share_price", record.get("price", ""))
        record.setdefault("strike_price", record.get("option_strike", record.get("strike", "")))
        record["quantity"] = normalize_order_quantity(str(record.get("symbol", "")), record.get("quantity"))
        record["execution_type"] = "LIVE"
        record["executed_at_utc"] = now

        day_key = _trade_day_key(record, today_key)
        state = daily_state.setdefault(day_key, {"count": 0.0, "realized_pnl": 0.0})
        if live_kill_switch_enabled():
            record["execution_status"] = "BLOCKED"
            record["broker_name"] = resolved_broker_name
            record["broker_status"] = "KILL_SWITCH"
            record["broker_message"] = "Live execution blocked by LIVE_TRADING_KILL_SWITCH."
            rows_to_write.append(record)
            continue

        trade_limit_hit = max_trades_per_day is not None and int(max_trades_per_day) > 0 and int(state["count"]) >= int(max_trades_per_day)
        loss_limit_hit = max_daily_loss is not None and float(max_daily_loss) > 0 and float(state["realized_pnl"]) <= -abs(float(max_daily_loss))
        if trade_limit_hit or loss_limit_hit:
            record["execution_status"] = "BLOCKED"
            record["broker_name"] = resolved_broker_name
            record["broker_status"] = "RISK_LIMIT"
            record["broker_message"] = f"Live execution blocked by {_risk_limit_message(max_trades_per_day, max_daily_loss)}"
            rows_to_write.append(record)
            continue

        if resolved_broker_client is None:
            record["execution_status"] = "SENT"
            record["broker_name"] = resolved_broker_name
            record["broker_status"] = "NOT_CONFIGURED"
            record["broker_message"] = "Live broker client not configured; row logged only."
        else:
            try:
                if build_order_request_from_candidate is None:
                    raise RuntimeError("Broker payload builder unavailable")
                client_id = getattr(resolved_broker_client, "client_id", "")
                order_request = build_order_request_from_candidate(
                    record,
                    client_id=str(client_id),
                    security_map=resolved_security_map,
                )
                result = resolved_broker_client.place_order(order_request)
                _apply_live_broker_result(record, resolved_broker_name, result)
                broker_status = str(record.get("broker_status", "SENT") or "SENT").upper()
                record["execution_status"] = "SENT" if broker_status not in {"REJECTED", "FAILED", "ERROR"} else "ERROR"
            except Exception as exc:
                record["execution_status"] = "ERROR"
                record["broker_name"] = resolved_broker_name
                record["broker_status"] = "ERROR"
                record["broker_message"] = str(exc)

        rows_to_write.append(record)
        if str(record.get("execution_status", "")).upper() != "BLOCKED":
            state["count"] += 1.0
            state["realized_pnl"] += _safe_float(record.get("pnl"))
            seen.add(key)

    if not rows_to_write:
        return []

    fieldnames: list[str] = []
    for row in rows_to_write:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    file_exists = output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows_to_write)

    return rows_to_write


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


