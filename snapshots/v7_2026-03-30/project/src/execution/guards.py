from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class ExecutionGuardConfig:
    cooldown_minutes: int = 15
    max_trades_per_day: int = 3
    max_daily_loss: float = 0.0
    session_start: str = "09:15"
    session_end: str = "15:30"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_time(value: str) -> time:
    return datetime.strptime(value, "%H:%M").time()


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).to_pydatetime()


def normalize_trade_schema(candidate: dict[str, Any]) -> dict[str, Any]:
    side = str(candidate.get("side", candidate.get("type", "")) or "").upper()
    timestamp = str(candidate.get("timestamp", candidate.get("signal_time", candidate.get("entry_time", ""))) or "")
    normalized = dict(candidate)
    normalized["symbol"] = str(candidate.get("symbol", "UNKNOWN") or "UNKNOWN")
    normalized["side"] = side
    normalized["timestamp"] = timestamp
    normalized["signal_time"] = str(candidate.get("signal_time", timestamp) or timestamp)
    normalized["entry"] = _safe_float(candidate.get("entry", candidate.get("entry_price", candidate.get("price", 0.0))))
    normalized["entry_price"] = normalized["entry"]
    normalized["stoploss"] = _safe_float(candidate.get("stoploss", candidate.get("stop_loss", candidate.get("sl", 0.0))))
    normalized["stop_loss"] = normalized["stoploss"]
    normalized["target"] = _safe_float(candidate.get("target", candidate.get("target_price", candidate.get("tp", 0.0))))
    normalized["target_price"] = normalized["target"]
    normalized["quantity"] = _safe_int(candidate.get("quantity", 0))
    normalized["strategy"] = str(candidate.get("strategy", candidate.get("setup_type", "TRADE")) or "TRADE")
    normalized["validation_score"] = _safe_float(candidate.get("validation_score", candidate.get("score", 0.0)))
    normalized["reason_codes"] = list(candidate.get("reason_codes", [])) if isinstance(candidate.get("reason_codes"), list) else []
    return normalized


def trade_unique_key(candidate: dict[str, Any], bucket_minutes: int = 5) -> str:
    normalized = normalize_trade_schema(candidate)
    parsed = _parse_timestamp(normalized.get("signal_time"))
    if parsed is None:
        bucket = "NA"
    else:
        minute_bucket = (parsed.minute // max(bucket_minutes, 1)) * max(bucket_minutes, 1)
        bucket = parsed.replace(minute=minute_bucket, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    setup_type = str(normalized.get("setup_type", normalized.get("strategy", "TRADE")) or "TRADE").upper()
    return "|".join([str(normalized.get("symbol", "UNKNOWN")).upper(), str(normalized.get("side", "")).upper(), setup_type, bucket])


def evaluate_trade_guards(
    candidate: dict[str, Any],
    historical_rows: list[dict[str, Any]],
    config: ExecutionGuardConfig | None = None,
) -> dict[str, Any]:
    cfg = config or ExecutionGuardConfig()
    normalized = normalize_trade_schema(candidate)
    reasons: list[str] = []
    signal_time = _parse_timestamp(normalized.get("signal_time"))
    if signal_time is None:
        reasons.append("MISSING_TIMESTAMP")
    if normalized["stop_loss"] <= 0:
        reasons.append("MISSING_STOP_LOSS")
    if normalized["target_price"] <= 0:
        reasons.append("MISSING_TARGET")
    if normalized["quantity"] <= 0:
        reasons.append("INVALID_QUANTITY")
    if normalized["entry_price"] <= 0:
        reasons.append("MISSING_PRICE")
    if str(normalized.get("side", "")) not in {"BUY", "SELL"}:
        reasons.append("INVALID_SIDE")

    if signal_time is not None:
        session_open = _parse_time(cfg.session_start)
        session_close = _parse_time(cfg.session_end)
        if signal_time.time() < session_open or signal_time.time() > session_close:
            reasons.append("OUTSIDE_SESSION")

    unique_key = trade_unique_key(normalized)
    historical_norm = [normalize_trade_schema(row) for row in historical_rows]
    historical_keys = {trade_unique_key(row) for row in historical_norm}
    if unique_key in historical_keys:
        reasons.append("DUPLICATE_TRADE")

    if signal_time is not None:
        same_day_rows = []
        realized_pnl = 0.0
        latest_times: list[datetime] = []
        for row in historical_norm:
            row_time = _parse_timestamp(row.get("signal_time"))
            if row_time is None:
                continue
            if row_time.date() == signal_time.date():
                same_day_rows.append(row)
                realized_pnl += _safe_float(row.get("pnl", 0.0))
                latest_times.append(row_time)
        if cfg.max_trades_per_day > 0 and len(same_day_rows) >= cfg.max_trades_per_day:
            reasons.append("MAX_TRADES_PER_DAY")
        if cfg.max_daily_loss > 0 and realized_pnl <= -abs(cfg.max_daily_loss):
            reasons.append("MAX_DAILY_LOSS")
        if latest_times and cfg.cooldown_minutes > 0:
            last_trade_time = max(latest_times)
            if (signal_time - last_trade_time).total_seconds() < cfg.cooldown_minutes * 60:
                reasons.append("COOLDOWN_ACTIVE")

    return {
        "allowed": len(reasons) == 0,
        "reasons": reasons,
        "normalized": normalized,
        "unique_key": unique_key,
    }


def execute_paper_trades(
    candidates: list[dict[str, Any]],
    output_path: Path,
    deduplicate: bool = True,
    *,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
    order_history_path: Path | None = None,
):
    """Mandatory paper-execution gateway for all repo execution paths."""
    from src.execution.paper_execution_service import CanonicalExecutionConfig, run_canonical_paper_execution
    from src.execution_engine import _execute_candidates, _read_trade_rows

    existing_rows = _read_trade_rows(output_path)
    result, _blocked_rows, _state = run_canonical_paper_execution(
        candidates,
        config=CanonicalExecutionConfig(
            output_path=output_path,
            order_history_path=order_history_path,
            deduplicate=deduplicate,
            max_trades_per_day=int(max_trades_per_day or 0),
            max_daily_loss=float(max_daily_loss or 0.0),
            max_open_trades=max_open_trades,
        ),
        adapter=lambda allowed, resolved_output_path, **kwargs: _execute_candidates(
            allowed,
            resolved_output_path,
            execution_type="PAPER",
            deduplicate=bool(kwargs.get("deduplicate", deduplicate)),
            max_trades_per_day=(None if not kwargs.get("max_trades_per_day") else kwargs.get("max_trades_per_day")),
            max_daily_loss=(None if not kwargs.get("max_daily_loss") else kwargs.get("max_daily_loss")),
            max_open_trades=kwargs.get("max_open_trades"),
            order_history_path=kwargs.get("order_history_path"),
        ),
        existing_rows=existing_rows,
    )
    return result


__all__ = [
    "ExecutionGuardConfig",
    "evaluate_trade_guards",
    "execute_paper_trades",
    "normalize_trade_schema",
    "trade_unique_key",
]
