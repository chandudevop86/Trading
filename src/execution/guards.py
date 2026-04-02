from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric


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


def execute_candidates(
    candidates: list[dict[str, Any]],
    output_path: Path,
    deduplicate: bool = True,
    *,
    execution_mode: str = "PAPER",
    broker_client: object | None = None,
    broker_name: str | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
    live_enabled: bool | None = None,
    symbol_allowlist: list[str] | set[str] | tuple[str, ...] | None = None,
    max_order_quantity: int | None = None,
    max_order_value: float | None = None,
    order_history_path: Path | None = None,
    optimizer_report_path: Path | None = None,
    enforce_optimizer_gate: bool | None = None,
):
    """Mandatory canonical execution gateway for all execution-capable paths."""
    from src.execution.paper_execution_service import CanonicalExecutionConfig, run_canonical_paper_execution
    from src.execution_engine import _execute_candidates, _read_trade_rows

    mode = str(execution_mode or "PAPER").upper()
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
            execution_type=mode,
            deduplicate=bool(kwargs.get("deduplicate", deduplicate)),
            max_trades_per_day=(None if not kwargs.get("max_trades_per_day") else kwargs.get("max_trades_per_day")),
            max_daily_loss=(None if not kwargs.get("max_daily_loss") else kwargs.get("max_daily_loss")),
            max_open_trades=kwargs.get("max_open_trades"),
            broker_client=broker_client,
            broker_name=broker_name,
            security_map=security_map,
            live_enabled=live_enabled,
            symbol_allowlist=symbol_allowlist,
            max_order_quantity=max_order_quantity,
            max_order_value=max_order_value,
            order_history_path=kwargs.get("order_history_path"),
            optimizer_report_path=optimizer_report_path,
            enforce_optimizer_gate=enforce_optimizer_gate,
        ),
        existing_rows=existing_rows,
    )
    return result


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
    import time
    started = time.perf_counter()
    try:
        result = execute_candidates(
        candidates,
        output_path,
        deduplicate=deduplicate,
        execution_mode="PAPER",
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
        order_history_path=order_history_path,
        )
        duration = round(time.perf_counter() - started, 4)
        set_metric('trading_cycle_duration_seconds', duration)
        record_stage('execute', status='SUCCESS', duration_seconds=duration, message='execute_paper_trades completed')
        log_event(component='paper_execution', event_name='execute_paper_trades', severity='INFO', message='Paper trades executed through guard gateway', context_json={'candidates': len(candidates), 'duration_seconds': duration})
        return result
    except Exception as exc:
        increment_metric('trading_cycle_failures_total', 1)
        record_stage('execute', status='FAIL', message=str(exc))
        log_exception(component='paper_execution', event_name='execute_paper_trades_failed', exc=exc, message='execute_paper_trades failed', context_json={'candidates': len(candidates)})
        raise


def execute_live_trades(
    candidates: list[dict[str, Any]],
    output_path: Path,
    deduplicate: bool = True,
    *,
    broker_client: object | None = None,
    broker_name: str | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
    live_enabled: bool | None = None,
    symbol_allowlist: list[str] | set[str] | tuple[str, ...] | None = None,
    max_order_quantity: int | None = None,
    max_order_value: float | None = None,
    order_history_path: Path | None = None,
    optimizer_report_path: Path | None = None,
    enforce_optimizer_gate: bool | None = None,
):
    import time
    started = time.perf_counter()
    try:
        result = execute_candidates(
        candidates,
        output_path,
        deduplicate=deduplicate,
        execution_mode="LIVE",
        broker_client=broker_client,
        broker_name=broker_name,
        security_map=security_map,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
        live_enabled=live_enabled,
        symbol_allowlist=symbol_allowlist,
        max_order_quantity=max_order_quantity,
        max_order_value=max_order_value,
        order_history_path=order_history_path,
        optimizer_report_path=optimizer_report_path,
        enforce_optimizer_gate=enforce_optimizer_gate,
    )


__all__ = [
    "ExecutionGuardConfig",
    "evaluate_trade_guards",
    "execute_candidates",
    "execute_live_trades",
    "execute_paper_trades",
    "normalize_trade_schema",
    "trade_unique_key",
]
