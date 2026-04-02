from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, time
from pathlib import Path
from typing import Any

import pandas as pd

from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric
from vinayak.validation.engine import validate_trade
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.live_execution_adapter import LiveExecutionAdapter
from vinayak.execution.paper_execution_adapter import PaperExecutionAdapter


_DEFAULT_SESSION_START = time(9, 15)
_DEFAULT_SESSION_END = time(15, 30)
_DEFAULT_COOLDOWN_MINUTES = 15
_DEFAULT_BUCKET_MINUTES = 5
_EXECUTED_STATUSES = {"FILLED", "EXECUTED", "SENT"}


@dataclass(slots=True)
class WorkspaceExecutionResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    executed_rows: list[dict[str, Any]] = field(default_factory=list)
    blocked_rows: list[dict[str, Any]] = field(default_factory=list)
    skipped_rows: list[dict[str, Any]] = field(default_factory=list)
    error_rows: list[dict[str, Any]] = field(default_factory=list)
    executed_count: int = 0
    blocked_count: int = 0
    skipped_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0


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


def _parse_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = pd.to_datetime(raw, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).to_pydatetime()


def _stringify_timestamp(value: Any) -> str:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return str(value or "")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _existing_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle) if row]
    except Exception:
        return []


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _existing_rows(path)
    merged = existing + [{key: ("" if value is None else value) for key, value in row.items()} for row in rows]
    fieldnames: list[str] = []
    for row in merged:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _normalize_validation_reasons(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _normalize_candidate(candidate: dict[str, Any], *, strategy: str, symbol: str) -> dict[str, Any]:
    item = dict(candidate)
    resolved_symbol = str(item.get("symbol") or symbol or "UNKNOWN").strip().upper() or "UNKNOWN"
    resolved_strategy = str(item.get("strategy_name") or item.get("strategy") or strategy or "UNKNOWN").strip() or "UNKNOWN"
    timestamp = _stringify_timestamp(item.get("timestamp") or item.get("signal_time") or item.get("entry_time"))
    side = str(item.get("side") or item.get("type") or "").strip().upper()
    entry = _safe_float(item.get("entry", item.get("entry_price", item.get("price", item.get("close", 0.0)))))
    stop_loss = _safe_float(item.get("stop_loss", item.get("stoploss", item.get("sl", 0.0))))
    target = _safe_float(item.get("target", item.get("target_price", item.get("tp", 0.0))))
    quantity = _safe_int(item.get("quantity", 0))
    setup_type = str(item.get("setup_type") or item.get("zone_type") or resolved_strategy).strip().upper() or resolved_strategy.upper()
    zone_id = str(item.get("zone_id") or f"{resolved_symbol}_{setup_type}_{timestamp.replace(':', '').replace(' ', '_')}").strip()
    trade_id = str(item.get("trade_id") or f"{resolved_symbol}_{setup_type}_{side}_{timestamp.replace(':', '').replace(' ', '_')}").strip()
    validation_status = str(item.get("validation_status") or "PENDING").strip().upper()
    validation_score = round(_safe_float(item.get("validation_score", item.get("score", 0.0))), 2)
    validation_reasons = _normalize_validation_reasons(item.get("validation_reasons", item.get("reason_codes", [])))
    execution_allowed = bool(item.get("execution_allowed", validation_status == "PASS" and not validation_reasons))
    timeframe = str(item.get("timeframe") or item.get("interval") or "").strip()
    normalized = {
        **item,
        "symbol": resolved_symbol,
        "timestamp": timestamp,
        "signal_time": str(item.get("signal_time") or timestamp),
        "entry_time": str(item.get("entry_time") or timestamp),
        "strategy_name": resolved_strategy,
        "strategy": resolved_strategy,
        "setup_type": setup_type,
        "zone_id": zone_id,
        "trade_id": trade_id,
        "side": side,
        "entry": round(entry, 4),
        "entry_price": round(entry, 4),
        "stop_loss": round(stop_loss, 4),
        "stoploss": round(stop_loss, 4),
        "target": round(target, 4),
        "target_price": round(target, 4),
        "quantity": quantity,
        "validation_status": validation_status,
        "validation_score": validation_score,
        "validation_reasons": validation_reasons,
        "execution_allowed": execution_allowed,
        "timeframe": timeframe,
    }
    risk = abs(entry - stop_loss)
    reward = abs(target - entry)
    normalized["rr_ratio"] = round(reward / risk, 4) if risk > 1e-9 else round(_safe_float(item.get("rr_ratio", 0.0)), 4)
    normalized["reason_codes"] = list(normalized.get("reason_codes", [])) if isinstance(normalized.get("reason_codes"), list) else []
    return normalized


def _trade_unique_key(candidate: dict[str, Any], bucket_minutes: int = _DEFAULT_BUCKET_MINUTES) -> str:
    parsed = _parse_timestamp(candidate.get("signal_time") or candidate.get("timestamp"))
    if parsed is None:
        bucket = "NA"
    else:
        size = max(int(bucket_minutes), 1)
        minute_bucket = (parsed.minute // size) * size
        bucket = parsed.replace(minute=minute_bucket, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    return "|".join([
        str(candidate.get("symbol", "UNKNOWN")).upper(),
        str(candidate.get("side", "")).upper(),
        str(candidate.get("setup_type", candidate.get("strategy_name", "TRADE"))).upper(),
        bucket,
    ])


def _executed_trade_times(rows: list[dict[str, Any]], signal_date: datetime) -> list[datetime]:
    times: list[datetime] = []
    for row in rows:
        status = str(row.get("execution_status") or row.get("status") or row.get("trade_status") or "").upper()
        if status and status not in _EXECUTED_STATUSES:
            continue
        row_time = _parse_timestamp(row.get("signal_time") or row.get("timestamp") or row.get("entry_time"))
        if row_time is not None and row_time.date() == signal_date.date():
            times.append(row_time)
    return times


def _guard_reasons(candidate: dict[str, Any], historical_rows: list[dict[str, Any]], batch_keys: set[str], *, max_trades_per_day: int | None, max_daily_loss: float | None, cooldown_minutes: int = _DEFAULT_COOLDOWN_MINUTES) -> tuple[list[str], str]:
    reasons: list[str] = []
    signal_time = _parse_timestamp(candidate.get("signal_time") or candidate.get("timestamp"))
    if signal_time is None:
        reasons.append("MISSING_TIMESTAMP")
    if candidate.get("validation_status") != "PASS":
        reasons.append("VALIDATION_STATUS_FAIL")
    if not bool(candidate.get("execution_allowed", False)):
        reasons.append("EXECUTION_GATE_BLOCKED")
    if _safe_float(candidate.get("entry_price")) <= 0:
        reasons.append("MISSING_PRICE")
    if _safe_float(candidate.get("stop_loss")) <= 0:
        reasons.append("MISSING_STOP_LOSS")
    if _safe_float(candidate.get("target_price")) <= 0:
        reasons.append("MISSING_TARGET")
    if _safe_int(candidate.get("quantity")) <= 0:
        reasons.append("INVALID_QUANTITY")
    if str(candidate.get("side", "")).upper() not in {"BUY", "SELL"}:
        reasons.append("INVALID_SIDE")

    unique_key = _trade_unique_key(candidate)
    historical_keys = {_trade_unique_key(_normalize_candidate(dict(row), strategy=str(row.get("strategy_name") or row.get("strategy") or "TRADE"), symbol=str(row.get("symbol") or candidate.get("symbol") or "UNKNOWN"))) for row in historical_rows}
    if unique_key in historical_keys or unique_key in batch_keys:
        reasons.append("DUPLICATE_TRADE")

    if signal_time is not None:
        if signal_time.time() < _DEFAULT_SESSION_START or signal_time.time() > _DEFAULT_SESSION_END:
            reasons.append("OUTSIDE_SESSION")
        today_rows = [row for row in historical_rows if (_parse_timestamp(row.get("signal_time") or row.get("timestamp") or row.get("entry_time")) or signal_time).date() == signal_time.date()]
        executed_today = [row for row in today_rows if str(row.get("execution_status") or row.get("status") or row.get("trade_status") or "").upper() in _EXECUTED_STATUSES]
        if max_trades_per_day and max_trades_per_day > 0 and len(executed_today) >= int(max_trades_per_day):
            reasons.append("MAX_TRADES_PER_DAY")
        realized_pnl = sum(_safe_float(row.get("pnl", 0.0)) for row in today_rows)
        if max_daily_loss and float(max_daily_loss) > 0 and realized_pnl <= -abs(float(max_daily_loss)):
            reasons.append("MAX_DAILY_LOSS")
        recent_times = _executed_trade_times(historical_rows, signal_time)
        if recent_times and cooldown_minutes > 0:
            delta_seconds = (signal_time - max(recent_times)).total_seconds()
            if delta_seconds < cooldown_minutes * 60:
                reasons.append("COOLDOWN_ACTIVE")

    return reasons, unique_key


def prepare_workspace_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for raw in signal_rows:
        candidate = _normalize_candidate(dict(raw), strategy=strategy, symbol=symbol)
        if candidate["validation_status"] in {"", "PENDING"}:
            validation = validate_trade(candidate, candles)
            candidate["validation_status"] = str(validation.get("decision", "FAIL") or "FAIL").upper()
            candidate["validation_score"] = round(_safe_float(validation.get("score", 0.0)), 2)
            candidate["validation_reasons"] = [str(item) for item in list(validation.get("reasons", []) or []) if str(item).strip()]
            candidate["execution_allowed"] = candidate["validation_status"] == "PASS"
            candidate["validation_metrics"] = dict(validation.get("metrics", {}) or {})
        candidate["reason_codes"] = list(candidate.get("reason_codes", [])) if isinstance(candidate.get("reason_codes"), list) else []
        candidate["reason_codes"] = list(dict.fromkeys(candidate["reason_codes"] + candidate["validation_reasons"]))
        prepared.append(candidate)
    valid_count = sum(1 for item in prepared if str(item.get('validation_status', '')).upper() == 'PASS')
    rejected_count = max(0, len(prepared) - valid_count)
    increment_metric('trade_candidates_total', len(prepared))
    increment_metric('zones_detected_total', len(prepared))
    increment_metric('zones_accepted_total', valid_count)
    increment_metric('zones_rejected_total', rejected_count)
    score_values = [float(item.get('validation_score', item.get('score', 0.0)) or 0.0) for item in prepared]
    set_metric('zone_score_avg', round(sum(score_values) / len(score_values), 2) if score_values else 0.0)
    set_metric('total_signals_today', len(prepared))
    set_metric('valid_signals_today', valid_count)
    set_metric('rejected_trades_today', rejected_count)
    log_event(component='execution_gateway', event_name='trade_candidates_prepared', severity='INFO', message='Prepared workspace candidates', context_json={'total': len(prepared), 'valid': valid_count, 'rejected': rejected_count})
    return prepared


def execute_workspace_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    signal_rows: list[dict[str, Any]],
    *,
    execution_mode: str,
    paper_log_path: str,
    live_log_path: str,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    security_map_path: str = 'data/dhan_security_map.csv',
    resolve_live_kwargs: callable | None = None,
):
    import time
    cycle_started = time.perf_counter()
    candidates = prepare_workspace_candidates(strategy, symbol, candles, signal_rows)
    set_metric('trading_app_up', 1)
    mode = str(execution_mode or 'NONE').upper()
    output_path = Path(str(live_log_path if mode == 'LIVE' else paper_log_path))
    historical_rows = _existing_rows(output_path)
    batch_keys: set[str] = set()
    result = WorkspaceExecutionResult()
    rows_to_write: list[dict[str, Any]] = []

    paper_adapter = PaperExecutionAdapter()
    live_adapter = LiveExecutionAdapter()
    live_kwargs = dict(resolve_live_kwargs(security_map_path) if resolve_live_kwargs is not None and mode == 'LIVE' else {})
    broker_name = str(live_kwargs.get('broker_name', 'SIM' if mode != 'LIVE' else 'LIVE'))

    for candidate in candidates:
        reasons, unique_key = _guard_reasons(
            candidate,
            historical_rows + rows_to_write,
            batch_keys,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
        )
        row = dict(candidate)
        row['trade_key'] = unique_key
        row['broker_name'] = broker_name
        row['execution_mode'] = mode
        row['price'] = row.get('entry_price')

        if reasons:
            row['execution_status'] = 'BLOCKED'
            row['trade_status'] = 'BLOCKED'
            row['reason'] = ', '.join(reasons)
            row['blocked_reason'] = row['reason']
            row['duplicate_reason'] = 'DUPLICATE_TRADE' if 'DUPLICATE_TRADE' in reasons else ''
            result.rows.append(row)
            increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
            if 'DUPLICATE_TRADE' in reasons:
                increment_metric('duplicate_trade_blocks_total', 1)
            log_event(component='execution_gateway', event_name='trade_execution_blocked', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade blocked before execution', context_json={'reasons': reasons, 'trade_id': row.get('trade_id', '')})
            result.blocked_rows.append(row)
            result.blocked_count += 1
            if 'DUPLICATE_TRADE' in reasons:
                result.duplicate_count += 1
            rows_to_write.append(row)
            batch_keys.add(unique_key)
            continue

        command = ExecutionCreateCommand(
            mode=mode,
            broker=broker_name,
            status='FILLED' if mode != 'LIVE' else None,
            executed_price=_safe_float(row.get('entry_price')),
        )
        try:
            if mode == 'LIVE':
                adapter_result = live_adapter.execute(command=command, reviewed_trade=None, signal=None)
            else:
                adapter_result = paper_adapter.execute(command=command, reviewed_trade=None, signal=None)
            row['execution_status'] = str(adapter_result.status or 'FILLED').upper()
            row['trade_status'] = 'EXECUTED' if row['execution_status'] in _EXECUTED_STATUSES else row['execution_status']
            row['broker_name'] = adapter_result.broker
            row['broker_reference'] = adapter_result.broker_reference
            row['price'] = _safe_float(adapter_result.executed_price, _safe_float(row.get('entry_price')))
            row['executed_at_utc'] = adapter_result.executed_at.isoformat() if adapter_result.executed_at is not None else ''
            row['reason'] = str(adapter_result.notes or '')
            row['duplicate_reason'] = ''
            if row['execution_status'] in _EXECUTED_STATUSES:
                result.executed_rows.append(row)
                result.executed_count += 1
                increment_metric('paper_trades_executed_total', 1 if mode == 'PAPER' else 0)
                set_metric('executed_paper_trades_today', result.executed_count if mode == 'PAPER' else 0)
                log_event(component='execution_gateway', event_name='trade_execution_success', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='INFO', message='Trade executed', context_json={'trade_id': row.get('trade_id', ''), 'mode': mode, 'status': row.get('execution_status', '')})
            else:
                result.blocked_rows.append(row)
                result.blocked_count += 1
                increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
                log_event(component='execution_gateway', event_name='trade_execution_nonfill', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade did not reach executed state', context_json={'trade_id': row.get('trade_id', ''), 'mode': mode, 'status': row.get('execution_status', '')})
            result.rows.append(row)
            increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
            if 'DUPLICATE_TRADE' in reasons:
                increment_metric('duplicate_trade_blocks_total', 1)
            log_event(component='execution_gateway', event_name='trade_execution_blocked', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade blocked before execution', context_json={'reasons': reasons, 'trade_id': row.get('trade_id', '')})
            rows_to_write.append(row)
            batch_keys.add(unique_key)
        except Exception as exc:
            row['execution_status'] = 'ERROR'
            row['trade_status'] = 'ERROR'
            row['reason'] = str(exc)
            row['duplicate_reason'] = ''
            result.rows.append(row)
            increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
            if 'DUPLICATE_TRADE' in reasons:
                increment_metric('duplicate_trade_blocks_total', 1)
            log_event(component='execution_gateway', event_name='trade_execution_blocked', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade blocked before execution', context_json={'reasons': reasons, 'trade_id': row.get('trade_id', '')})
            result.error_rows.append(row)
            result.error_count += 1
            rows_to_write.append(row)
            batch_keys.add(unique_key)

    _write_rows(output_path, rows_to_write)
    duration = round(time.perf_counter() - cycle_started, 4)
    set_metric('trading_cycle_duration_seconds', duration)
    record_stage('execute', status='SUCCESS' if result.error_count == 0 else 'WARN', duration_seconds=duration, symbol=symbol, strategy=strategy, message='Workspace execution cycle finished')
    set_metric('paper_trade_rejections_total', int(_safe_int(result.blocked_count + result.error_count)))
    return candidates, result


__all__ = [
    'WorkspaceExecutionResult',
    'execute_workspace_candidates',
    'prepare_workspace_candidates',
]

