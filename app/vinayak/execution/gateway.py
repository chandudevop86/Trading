from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

import pandas as pd

from vinayak.db.repositories.execution_state_repository import ExecutionStateRepository
from vinayak.domain.models import (
    ExecutionMode,
    ExecutionRequest,
    ExecutionSide,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalType,
)
from vinayak.execution.guards import (
    WorkspaceGuardContext,
    evaluate_cooldown_guard,
    evaluate_duplicate_guard,
    evaluate_portfolio_guard,
    evaluate_session_guard,
)
from vinayak.execution.runtime import build_execution_facade
from vinayak.observability.observability_logger import log_event
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric
from vinayak.validation.engine import CleanerConfig, _latest_metrics, coerce_ohlcv, validate_trade


_DEFAULT_SESSION_START = time(9, 15)
_DEFAULT_SESSION_END = time(15, 30)
_DEFAULT_COOLDOWN_MINUTES = 15
_DEFAULT_BUCKET_MINUTES = 5
_EXECUTED_STATUSES = {"FILLED", "EXECUTED", "SENT", "ACCEPTED"}


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
def _coerce_execution_allowed(value: Any, *, validation_status: str, validation_reasons: list[str]) -> bool:
    default = validation_status == "PASS" and not validation_reasons
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    lowered = str(value).strip().lower()
    if lowered == "":
        return default
    if lowered in {"1", "true", "yes", "y", "on", "pass", "passed"}:
        return True
    if lowered in {"0", "false", "no", "n", "off", "fail", "failed"}:
        return False
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


def _write_rows(path: Path, rows: list[dict[str, Any]], *, existing_rows: list[dict[str, Any]] | None = None) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = list(existing_rows) if existing_rows is not None else _existing_rows(path)
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
    lots = _safe_int(item.get("lots", 1))
    setup_type = str(item.get("setup_type") or item.get("zone_type") or resolved_strategy).strip().upper() or resolved_strategy.upper()
    zone_id = str(item.get("zone_id") or f"{resolved_symbol}_{setup_type}_{timestamp.replace(':', '').replace(' ', '_')}").strip()
    trade_id = str(item.get("trade_id") or f"{resolved_symbol}_{setup_type}_{side}_{timestamp.replace(':', '').replace(' ', '_')}").strip()
    validation_status = str(item.get("validation_status") or "PENDING").strip().upper()
    validation_score = round(_safe_float(item.get("validation_score", item.get("score", 0.0))), 2)
    validation_reasons = _normalize_validation_reasons(item.get("validation_reasons", item.get("reason_codes", [])))
    execution_allowed = _coerce_execution_allowed(item.get("execution_allowed"), validation_status=validation_status, validation_reasons=validation_reasons)
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
        "lots": lots,
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


def _historical_trade_keys(rows: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for row in rows:
        normalized = _normalize_candidate(
            dict(row),
            strategy=str(row.get("strategy_name") or row.get("strategy") or "TRADE"),
            symbol=str(row.get("symbol") or "UNKNOWN"),
        )
        keys.add(_trade_unique_key(normalized))
    return keys

def _has_active_trade(historical_rows: list[dict[str, Any]]) -> bool:
    for row in historical_rows:
        status = str(row.get("execution_status") or row.get("status") or row.get("trade_status") or "").upper()
        trade_status = str(row.get("trade_status") or "").upper()
        if trade_status in {"CLOSED", "EXITED", "CANCELLED", "REJECTED", "BLOCKED", "ERROR"}:
            continue
        if status in _EXECUTED_STATUSES:
            return True
    return False


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


def _guard_reasons(
    candidate: dict[str, Any],
    historical_rows: list[dict[str, Any]],
    batch_keys: set[str],
    *,
    execution_mode: str,
    state_repository: ExecutionStateRepository,
    capital: float | None,
    per_trade_risk_pct: float | None,
    max_trades_per_day: int | None,
    max_daily_loss: float | None,
    max_position_value: float | None,
    max_open_positions: int | None,
    max_symbol_exposure_pct: float | None,
    max_portfolio_exposure_pct: float | None,
    max_open_risk_pct: float | None,
    kill_switch_enabled: bool,
    cooldown_minutes: int = _DEFAULT_COOLDOWN_MINUTES,
) -> tuple[list[str], str, dict[str, Any], dict[str, Any]]:
    reasons: list[str] = []
    signal_time = _parse_timestamp(candidate.get("signal_time") or candidate.get("timestamp"))
    if candidate.get("validation_status") != "PASS":
        reasons.append("VALIDATION_STATUS_FAIL")
    if not _coerce_execution_allowed(candidate.get("execution_allowed"), validation_status=str(candidate.get("validation_status") or "").upper(), validation_reasons=_normalize_validation_reasons(candidate.get("validation_reasons", []))):
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
    adjusted_candidate = dict(candidate)
    risk_snapshot: dict[str, Any] = {}
    if signal_time is None:
        reasons.append("MISSING_TIMESTAMP")
    else:
        guard_context = WorkspaceGuardContext(
            candidate=candidate,
            signal_time=signal_time,
            execution_mode=execution_mode,
            batch_keys=batch_keys,
            current_batch_rows=historical_rows,
            state_repository=state_repository,
            trade_key=unique_key,
            capital=capital,
            per_trade_risk_pct=per_trade_risk_pct,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
            max_position_value=max_position_value,
            max_open_positions=max_open_positions,
            max_symbol_exposure_pct=max_symbol_exposure_pct,
            max_portfolio_exposure_pct=max_portfolio_exposure_pct,
            max_open_risk_pct=max_open_risk_pct,
            kill_switch_enabled=kill_switch_enabled,
            cooldown_minutes=cooldown_minutes,
            bucket_minutes=_DEFAULT_BUCKET_MINUTES,
        )
        reasons.extend(evaluate_duplicate_guard(guard_context).reasons)
        reasons.extend(evaluate_session_guard(guard_context).reasons)
        reasons.extend(evaluate_cooldown_guard(guard_context).reasons)
        portfolio_result = evaluate_portfolio_guard(guard_context)
        reasons.extend(portfolio_result.reasons)
        adjusted_candidate = portfolio_result.candidate or adjusted_candidate
        risk_snapshot = dict(portfolio_result.risk_snapshot)

    deduped: list[str] = []
    for reason in reasons:
        if reason and reason not in deduped:
            deduped.append(reason)
    return deduped, unique_key, risk_snapshot, adjusted_candidate


def _domain_timeframe(value: Any) -> Timeframe:
    mapping = {
        "1m": Timeframe.M1,
        "5m": Timeframe.M5,
        "15m": Timeframe.M15,
        "30m": Timeframe.M30,
        "1h": Timeframe.H1,
        "1d": Timeframe.D1,
    }
    return mapping.get(str(value or "").strip().lower(), Timeframe.M5)


def _execution_mode(value: str) -> ExecutionMode:
    return ExecutionMode.LIVE if str(value or "").upper() == "LIVE" else ExecutionMode.PAPER


def _execution_side(value: Any) -> ExecutionSide:
    return ExecutionSide.SELL if str(value or "").upper() == "SELL" else ExecutionSide.BUY


def _build_workspace_execution_request(
    candidate: dict[str, Any],
    *,
    execution_mode: str,
    capital: float | None,
    per_trade_risk_pct: float | None,
    max_trades_per_day: int | None,
    max_daily_loss: float | None,
    cooldown_minutes: int = _DEFAULT_COOLDOWN_MINUTES,
) -> ExecutionRequest:
    signal_time = _parse_timestamp(candidate.get("signal_time") or candidate.get("timestamp") or candidate.get("entry_time"))
    if signal_time is None:
        raise ValueError("Workspace execution candidate is missing a valid timestamp.")
    generated_at = signal_time.replace(tzinfo=UTC) if signal_time.tzinfo is None else signal_time.astimezone(UTC)
    symbol = str(candidate.get("symbol") or "UNKNOWN").strip().upper() or "UNKNOWN"
    strategy_name = str(candidate.get("strategy_name") or candidate.get("strategy") or "UNKNOWN").strip().upper() or "UNKNOWN"
    trade_id = str(candidate.get("trade_id") or candidate.get("zone_id") or f"{symbol}-{generated_at:%Y%m%d%H%M%S}")
    account_id = f"workspace:{symbol}"
    idempotency_key = f"{account_id}:{trade_id}:{str(execution_mode or '').upper()}"
    max_daily_loss_pct = Decimal("3")
    if capital and capital > 0 and max_daily_loss is not None:
        computed = Decimal(str(round((float(max_daily_loss) / float(capital)) * 100.0, 4)))
        max_daily_loss_pct = computed if computed > 0 else Decimal("0.0001")
    risk = RiskConfig(
        risk_per_trade_pct=Decimal(str(per_trade_risk_pct if per_trade_risk_pct is not None else 1)),
        max_daily_loss_pct=max_daily_loss_pct,
        max_trades_per_day=int(max_trades_per_day or 5),
        cooldown_minutes=int(cooldown_minutes),
        allow_live_trading=str(execution_mode or "").upper() == "LIVE",
        live_unlock_token_required=True,
    )
    signal = TradeSignal(
        idempotency_key=idempotency_key,
        strategy_name=strategy_name,
        symbol=symbol,
        timeframe=_domain_timeframe(candidate.get("timeframe") or candidate.get("interval")),
        signal_type=TradeSignalType.ENTRY,
        generated_at=generated_at,
        candle_timestamp=generated_at,
        side=_execution_side(candidate.get("side")),
        entry_price=Decimal(str(_safe_float(candidate.get("entry_price")))),
        stop_loss=Decimal(str(_safe_float(candidate.get("stop_loss")))),
        target_price=Decimal(str(_safe_float(candidate.get("target_price")))),
        quantity=Decimal(str(max(_safe_int(candidate.get("quantity")), 1))),
        confidence=Decimal("0.8"),
        rationale=str(candidate.get("setup_type") or candidate.get("strategy_name") or "workspace_candidate"),
        metadata={
            "trade_id": trade_id,
            "zone_id": str(candidate.get("zone_id") or ""),
            "validation_status": str(candidate.get("validation_status") or ""),
            "validation_score": _safe_float(candidate.get("validation_score")),
            "validation_reasons": list(candidate.get("validation_reasons", [])) if isinstance(candidate.get("validation_reasons"), list) else [],
        },
    )
    return ExecutionRequest(
        idempotency_key=idempotency_key,
        requested_at=generated_at,
        mode=_execution_mode(execution_mode),
        signal=signal,
        risk=risk,
        account_id=account_id,
    )


def prepare_workspace_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    cleaned_candles = coerce_ohlcv(
        candles,
        CleanerConfig(expected_interval_minutes=5, require_vwap=True, allow_vwap_compute=True),
    )
    shared_market_metrics = _latest_metrics(cleaned_candles)
    for raw in signal_rows:
        candidate = _normalize_candidate(dict(raw), strategy=strategy, symbol=symbol)
        validation = validate_trade(
            candidate,
            candles,
            cleaned_candles=cleaned_candles,
            market_metrics=shared_market_metrics,
        )
        candidate["validation_status"] = str(validation.get("decision", "FAIL") or "FAIL").upper()
        candidate["validation_score"] = round(_safe_float(validation.get("score", 0.0)), 2)
        candidate["validation_reasons"] = [str(item).strip() for item in list(validation.get("reasons", []) or []) if str(item).strip()]
        candidate["execution_allowed"] = candidate["validation_status"] == "PASS" and not candidate["validation_reasons"]
        candidate["validation_metrics"] = dict(validation.get("metrics", {}) or {})
        candidate["strict_validation_score"] = int(_safe_float(candidate["validation_metrics"].get("strict_validation_score", candidate.get("strict_validation_score", 0))))
        candidate["zone_score_components"] = dict(candidate["validation_metrics"])
        candidate["rejection_reason"] = ", ".join(candidate["validation_reasons"])
        candidate["validation_log"] = dict(validation.get("rejection_log", {}) or {})
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
    capital: float | None = None,
    per_trade_risk_pct: float | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_position_value: float | None = None,
    max_open_positions: int | None = None,
    max_symbol_exposure_pct: float | None = None,
    max_portfolio_exposure_pct: float | None = None,
    max_open_risk_pct: float | None = None,
    kill_switch_enabled: bool = False,
    security_map_path: str = 'data/dhan_security_map.csv',
    resolve_live_kwargs: callable | None = None,
    db_session: Session | None = None,
):
    candidates = prepare_workspace_candidates(strategy, symbol, candles, signal_rows)
    if db_session is None:
        raise ValueError(
            'Workspace execution requires a database-backed execution facade. '
            'Pass db_session and use build_execution_facade(session).'
        )

    from vinayak.execution.workspace_runtime import run_workspace_execution

    result = run_workspace_execution(
        strategy=strategy,
        symbol=symbol,
        candidates=candidates,
        execution_mode=execution_mode,
        paper_log_path=paper_log_path,
        live_log_path=live_log_path,
        capital=capital,
        per_trade_risk_pct=per_trade_risk_pct,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_position_value=max_position_value,
        max_open_positions=max_open_positions,
        max_symbol_exposure_pct=max_symbol_exposure_pct,
        max_portfolio_exposure_pct=max_portfolio_exposure_pct,
        max_open_risk_pct=max_open_risk_pct,
        kill_switch_enabled=kill_switch_enabled,
        security_map_path=security_map_path,
        resolve_live_kwargs=resolve_live_kwargs,
        db_session=db_session,
    )
    return candidates, result



__all__ = [
    'WorkspaceExecutionResult',
    'execute_workspace_candidates',
    'prepare_workspace_candidates',
]

