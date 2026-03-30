from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

import pandas as pd

from src.execution.contracts import normalize_candidate_contract, validate_candidate_contract
from src.execution.state import TradingState

IST = ZoneInfo('Asia/Kolkata')



@dataclass(slots=True)
class GuardConfig:
    cooldown_minutes: int = 15
    max_trades_per_day: int = 3
    max_daily_loss: float = 0.0
    allowed_start_time: str = "09:15"
    cutoff_time: str = "15:30"
    stale_after_minutes: int = 0


@dataclass(slots=True)
class GuardResult:
    allowed: bool
    reasons: list[str]
    metrics: dict[str, Any]


def _parse_time(value: str) -> datetime.time:
    return datetime.strptime(value, "%H:%M").time()


def check_all_guards(candidate: dict[str, Any], state: TradingState, config: GuardConfig | None = None) -> GuardResult:
    cfg = config or GuardConfig()
    ok, contract_reasons, normalized = validate_candidate_contract(candidate)
    reasons: list[str] = []
    metrics: dict[str, Any] = {
        "contract_valid": ok,
        "zone_id": str(normalized.get("zone_id", "") or ""),
        "validation_status": str(normalized.get("validation_status", "") or ""),
        "execution_allowed": bool(normalized.get("execution_allowed", False)),
        "rr_ratio": float(normalized.get("rr_ratio", 0.0) or 0.0),
    }
    if not ok:
        reasons.extend(contract_reasons)
    if str(normalized.get("validation_status", "FAIL") or "FAIL").upper() != "PASS":
        reasons.append("VALIDATION_NOT_PASS")
    if not bool(normalized.get("execution_allowed", False)):
        reasons.append("EXECUTION_NOT_ALLOWED")
    if not str(normalized.get("zone_id", "") or "").strip():
        reasons.append("MISSING_ZONE_ID")
    if state.is_duplicate_zone(normalized):
        reasons.append("DUPLICATE_ZONE")
    if not state.cooldown_ok(normalized, cfg.cooldown_minutes):
        reasons.append("COOLDOWN_ACTIVE")
    if not state.daily_loss_ok(normalized, cfg.max_daily_loss):
        reasons.append("MAX_DAILY_LOSS")
    if not state.max_trades_ok(normalized, cfg.max_trades_per_day):
        reasons.append("MAX_TRADES_PER_DAY")

    timestamp = pd.to_datetime(normalized.get("timestamp"), errors="coerce")
    session_timestamp = pd.NaT
    if pd.isna(timestamp):
        reasons.append("INVALID_SESSION")
    else:
        if getattr(timestamp, 'tzinfo', None) is None:
            timestamp = pd.Timestamp(timestamp).tz_localize('UTC')
        else:
            timestamp = pd.Timestamp(timestamp)
        session_timestamp = timestamp.tz_convert(IST)
        start = _parse_time(cfg.allowed_start_time)
        end = _parse_time(cfg.cutoff_time)
        if session_timestamp.time() < start or session_timestamp.time() > end:
            reasons.append("INVALID_SESSION")
        if cfg.stale_after_minutes > 0:
            minutes_old = (pd.Timestamp.now(tz=IST) - session_timestamp).total_seconds() / 60.0
            metrics["minutes_old"] = round(minutes_old, 2)
            if minutes_old > float(cfg.stale_after_minutes):
                reasons.append("STALE_CANDIDATE")

    entry = float(normalized.get("entry", 0.0) or 0.0)
    stop = float(normalized.get("stop_loss", 0.0) or 0.0)
    target = float(normalized.get("target", 0.0) or 0.0)
    side = str(normalized.get("side", "") or "").upper()
    if side == "BUY" and not (stop < entry < target):
        reasons.append("INVALID_TRADE_LEVELS")
    if side == "SELL" and not (target < entry < stop):
        reasons.append("INVALID_TRADE_LEVELS")

    unique_reasons = []
    seen: set[str] = set()
    for reason in reasons:
        if reason not in seen:
            seen.add(reason)
            unique_reasons.append(reason)
    metrics["cooldown_minutes"] = cfg.cooldown_minutes
    metrics["daily_trade_count"] = state.daily_trade_count_by_day.get(pd.Timestamp(timestamp).strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN", 0)
    metrics["daily_pnl"] = state.daily_pnl_by_day.get(pd.Timestamp(timestamp).strftime("%Y-%m-%d") if not pd.isna(timestamp) else "UNKNOWN", 0.0)
    return GuardResult(allowed=len(unique_reasons) == 0, reasons=unique_reasons, metrics=metrics)


__all__ = ["GuardConfig", "GuardResult", "check_all_guards"]

