from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Sequence

import pandas as pd

from vinayak.strategies.demand_supply.enrichment import prepare_supply_demand_frame
from vinayak.strategies.demand_supply.models import SupplyDemandStrategyConfig, SupplyDemandZone, ValidationDecision
from vinayak.strategies.demand_supply.normalization import parse_time


def zone_score_bucket(score: float) -> str:
    if score >= 80.0:
        return "80+"
    if score >= 70.0:
        return "70-79"
    if score >= 60.0:
        return "60-69"
    return "<60"


def validation_score_bucket(score: float) -> str:
    if score >= 9.0:
        return "9-10"
    if score >= 7.0:
        return "7-8"
    if score >= 5.0:
        return "5-6"
    return "<5"


def entry_candle(frame: pd.DataFrame, zone: SupplyDemandZone) -> pd.Series:
    for idx in range(zone.base_end_index + 1, len(frame)):
        row = frame.iloc[idx]
        if float(row["low"]) <= float(zone.zone_high) and float(row["high"]) >= float(zone.zone_low):
            return row
    return frame.iloc[min(zone.base_end_index + 1, len(frame) - 1)]


def body_ratio(row: pd.Series) -> float:
    candle_range = max(float(row["high"] - row["low"]), 1e-6)
    return abs(float(row["close"]) - float(row["open"])) / candle_range


def directional_wick_ratio(row: pd.Series, side: str) -> float:
    candle_range = max(float(row["high"] - row["low"]), 1e-6)
    if side == "BUY":
        return max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0) / candle_range
    return max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0) / candle_range


def trade_side(zone: SupplyDemandZone) -> str:
    return "BUY" if zone.type == "demand" else "SELL"


def rejection_strength(row: pd.Series, side: str) -> float:
    candle_range = max(float(row["high"] - row["low"]), 1e-6)
    body = abs(float(row["close"]) - float(row["open"])) / candle_range
    lower_wick = max(min(float(row["open"]), float(row["close"])) - float(row["low"]), 0.0) / candle_range
    upper_wick = max(float(row["high"]) - max(float(row["open"]), float(row["close"])), 0.0) / candle_range
    if side == "BUY":
        return body * 60.0 + lower_wick * 40.0
    return body * 60.0 + upper_wick * 40.0


def existing_zone_ids(rows: Sequence[dict[str, Any]] | None) -> set[str]:
    if not rows:
        return set()
    return {str(row.get("zone_id", "") or "").strip() for row in rows if str(row.get("zone_id", "") or "").strip()}


def latest_trade_timestamp(rows: Sequence[dict[str, Any]] | None) -> datetime | None:
    if not rows:
        return None
    parsed: list[datetime] = []
    for row in rows:
        raw = row.get("trade_time") or row.get("timestamp") or row.get("signal_time") or row.get("entry_time")
        stamp = pd.to_datetime(raw, errors="coerce")
        if not pd.isna(stamp):
            parsed.append(pd.Timestamp(stamp).to_pydatetime())
    return max(parsed) if parsed else None


def find_retest_confirmation(frame: pd.DataFrame, zone: SupplyDemandZone, side: str, config: SupplyDemandStrategyConfig) -> tuple[pd.Series, dict[str, Any]]:
    touch_idx: int | None = None
    for idx in range(zone.base_end_index + 1, len(frame)):
        row = frame.iloc[idx]
        if float(row["low"]) <= float(zone.zone_high) and float(row["high"]) >= float(zone.zone_low):
            touch_idx = idx
            break
    if touch_idx is None:
        fallback = entry_candle(frame, zone)
        return fallback, {"fresh_zone": zone.test_count <= 1 and not zone.violated, "retest_clean": False, "retest_confirmed": False, "retest_touch_count": 0, "retest_touch_timestamp": "", "retest_confirmation_timestamp": "", "retest_body_ratio": 0.0, "retest_wick_ratio": 0.0}
    touch_row = frame.iloc[touch_idx]
    confirmation_limit = min(len(frame), touch_idx + max(int(config.max_retest_bars), 1) + 1)
    for idx in range(touch_idx + 1, confirmation_limit):
        row = frame.iloc[idx]
        ratio = body_ratio(row)
        wick_ratio = directional_wick_ratio(row, side)
        if side == "BUY":
            directional_close = float(row["close"]) > float(row["open"]) and float(row["close"]) >= float(zone.zone_high)
        else:
            directional_close = float(row["close"]) < float(row["open"]) and float(row["close"]) <= float(zone.zone_low)
        if directional_close and ratio >= max(float(config.min_confirmation_body_ratio) - 0.1, 0.4):
            return row, {"fresh_zone": zone.test_count <= 1 and not zone.violated, "retest_clean": True, "retest_confirmed": True, "retest_touch_count": 1, "retest_touch_timestamp": pd.Timestamp(touch_row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"), "retest_confirmation_timestamp": pd.Timestamp(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"), "retest_body_ratio": round(ratio, 4), "retest_wick_ratio": round(wick_ratio, 4)}
    return touch_row, {"fresh_zone": zone.test_count == 0 and not zone.violated, "retest_clean": False, "retest_confirmed": False, "retest_touch_count": 1, "retest_touch_timestamp": pd.Timestamp(touch_row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"), "retest_confirmation_timestamp": "", "retest_body_ratio": 0.0, "retest_wick_ratio": 0.0}


def validate_supply_demand_trade(
    zone: SupplyDemandZone,
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    traded_zone_ids: set[str] | None = None,
    last_trade_at: datetime | None = None,
) -> ValidationDecision:
    cfg = config or SupplyDemandStrategyConfig()
    frame = prepare_supply_demand_frame(candles)
    side = trade_side(zone)
    row, retest = find_retest_confirmation(frame, zone, side, cfg)
    reasons: list[str] = []
    strength = rejection_strength(row, side)
    entry_time = pd.Timestamp(row["timestamp"]).to_pydatetime()
    zone_selection_score = round(float(zone.total_score) / 10.0, 2)
    strong_move_away = float(zone.move_away_score) >= 50.0
    clean_base = float(zone.cleanliness_score) >= 45.0
    rejection_strong = strength >= max(float(cfg.min_reaction_strength) * 100.0 - 30.0, 45.0)
    structure_clean = float(zone.structure_clarity_score) >= min(float(cfg.min_structure_clarity_score), 50.0)
    score = 0
    if bool(retest["fresh_zone"]):
        score += 2
    if strong_move_away:
        score += 2
    if clean_base:
        score += 2
    if bool(retest["retest_clean"]):
        score += 2
    if rejection_strong:
        score += 1
    if structure_clean:
        score += 1
    if zone.total_score < float(cfg.min_total_score) or zone_selection_score < max(float(cfg.min_zone_selection_score) - 1.0, 4.0):
        reasons.append("weak_zone_score")
    if zone.violated:
        reasons.append("violated_zone")
    if zone.test_count > int(cfg.max_retests):
        reasons.append("max_retests_exceeded")
    if zone.departure_ratio < float(cfg.min_departure_ratio):
        reasons.append("weak_departure")
    if entry_time.time() > parse_time(str(cfg.entry_cutoff_time)):
        reasons.append("entry_after_cutoff")
    if bool(cfg.require_retest_confirmation) and not bool(retest["retest_confirmed"]):
        reasons.append("retest_not_confirmed")
    if not rejection_strong:
        reasons.append("rejection_candle_weak")
    if not structure_clean:
        reasons.append("weak_structure_clarity")
    if traded_zone_ids and zone.zone_id in traded_zone_ids:
        reasons.append("duplicate_zone")
    if last_trade_at is not None and (entry_time - last_trade_at) < timedelta(minutes=int(cfg.cooldown_minutes)):
        reasons.append("duplicate_zone")
    if bool(cfg.use_session_filter):
        session_value = str(row.get("session", "") or "").upper()
        allowed = {str(item).upper() for item in cfg.allowed_sessions}
        if session_value not in allowed:
            reasons.append("session_filter_failed")
    if bool(cfg.use_vwap_filter) and "vwap" in frame.columns:
        if side == "BUY" and float(row["close"]) < float(row["vwap"]):
            reasons.append("vwap_alignment_failed")
        if side == "SELL" and float(row["close"]) > float(row["vwap"]):
            reasons.append("vwap_alignment_failed")
    if bool(cfg.use_trend_filter):
        ema21 = float(row.get("ema_21", row["close"]))
        ema50 = float(row.get("ema_50", row["close"]))
        if side == "BUY" and ema21 < ema50:
            reasons.append("trend_alignment_failed")
        if side == "SELL" and ema21 > ema50:
            reasons.append("trend_alignment_failed")
    if bool(cfg.use_volatility_filter) and float(row.get("atr_pct", 0.0)) < float(cfg.volatility_floor_pct):
        reasons.append("weak_departure")
    stop_buffer = max(abs(zone.zone_high - zone.zone_low) * float(cfg.stop_buffer_pct), float(row.get("atr_14", 0.0)) * float(cfg.stop_buffer_pct), 0.01)
    entry_price = float(row["close"])
    if side == "BUY":
        stop_loss = float(zone.zone_low) - stop_buffer
        target_price = entry_price + (entry_price - stop_loss) * float(cfg.min_rr_ratio)
    else:
        stop_loss = float(zone.zone_high) + stop_buffer
        target_price = entry_price - (stop_loss - entry_price) * float(cfg.min_rr_ratio)
    risk_per_unit = abs(entry_price - stop_loss)
    rr_ratio = abs(target_price - entry_price) / max(risk_per_unit, 1e-6)
    if rr_ratio < float(cfg.min_rr_ratio):
        reasons.append("invalid_rr")
    if score < 7:
        reasons.append("weak_zone_score")
    metrics = {
        "entry_timestamp": entry_time.strftime("%Y-%m-%d %H:%M:%S"),
        "entry_price": round(entry_price, 4),
        "stop_loss": round(stop_loss, 4),
        "target_price": round(target_price, 4),
        "risk_per_unit": round(risk_per_unit, 4),
        "rr_ratio": round(rr_ratio, 4),
        "rejection_strength": round(strength, 2),
        "zone_score": round(zone.total_score, 2),
        "zone_score_bucket": zone_score_bucket(float(zone.total_score)),
        "zone_selection_score": zone_selection_score,
        "structure_type": str(zone.structure_type),
        "session": str(row.get("session", "") or "UNKNOWN").upper(),
        "structure_clarity": round(float(zone.structure_clarity_score), 2),
        "strict_validation_score": int(score),
        "validation_score_bucket": validation_score_bucket(float(score)),
        "fresh_zone": bool(retest["fresh_zone"]),
        "strong_move_away": strong_move_away,
        "clean_base": clean_base,
        "retest_clean": bool(retest["retest_clean"]),
        "retest_confirmed": bool(retest["retest_confirmed"]),
        "retest_touch_count": int(retest["retest_touch_count"]),
        "retest_touch_timestamp": str(retest["retest_touch_timestamp"]),
        "retest_confirmation_timestamp": str(retest["retest_confirmation_timestamp"]),
        "retest_body_ratio": float(retest["retest_body_ratio"]),
        "retest_wick_ratio": float(retest["retest_wick_ratio"]),
        "rejection_strong": rejection_strong,
        "structure_clean": structure_clean,
    }
    validation_score = max(0.0, round(score, 2))
    return ValidationDecision(len(reasons) == 0, reasons, validation_score, len(reasons) == 0, metrics)


__all__ = [
    "existing_zone_ids",
    "latest_trade_timestamp",
    "trade_side",
    "validate_supply_demand_trade",
    "validation_score_bucket",
    "zone_score_bucket",
]
