from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from vinayak.data.cleaner import CleanerConfig, OHLCVValidationError, coerce_ohlcv
from vinayak.metrics.validation_metrics import compute_setup_quality_score
from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric


@dataclass(slots=True)
class ValidationConfig:
    rr_threshold: float = 2.0
    min_score: float = 7.0
    expected_interval_minutes: int = 5
    require_vwap_alignment: bool = True
    require_trend_alignment: bool = True
    max_chop_score: float = 6.0
    min_volatility_pct: float = 0.15
    min_zone_score: float = 50.0
    min_freshness_score: float = 60.0
    min_move_away_score: float = 50.0
    min_base_quality_score: float = 45.0
    min_retest_quality_score: float = 60.0
    min_rejection_strength: float = 45.0
    min_structure_clarity: float = 50.0


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_component_score(value: object, default: float = 0.0) -> float:
    score = _safe_float(value, default)
    if score <= 10.0:
        score *= 10.0
    return round(max(0.0, min(score, 100.0)), 4)


def _safe_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in {"1", "true", "yes", "y", "on", "pass", "passed"}:
        return True
    if lowered in {"0", "false", "no", "n", "off", "fail", "failed"}:
        return False
    return default


def _side(setup: dict[str, Any]) -> str:
    return str(setup.get("side", setup.get("type", "")) or "").strip().upper()


def _latest_metrics(frame: pd.DataFrame) -> dict[str, float | bool]:
    candle = frame.iloc[-1]
    close = float(candle["close"])
    ema_fast = frame["close"].ewm(span=5, adjust=False).mean().iloc[-1]
    ema_slow = frame["close"].ewm(span=13, adjust=False).mean().iloc[-1]
    tr = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - frame["close"].shift(1)).abs(),
            (frame["low"] - frame["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = float(tr.rolling(14, min_periods=1).mean().iloc[-1])
    atr_pct = (atr / close) * 100.0 if close > 0 else 0.0
    overlap_values: list[float] = []
    sample = frame.tail(min(len(frame), 10))
    for idx in range(1, len(sample)):
        prev = sample.iloc[idx - 1]
        cur = sample.iloc[idx]
        overlap = max(0.0, min(float(prev["high"]), float(cur["high"])) - max(float(prev["low"]), float(cur["low"])))
        union = max(float(max(prev["high"], cur["high"]) - min(prev["low"], cur["low"])), 1e-6)
        overlap_values.append(overlap / union)
    chop_score = min(10.0, (sum(overlap_values) / max(len(overlap_values), 1)) * 10.0)
    return {
        "close": close,
        "vwap": float(candle.get("vwap", close)),
        "ema_fast": float(ema_fast),
        "ema_slow": float(ema_slow),
        "atr_pct": float(atr_pct),
        "chop_score": float(chop_score),
    }


def _build_scorecard(setup: dict[str, Any], market_metrics: dict[str, float | bool], config: ValidationConfig) -> dict[str, Any]:
    zone_score = _normalize_component_score(setup.get("zone_score", setup.get("zone_selection_score", setup.get("validation_score", setup.get("score", 0.0)))))
    freshness_score = _normalize_component_score(setup.get("freshness_score", setup.get("freshness", 100.0 if _safe_float(setup.get("touch_count", 0.0)) == 0 else 60.0)))
    move_away_score = _normalize_component_score(setup.get("move_away_score", setup.get("move_away_strength", setup.get("departure_atr", 0.0))))
    base_quality_score = _normalize_component_score(setup.get("base_quality_score", setup.get("cleanliness_score", setup.get("base_tightness_score", zone_score))))
    retest_quality_score = _normalize_component_score(setup.get("retest_quality", setup.get("retest_score", setup.get("reaction_score", 0.0))))
    rejection_strength = _normalize_component_score(setup.get("rejection_strength", setup.get("rejection_score", 0.0)))
    structure_clarity = _normalize_component_score(setup.get("structure_clarity", setup.get("structure_score", max(0.0, 10.0 - float(market_metrics["chop_score"])))) )
    vwap_alignment = _safe_bool(setup.get("vwap_alignment"), True)
    trend_alignment = _safe_bool(setup.get("trend_alignment", setup.get("trend_ok")), True)
    volatility_ok = _safe_bool(setup.get("volatility_ok"), float(market_metrics["atr_pct"]) >= config.min_volatility_pct)
    chop_ok = _safe_bool(setup.get("chop_ok"), float(market_metrics["chop_score"]) <= config.max_chop_score)
    retest_confirmed = _safe_bool(setup.get("retest_confirmed"), retest_quality_score >= config.min_retest_quality_score)

    fresh_zone = freshness_score >= config.min_freshness_score
    strong_move_away = move_away_score >= config.min_move_away_score
    clean_base = base_quality_score >= config.min_base_quality_score
    retest_clean = retest_quality_score >= config.min_retest_quality_score and retest_confirmed
    rejection_strong = rejection_strength >= config.min_rejection_strength
    structure_clean = structure_clarity >= config.min_structure_clarity
    strict_validation_score = 0
    if fresh_zone:
        strict_validation_score += 2
    if strong_move_away:
        strict_validation_score += 2
    if clean_base:
        strict_validation_score += 2
    if retest_clean:
        strict_validation_score += 2
    if rejection_strong:
        strict_validation_score += 1
    if structure_clean:
        strict_validation_score += 1

    quality_input = {
        "zone_score": zone_score,
        "freshness_score": freshness_score,
        "move_away_score": move_away_score,
        "rejection_strength": rejection_strength,
        "structure_clarity": structure_clarity,
        "retest_confirmed": retest_confirmed,
        "vwap_alignment": vwap_alignment,
        "trend_ok": trend_alignment,
        "volatility_ok": volatility_ok,
        "chop_ok": chop_ok,
    }
    setup_quality_score = compute_setup_quality_score(quality_input)
    return {
        "zone_score": zone_score,
        "freshness_score": freshness_score,
        "move_away_score": move_away_score,
        "base_quality_score": base_quality_score,
        "retest_quality_score": retest_quality_score,
        "rejection_strength": rejection_strength,
        "structure_clarity": structure_clarity,
        "vwap_alignment": vwap_alignment,
        "trend_alignment": trend_alignment,
        "volatility_ok": volatility_ok,
        "chop_ok": chop_ok,
        "retest_confirmed": retest_confirmed,
        "fresh_zone": fresh_zone,
        "strong_move_away": strong_move_away,
        "clean_base": clean_base,
        "retest_clean": retest_clean,
        "rejection_strong": rejection_strong,
        "structure_clean": structure_clean,
        "strict_validation_score": strict_validation_score,
        "setup_quality_score": round(setup_quality_score, 4),
    }


def _build_rejection_reasons(scorecard: dict[str, Any], rr_ratio: float, config: ValidationConfig) -> list[str]:
    reasons: list[str] = []
    if rr_ratio < config.rr_threshold:
        reasons.append("bad_rr")
    if config.require_vwap_alignment and not bool(scorecard["vwap_alignment"]):
        reasons.append("no_vwap_alignment")
    if config.require_trend_alignment and not bool(scorecard["trend_alignment"]):
        reasons.append("trend_misaligned")
    if not bool(scorecard["volatility_ok"]):
        reasons.append("low_volatility")
    if not bool(scorecard["chop_ok"]):
        reasons.append("chop_market_fail")
    if not bool(scorecard["fresh_zone"]):
        reasons.append("stale_zone")
    if not bool(scorecard["strong_move_away"]):
        reasons.append("weak_move_away")
    if not bool(scorecard["clean_base"]):
        reasons.append("dirty_zone_base")
    if not bool(scorecard["retest_confirmed"]):
        reasons.append("retest_not_confirmed")
    if not bool(scorecard["retest_clean"]):
        reasons.append("weak_retest_quality")
    if not bool(scorecard["rejection_strong"]):
        reasons.append("rejection_candle_weak")
    if not bool(scorecard["structure_clean"]):
        reasons.append("weak_structure_clarity")
    if float(scorecard["zone_score"]) < config.min_zone_score:
        reasons.append("weak_zone_score")
    if int(scorecard["strict_validation_score"]) < int(config.min_score):
        reasons.append("low_validation_score")
    deduped: list[str] = []
    for reason in reasons:
        if reason not in deduped:
            deduped.append(reason)
    return deduped


def _build_rejection_log(setup: dict[str, Any], strategy: str, symbol: str, reasons: list[str], scorecard: dict[str, Any], rr_ratio: float) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "strategy": strategy,
        "trade_id": str(setup.get("trade_id", "") or ""),
        "zone_id": str(setup.get("zone_id", "") or ""),
        "timestamp": str(setup.get("timestamp", setup.get("signal_time", setup.get("entry_time", ""))) or ""),
        "rejection_reason": ", ".join(reasons),
        "validation_reasons": list(reasons),
        "strict_validation_score": int(scorecard.get("strict_validation_score", 0)),
        "setup_quality_score": float(scorecard.get("setup_quality_score", 0.0)),
        "rr_ratio": round(rr_ratio, 4),
        "scorecard": dict(scorecard),
    }


def validate_trade(setup: dict[str, Any], candles: pd.DataFrame, config: ValidationConfig | None = None, *, cleaned_candles: pd.DataFrame | None = None, market_metrics: dict[str, float | bool] | None = None) -> dict[str, Any]:
    cfg = config or ValidationConfig()
    symbol = str(dict(setup or {}).get("symbol", "") or "")
    strategy = str(dict(setup or {}).get("strategy_name", dict(setup or {}).get("strategy", "")) or "")
    try:
        cleaned = cleaned_candles
        if cleaned is None:
            cleaned = coerce_ohlcv(
                candles,
                CleanerConfig(expected_interval_minutes=cfg.expected_interval_minutes, require_vwap=True, allow_vwap_compute=True),
            )
        resolved_market_metrics = dict(market_metrics or {}) if isinstance(market_metrics, dict) else {}
        if not resolved_market_metrics:
            resolved_market_metrics = _latest_metrics(cleaned)
        side = _side(setup)
        entry = _safe_float(setup.get("entry", setup.get("entry_price", setup.get("price"))))
        stop = _safe_float(setup.get("stoploss", setup.get("stop_loss", setup.get("sl"))))
        target = _safe_float(setup.get("target", setup.get("target_price", setup.get("tp"))))
        rr_ratio = 0.0
        if entry > 0 and stop > 0 and target > 0:
            risk = abs(entry - stop)
            reward = abs(target - entry)
            rr_ratio = reward / risk if risk > 1e-6 else 0.0
        scorecard = _build_scorecard(dict(setup or {}), resolved_market_metrics, cfg)
        scorecard["side"] = side
        scorecard["rr_ratio"] = round(rr_ratio, 4)
        scorecard["atr_pct"] = round(_safe_float(resolved_market_metrics.get("atr_pct")), 4)
        scorecard["chop_score"] = round(_safe_float(resolved_market_metrics.get("chop_score")), 4)
        reasons = _build_rejection_reasons(scorecard, rr_ratio, cfg)
        score = max(float(scorecard["strict_validation_score"]), round(float(scorecard["setup_quality_score"]) / 10.0, 2))
        rejection_log = _build_rejection_log(dict(setup or {}), strategy, symbol, reasons, scorecard, rr_ratio)
        payload = {
            "decision": "PASS" if not reasons and score >= cfg.min_score else "FAIL",
            "score": round(score, 2),
            "reasons": reasons,
            "metrics": {**resolved_market_metrics, **scorecard},
            "rejection_log": rejection_log,
        }
        if payload["decision"] != "PASS":
            increment_metric("trade_validation_failures_total", 1)
        set_metric("last_validation_status", payload["decision"])
        record_stage("validation", status="SUCCESS" if payload["decision"] == "PASS" else "WARN", symbol=symbol, strategy=strategy, message="Trade validation completed")
        log_event(
            component="validation_engine",
            event_name="trade_validation",
            symbol=symbol,
            strategy=strategy,
            severity="INFO" if payload["decision"] == "PASS" else "WARNING",
            message="Trade validation completed",
            context_json=payload,
        )
        return payload
    except (OHLCVValidationError, TypeError, ValueError) as exc:
        increment_metric("schema_validation_failures_total", 1)
        increment_metric("trade_validation_failures_total", 1)
        record_stage("validation", status="FAIL", symbol=symbol, strategy=strategy, message=str(exc))
        log_exception(component="validation_engine", event_name="trade_validation_failed", exc=exc, symbol=symbol, strategy=strategy, message="Trade validation failed")
        return {
            "decision": "FAIL",
            "score": 0.0,
            "reasons": ["invalid_input"],
            "metrics": {"error": str(exc)},
            "rejection_log": _build_rejection_log(dict(setup or {}), strategy, symbol, ["invalid_input"], {"strict_validation_score": 0, "setup_quality_score": 0.0}, 0.0),
        }

__all__ = ["ValidationConfig", "validate_trade"]

