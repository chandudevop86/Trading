from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric
from src.data.cleaner import CleanerConfig, OHLCVValidationError, coerce_ohlcv
from src.strict_zone_validation import StrictValidationConfig, validate_zone_candidate


@dataclass(slots=True)
class ValidationConfig:
    rr_threshold: float = 2.0
    min_score: float = 7.0
    expected_interval_minutes: int = 5
    require_vwap_alignment: bool = True
    require_trend_alignment: bool = True
    max_chop_score: float = 6.0
    min_volatility_pct: float = 0.15


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _side(setup: dict[str, Any]) -> str:
    return str(setup.get("side", setup.get("type", "")) or "").strip().upper()


def _latest_metrics(frame: pd.DataFrame) -> dict[str, float | bool]:
    candle = frame.iloc[-1]
    close = float(candle["close"])
    ema_fast = frame["close"].ewm(span=5, adjust=False).mean().iloc[-1]
    ema_slow = frame["close"].ewm(span=13, adjust=False).mean().iloc[-1]
    tr = pd.concat([
        frame["high"] - frame["low"],
        (frame["high"] - frame["close"].shift(1)).abs(),
        (frame["low"] - frame["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = float(tr.rolling(14, min_periods=1).mean().iloc[-1])
    atr_pct = (atr / close) * 100.0 if close > 0 else 0.0
    overlap_values = []
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


def _generic_validate(setup: dict[str, Any], candles: pd.DataFrame, config: ValidationConfig) -> dict[str, Any]:
    cleaned = coerce_ohlcv(candles, CleanerConfig(expected_interval_minutes=config.expected_interval_minutes, require_vwap=True))
    metrics = _latest_metrics(cleaned)
    reasons: list[str] = []
    side = _side(setup)
    entry = _safe_float(setup.get("entry", setup.get("entry_price", setup.get("price"))))
    stop = _safe_float(setup.get("stoploss", setup.get("stop_loss", setup.get("sl"))))
    target = _safe_float(setup.get("target", setup.get("target_price", setup.get("tp"))))
    rr_ratio = 0.0
    if entry > 0 and stop > 0 and target > 0:
        risk = abs(entry - stop)
        reward = abs(target - entry)
        rr_ratio = reward / risk if risk > 1e-6 else 0.0
    metrics["rr_ratio"] = round(rr_ratio, 4)
    metrics["zone_score"] = round(_safe_float(setup.get("zone_score", setup.get("validation_score", setup.get("score", 0.0)))), 4)
    metrics["retest_quality"] = round(_safe_float(setup.get("retest_quality", setup.get("retest_score", setup.get("reaction_score", 0.0)))), 4)
    metrics["rejection_strength"] = round(_safe_float(setup.get("rejection_strength", setup.get("rejection_score", 0.0))), 4)
    metrics["structure_clarity"] = round(_safe_float(setup.get("structure_clarity", setup.get("structure_score", max(0.0, 10.0 - float(metrics["chop_score"]))))), 4)
    metrics["move_away_strength"] = round(_safe_float(setup.get("move_away_strength", setup.get("departure_atr", 0.0))), 4)
    metrics["freshness"] = round(_safe_float(setup.get("freshness", 10.0 if _safe_float(setup.get("touch_count", 0.0)) == 0 else 6.0)), 4)

    vwap_alignment = True
    trend_alignment = True
    if side == "BUY":
        vwap_alignment = float(metrics["close"]) >= float(metrics["vwap"])
        trend_alignment = float(metrics["ema_fast"]) >= float(metrics["ema_slow"])
    elif side == "SELL":
        vwap_alignment = float(metrics["close"]) <= float(metrics["vwap"])
        trend_alignment = float(metrics["ema_fast"]) <= float(metrics["ema_slow"])
    metrics["vwap_alignment"] = vwap_alignment
    metrics["trend_alignment"] = trend_alignment

    if rr_ratio < config.rr_threshold:
        reasons.append("bad_rr")
    if config.require_vwap_alignment and not vwap_alignment:
        reasons.append("no_vwap_alignment")
    if config.require_trend_alignment and not trend_alignment:
        reasons.append("trend_misaligned")
    if float(metrics["atr_pct"]) < config.min_volatility_pct:
        reasons.append("low_volatility")
    if float(metrics["chop_score"]) > config.max_chop_score:
        reasons.append("chop_market_fail")
    if float(metrics["structure_clarity"]) < 6.0:
        reasons.append("weak_structure_clarity")

    score = (
        0.20 * float(metrics["zone_score"]) +
        0.15 * float(metrics["freshness"]) +
        0.20 * float(metrics["retest_quality"]) +
        0.15 * float(metrics["rejection_strength"]) +
        0.15 * float(metrics["structure_clarity"]) +
        0.15 * (10.0 if vwap_alignment and trend_alignment else 4.0 if (vwap_alignment or trend_alignment) else 0.0)
    )
    decision = "PASS" if not reasons and score >= config.min_score else "FAIL"
    return {
        "decision": decision,
        "score": round(score, 2),
        "reasons": reasons,
        "metrics": metrics,
    }


def validate_trade(setup: dict[str, Any], candles: pd.DataFrame, config: ValidationConfig | None = None) -> dict[str, Any]:
    """Validate a candidate setup against cleaned candles and return an execution gate."""
    cfg = config or ValidationConfig()
    symbol = str(dict(setup or {}).get('symbol', '') or '')
    strategy = str(dict(setup or {}).get('strategy_name', dict(setup or {}).get('strategy', '')) or '')
    try:
        if isinstance(setup, dict) and {"zone_id", "zone_type", "zone_low", "zone_high"}.issubset(set(setup.keys())):
            higher_tf = setup.get("higher_tf_candles")
            if isinstance(higher_tf, pd.DataFrame):
                zone_cfg = StrictValidationConfig(
                    min_rr=cfg.rr_threshold,
                    min_validation_score=cfg.min_score,
                    require_vwap_alignment=cfg.require_vwap_alignment,
                    entry_timeframe_minutes=cfg.expected_interval_minutes,
                )
                result = validate_zone_candidate(dict(setup), candles, higher_tf, zone_cfg)
                payload = {
                    "decision": str(result.get("status", "FAIL")).upper(),
                    "score": float(result.get("validation_score", 0.0)),
                    "reasons": list(result.get("fail_reasons", []) or []),
                    "metrics": dict(result.get("metrics", {}) or {}),
                }
                if payload['decision'] != 'PASS':
                    increment_metric('trade_validation_failures_total', 1)
                record_stage('validation', status='SUCCESS' if payload['decision'] == 'PASS' else 'WARN', symbol=symbol, strategy=strategy, message='Zone candidate validated')
                log_event(component='validation_engine', event_name='trade_validation', symbol=symbol, strategy=strategy, severity='INFO' if payload['decision'] == 'PASS' else 'WARNING', message='Zone validation completed', context_json=payload)
                return payload
        payload = _generic_validate(dict(setup), candles, cfg)
        if payload['decision'] != 'PASS':
            increment_metric('trade_validation_failures_total', 1)
        set_metric('last_validation_status', payload['decision'])
        record_stage('validation', status='SUCCESS' if payload['decision'] == 'PASS' else 'WARN', symbol=symbol, strategy=strategy, message='Trade validation completed')
        log_event(component='validation_engine', event_name='trade_validation', symbol=symbol, strategy=strategy, severity='INFO' if payload['decision'] == 'PASS' else 'WARNING', message='Trade validation completed', context_json=payload)
        return payload
    except (OHLCVValidationError, TypeError, ValueError) as exc:
        increment_metric('schema_validation_failures_total', 1)
        increment_metric('trade_validation_failures_total', 1)
        record_stage('validation', status='FAIL', symbol=symbol, strategy=strategy, message=str(exc))
        log_exception(component='validation_engine', event_name='trade_validation_failed', exc=exc, symbol=symbol, strategy=strategy, message='Trade validation failed')
        return {
            "decision": "FAIL",
            "score": 0.0,
            "reasons": ["invalid_input"],
            "metrics": {"error": str(exc)},
        }


__all__ = ["ValidationConfig", "validate_trade"]
