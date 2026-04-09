from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from vinayak.analytics.metrics import compute_trade_metrics, evaluate_production_readiness


DEFAULT_READINESS_THRESHOLDS = {
    "min_trades": 100,
    "min_expectancy": 0.0,
    "min_profit_factor": 1.3,
    "max_drawdown": 10.0,
    "min_validation_pass_rate": 55.0,
    "ready_profit_factor": 1.6,
    "ready_max_drawdown": 6.0,
}


def _coerce_frame(rows: Any) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(rows or [])


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def summarize_validation_failures(rejects_df: Any) -> dict[str, int]:
    frame = _coerce_frame(rejects_df)
    counter: Counter[str] = Counter()
    if frame.empty:
        return {}
    for column in ("reasons", "validation_reasons", "reason_codes", "blocked_reason"):
        if column not in frame.columns:
            continue
        for value in frame[column].tolist():
            if isinstance(value, list):
                counter.update(str(item) for item in value if str(item).strip())
            elif isinstance(value, str) and value.strip():
                counter.update(part.strip() for part in value.split(",") if part.strip())
    return dict(counter)


def evaluate_readiness(trades_df: Any, rejects_df: Any, config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = {**DEFAULT_READINESS_THRESHOLDS, **dict(config or {})}
    trades = _coerce_frame(trades_df)
    rejects = _coerce_frame(rejects_df)

    metrics = compute_trade_metrics(trades)
    top_rejection_reasons = summarize_validation_failures(rejects)
    raw_verdict = evaluate_production_readiness(metrics)

    total_candidates = int(len(trades) + len(rejects))
    total_rejected = int(len(rejects))
    if not trades.empty and "validation_status" in trades.columns:
        total_passed = int(len(trades[trades["validation_status"].astype(str).str.upper() == "PASS"]))
    else:
        total_passed = int(len(trades))
    if not trades.empty and "execution_status" in trades.columns:
        total_executed = int(
            len(trades[trades["execution_status"].astype(str).str.upper().isin(["EXECUTED", "FILLED", "SENT", "OPEN", "CLOSED"])])
        )
    else:
        total_executed = int(len(trades))

    total_candidates = max(total_candidates, int(metrics.get("total_trades", 0)) + total_rejected)
    validation_pass_rate = round((total_passed / total_candidates * 100.0), 2) if total_candidates > 0 else 0.0

    failure_counts = {
        "insufficient_trade_sample": 1 if int(metrics.get("total_trades", 0)) < int(cfg["min_trades"]) else 0,
        "non_positive_expectancy": 1 if _safe_float(metrics.get("expectancy", 0.0)) <= float(cfg["min_expectancy"]) else 0,
        "profit_factor_too_low": 1 if _safe_float(metrics.get("profit_factor", 0.0)) < float(cfg["min_profit_factor"]) else 0,
        "drawdown_too_high": 1 if _safe_float(metrics.get("max_drawdown", 0.0)) > float(cfg["max_drawdown"]) else 0,
        "duplicate_prevention_not_proven": 1 if not bool(metrics.get("duplicate_prevention_proven", False)) else 0,
        "validation_pass_rate_too_low": 1 if validation_pass_rate < float(cfg["min_validation_pass_rate"]) else 0,
        "live_evidence_not_enabled": 1 if raw_verdict == "READY" else 0,
    }

    reasons: list[str] = []
    if failure_counts["insufficient_trade_sample"]:
        reasons.append("INSUFFICIENT_TRADE_SAMPLE")
    if failure_counts["non_positive_expectancy"]:
        reasons.append("NON_POSITIVE_EXPECTANCY")
    if failure_counts["profit_factor_too_low"]:
        reasons.append("PROFIT_FACTOR_TOO_LOW")
    if failure_counts["drawdown_too_high"]:
        reasons.append("DRAWDOWN_TOO_HIGH")
    if failure_counts["duplicate_prevention_not_proven"]:
        reasons.append("DUPLICATE_PREVENTION_NOT_PROVEN")
    if failure_counts["validation_pass_rate_too_low"]:
        reasons.append("VALIDATION_PASS_RATE_TOO_LOW")

    verdict = raw_verdict
    if raw_verdict == "READY":
        verdict = "PAPER_ONLY"
        reasons.append("LIVE_EVIDENCE_NOT_ENABLED_IN_PAPER_REVIEW")

    threshold_status = {
        "min_trades": int(metrics.get("total_trades", 0)) >= int(cfg["min_trades"]),
        "min_expectancy": _safe_float(metrics.get("expectancy", 0.0)) > float(cfg["min_expectancy"]),
        "min_profit_factor": _safe_float(metrics.get("profit_factor", 0.0)) >= float(cfg["min_profit_factor"]),
        "max_drawdown": _safe_float(metrics.get("max_drawdown", 0.0)) <= float(cfg["max_drawdown"]),
        "min_validation_pass_rate": validation_pass_rate >= float(cfg["min_validation_pass_rate"]),
        "duplicate_prevention_proven": bool(metrics.get("duplicate_prevention_proven", False)),
    }

    return {
        "verdict": verdict,
        "readiness_decision": verdict,
        "thresholds": dict(cfg),
        "threshold_status": threshold_status,
        "reasons": reasons,
        "not_ready_reasons": reasons,
        "failure_counts": failure_counts,
        "totals": {
            "total_candidates": total_candidates,
            "total_passed": total_passed,
            "total_rejected": total_rejected,
            "total_executed": total_executed,
        },
        "metrics": {
            "validation_pass_rate": validation_pass_rate,
            "win_rate": metrics.get("win_rate", 0.0),
            "expectancy": metrics.get("expectancy", 0.0),
            "profit_factor": metrics.get("profit_factor", 0.0),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "avg_r_multiple": metrics.get("avg_r_multiple", 0.0),
            "duplicate_prevention_proven": bool(metrics.get("duplicate_prevention_proven", False)),
        },
        "top_rejection_reasons": top_rejection_reasons,
        "validation_failure_summary": top_rejection_reasons,
        "total_candidates": total_candidates,
        "total_passed": total_passed,
        "total_rejected": total_rejected,
        "total_executed": total_executed,
        "validation_pass_rate": validation_pass_rate,
        "win_rate": metrics.get("win_rate", 0.0),
        "expectancy": metrics.get("expectancy", 0.0),
        "profit_factor": metrics.get("profit_factor", 0.0),
        "max_drawdown": metrics.get("max_drawdown", 0.0),
        "avg_r_multiple": metrics.get("avg_r_multiple", 0.0),
        "duplicate_prevention_proven": bool(metrics.get("duplicate_prevention_proven", False)),
    }


__all__ = ["DEFAULT_READINESS_THRESHOLDS", "evaluate_readiness", "summarize_validation_failures"]


