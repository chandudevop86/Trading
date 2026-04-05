from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from src.analytics.metrics import compute_trade_metrics, evaluate_production_readiness


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


def _reason_count(value: Any) -> int:
    if isinstance(value, list):
        return sum(1 for item in value if str(item).strip())
    if isinstance(value, str):
        return sum(1 for item in value.split(",") if item.strip())
    return 0


def _clean_trade_frame(rows: Any) -> pd.DataFrame:
    trades = _coerce_frame(rows)
    if trades.empty:
        return trades.copy()

    exit_time = pd.to_datetime(trades.get("exit_time", pd.Series([None] * len(trades), index=trades.index)), errors="coerce", utc=True)
    execution_status = trades.get("execution_status", pd.Series([""] * len(trades), index=trades.index)).fillna("").astype(str).str.upper()
    trade_status = trades.get("trade_status", pd.Series([""] * len(trades), index=trades.index)).fillna("").astype(str).str.upper()
    validation_status = trades.get("validation_status", pd.Series([""] * len(trades), index=trades.index)).fillna("").astype(str).str.upper()
    rejection_reason = trades.get("rejection_reason", pd.Series([""] * len(trades), index=trades.index)).fillna("").astype(str).str.strip()
    rejection_count = trades.get("validation_reasons", pd.Series([[]] * len(trades), index=trades.index)).apply(_reason_count)
    duplicate_blocked = trades.get("duplicate_blocked", pd.Series([False] * len(trades), index=trades.index)).fillna(False).astype(bool)
    strict_validation_score = pd.to_numeric(trades.get("strict_validation_score", pd.Series([None] * len(trades), index=trades.index)), errors="coerce")

    closed_mask = exit_time.notna() | execution_status.isin(["CLOSED", "EXITED", "FILLED"]) | trade_status.isin(["CLOSED", "EXITED"])
    validation_mask = validation_status.eq("PASS") | validation_status.eq("")
    rejection_mask = rejection_reason.eq("") & rejection_count.eq(0)
    strict_mask = strict_validation_score.isna() | strict_validation_score.ge(7)
    execution_mask = ~execution_status.isin(["REJECTED", "BLOCKED", "ERROR", "CANCELLED"])

    keep_mask = closed_mask & validation_mask & rejection_mask & strict_mask & execution_mask & (~duplicate_blocked)
    return trades.loc[keep_mask].copy().reset_index(drop=True)


def summarize_validation_failures(rejects_df: Any) -> dict[str, int]:
    frame = _coerce_frame(rejects_df)
    counter: Counter[str] = Counter()
    if frame.empty:
        return {}
    for column in ("reasons", "validation_reasons", "reason_codes", "blocked_reason", "rejection_reason"):
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
    clean_trades = _clean_trade_frame(trades)

    metrics = compute_trade_metrics(clean_trades)
    top_rejection_reasons = summarize_validation_failures(rejects)
    raw_verdict = evaluate_production_readiness(metrics)

    total_candidates = int(len(trades) + len(rejects))
    total_rejected = int(len(rejects))
    if not trades.empty and "validation_status" in trades.columns:
        total_passed = int(len(trades[trades["validation_status"].astype(str).str.upper() == "PASS"]))
    else:
        total_passed = int(len(clean_trades))
    total_executed = int(len(clean_trades))
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

    edge_report = {
        "clean_trade_count": int(len(clean_trades)),
        "expectancy": metrics.get("expectancy", 0.0),
        "profit_factor": metrics.get("profit_factor", 0.0),
        "max_drawdown": metrics.get("max_drawdown", 0.0),
        "win_rate": metrics.get("win_rate", 0.0),
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
            "clean_trade_count": int(len(clean_trades)),
        },
        "metrics": {
            "validation_pass_rate": validation_pass_rate,
            "win_rate": metrics.get("win_rate", 0.0),
            "expectancy": metrics.get("expectancy", 0.0),
            "profit_factor": metrics.get("profit_factor", 0.0),
            "max_drawdown": metrics.get("max_drawdown", 0.0),
            "avg_r_multiple": metrics.get("avg_r_multiple", 0.0),
            "duplicate_prevention_proven": bool(metrics.get("duplicate_prevention_proven", False)),
            "clean_trade_count": int(len(clean_trades)),
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
        "clean_trade_count": int(len(clean_trades)),
        "clean_trade_metrics_only": True,
        "edge_proof_status": verdict,
        "readiness_summary": f"Readiness is based on {len(clean_trades)} clean trades only; rejected candidates are tracked separately.",
        "edge_report": edge_report,
    }


__all__ = ["DEFAULT_READINESS_THRESHOLDS", "evaluate_readiness", "summarize_validation_failures"]
