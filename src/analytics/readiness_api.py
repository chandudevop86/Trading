from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from src.analytics.metrics import compute_trade_metrics, evaluate_production_readiness


def summarize_validation_failures(rejects_df: Any) -> dict[str, int]:
    frame = rejects_df.copy() if isinstance(rejects_df, pd.DataFrame) else pd.DataFrame(rejects_df or [])
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
    cfg = dict(config or {})
    trades = trades_df.copy() if isinstance(trades_df, pd.DataFrame) else pd.DataFrame(trades_df or [])
    rejects = rejects_df.copy() if isinstance(rejects_df, pd.DataFrame) else pd.DataFrame(rejects_df or [])

    metrics = compute_trade_metrics(trades)
    total_candidates = int(len(trades) + len(rejects))
    total_executed = int(len(trades[trades.get("execution_status", pd.Series([], dtype=object)).astype(str).str.upper().isin(["EXECUTED", "FILLED", "SENT", "OPEN"])])) if not trades.empty and "execution_status" in trades.columns else int(len(trades))
    total_passed = int(len(trades[trades.get("validation_status", pd.Series([], dtype=object)).astype(str).str.upper() == "PASS"])) if not trades.empty and "validation_status" in trades.columns else int(len(trades))
    total_rejected = int(len(rejects))
    top_rejection_reasons = summarize_validation_failures(rejects)
    readiness_decision = evaluate_production_readiness(metrics)

    reasons: list[str] = []
    min_trades = int(cfg.get("min_trades", 100) or 100)
    min_profit_factor = float(cfg.get("min_profit_factor", 1.3) or 1.3)
    max_drawdown = float(cfg.get("max_drawdown", 10.0) or 10.0)
    if int(metrics.get("total_trades", 0)) < min_trades:
        reasons.append("INSUFFICIENT_TRADE_SAMPLE")
    if float(metrics.get("expectancy", 0.0)) <= 0:
        reasons.append("NON_POSITIVE_EXPECTANCY")
    if float(metrics.get("profit_factor", 0.0)) < min_profit_factor:
        reasons.append("PROFIT_FACTOR_TOO_LOW")
    if float(metrics.get("max_drawdown", 0.0)) > max_drawdown:
        reasons.append("DRAWDOWN_TOO_HIGH")
    if not bool(metrics.get("duplicate_prevention_proven", False)):
        reasons.append("DUPLICATE_PREVENTION_NOT_PROVEN")
    if readiness_decision == "READY":
        readiness_decision = "PAPER_ONLY"
        reasons.append("LIVE_EVIDENCE_NOT_ENABLED_IN_PAPER_REVIEW")

    total_candidates = max(total_candidates, int(metrics.get("total_trades", 0)) + total_rejected)
    validation_pass_rate = (total_passed / total_candidates * 100.0) if total_candidates > 0 else 0.0

    return {
        "total_candidates": total_candidates,
        "total_passed": total_passed,
        "total_rejected": total_rejected,
        "total_executed": total_executed,
        "validation_pass_rate": round(validation_pass_rate, 2),
        "win_rate": metrics.get("win_rate", 0.0),
        "expectancy": metrics.get("expectancy", 0.0),
        "profit_factor": metrics.get("profit_factor", 0.0),
        "max_drawdown": metrics.get("max_drawdown", 0.0),
        "avg_r_multiple": metrics.get("avg_r_multiple", 0.0),
        "duplicate_prevention_proven": bool(metrics.get("duplicate_prevention_proven", False)),
        "top_rejection_reasons": top_rejection_reasons,
        "readiness_decision": readiness_decision,
        "not_ready_reasons": reasons,
    }


__all__ = ["evaluate_readiness", "summarize_validation_failures"]
