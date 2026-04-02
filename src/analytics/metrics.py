from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_frame(rows: Any) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(rows or [])


def compute_trade_metrics(rows: Any) -> dict[str, Any]:
    frame = _coerce_frame(rows)
    if frame.empty:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "expectancy": 0.0,
            "profit_factor": 0.0,
            "max_drawdown": 0.0,
            "avg_r_multiple": 0.0,
            "validation_pass_rate": 0.0,
            "rejection_reasons_count": {},
            "duplicate_prevention_proven": False,
        }

    pnl = pd.to_numeric(frame.get("pnl", pd.Series([0.0] * len(frame))), errors="coerce").fillna(0.0)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    total_trades = int(len(frame))
    win_rate = float((pnl > 0).mean() * 100.0)
    avg_win = float(wins.mean()) if not wins.empty else 0.0
    avg_loss = abs(float(losses.mean())) if not losses.empty else 0.0
    loss_rate = 1.0 - float((pnl > 0).mean())
    expectancy = float(((pnl > 0).mean() * avg_win) - (loss_rate * avg_loss))
    profit_factor = float(wins.sum() / abs(losses.sum())) if not losses.empty and abs(float(losses.sum())) > 1e-9 else float("inf") if not wins.empty else 0.0

    equity_curve = pnl.cumsum()
    rolling_peak = equity_curve.cummax()
    drawdown = equity_curve - rolling_peak
    max_drawdown = abs(float(drawdown.min())) if not drawdown.empty else 0.0

    risk_per_trade = pd.to_numeric(frame.get("risk_per_unit", pd.Series([0.0] * len(frame))), errors="coerce").fillna(0.0)
    quantity = pd.to_numeric(frame.get("quantity", pd.Series([0.0] * len(frame))), errors="coerce").fillna(0.0)
    risk_cash = (risk_per_trade * quantity).replace(0.0, pd.NA)
    r_multiple = (pnl / risk_cash).fillna(0.0)
    avg_r_multiple = float(r_multiple.mean()) if not r_multiple.empty else 0.0

    validation_status = frame.get("validation_status", pd.Series([""] * len(frame)))
    validation_pass_rate = float((validation_status.astype(str).str.upper() == "PASS").mean() * 100.0)

    reason_counter: Counter[str] = Counter()
    if "validation_reasons" in frame.columns:
        for value in frame["validation_reasons"].tolist():
            if isinstance(value, list):
                reason_counter.update(str(item) for item in value if str(item).strip())
            elif isinstance(value, str) and value.strip():
                parts = [part.strip() for part in value.split(",") if part.strip()]
                reason_counter.update(parts)
    duplicate_prevention_proven = not frame.get("duplicate_reason", pd.Series([""] * len(frame))).astype(str).str.contains("DUPLICATE", case=False, na=False).any()

    return {
        "total_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "expectancy": round(expectancy, 2),
        "profit_factor": round(profit_factor if profit_factor != float("inf") else 999.0, 2),
        "max_drawdown": round(max_drawdown, 2),
        "avg_r_multiple": round(avg_r_multiple, 2),
        "validation_pass_rate": round(validation_pass_rate, 2),
        "rejection_reasons_count": dict(reason_counter),
        "duplicate_prevention_proven": bool(duplicate_prevention_proven),
    }


def evaluate_production_readiness(metrics: dict[str, Any]) -> str:
    total_trades = int(metrics.get("total_trades", 0) or 0)
    expectancy = _safe_float(metrics.get("expectancy", 0.0))
    profit_factor = _safe_float(metrics.get("profit_factor", 0.0))
    max_drawdown = _safe_float(metrics.get("max_drawdown", 0.0))
    validation_pass_rate = _safe_float(metrics.get("validation_pass_rate", 0.0))
    duplicate_prevention_proven = bool(metrics.get("duplicate_prevention_proven", False))

    if total_trades < 100:
        return "NOT_READY"
    if expectancy <= 0:
        return "NOT_READY"
    if profit_factor < 1.3:
        return "NOT_READY"
    if max_drawdown > 10.0:
        return "NOT_READY"
    if not duplicate_prevention_proven:
        return "NOT_READY"
    if validation_pass_rate < 55.0:
        return "PAPER_ONLY"
    if profit_factor >= 1.6 and expectancy > 0 and max_drawdown <= 6.0:
        return "READY"
    return "PAPER_ONLY"


__all__ = ["compute_trade_metrics", "evaluate_production_readiness"]
