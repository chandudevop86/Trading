from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

import pandas as pd

from vinayak.analytics.metrics import compute_trade_metrics, evaluate_production_readiness
from vinayak.core.statuses import ValidationStatus
from vinayak.strategies.demand_supply.candidate_generation import generate_supply_demand_trade_candidates
from vinayak.strategies.demand_supply.models import SupplyDemandStrategyConfig, SupplyDemandZone
from vinayak.strategies.demand_supply.normalization import safe_float


def evaluate_supply_demand_readiness(
    trades: Sequence[dict[str, Any]],
    rejected_rows: Sequence[dict[str, Any]],
    *,
    min_trades: int = 100,
    max_drawdown_threshold: float = 10.0,
) -> dict[str, Any]:
    metrics = compute_trade_metrics(list(trades))
    rejection_counts = Counter()
    invalid_schema_count = 0
    for row in rejected_rows:
        reasons = row.get("reasons", [])
        if isinstance(reasons, str):
            reasons = [part.strip() for part in reasons.split(",") if part.strip()]
        for reason in reasons:
            rejection_counts[str(reason)] += 1
        if "invalid_schema" in reasons:
            invalid_schema_count += 1
    readiness = evaluate_production_readiness(metrics)
    duplicate_trade_rate = 0.0 if not trades else round(sum(1 for row in trades if str(row.get("duplicate_reason", "")).strip()) / len(trades), 4)
    passed = (
        int(metrics.get("total_trades", 0)) >= int(min_trades)
        and float(metrics.get("expectancy", 0.0)) > 0.0
        and float(metrics.get("profit_factor", 0.0)) > 1.3
        and float(metrics.get("max_drawdown", 0.0)) <= float(max_drawdown_threshold)
        and duplicate_trade_rate == 0.0
        and invalid_schema_count == 0
    )
    return {
        "status": ValidationStatus.PASS if passed else ValidationStatus.FAIL,
        "readiness": readiness,
        "total_trades": int(metrics.get("total_trades", 0)),
        "win_rate": float(metrics.get("win_rate", 0.0)),
        "average_rr": float(metrics.get("avg_r_multiple", 0.0)),
        "expectancy": float(metrics.get("expectancy", 0.0)),
        "profit_factor": float(metrics.get("profit_factor", 0.0)),
        "gross_profit": round(sum(max(safe_float(row.get("pnl", 0.0)), 0.0) for row in trades), 2),
        "gross_loss": round(abs(sum(min(safe_float(row.get("pnl", 0.0)), 0.0) for row in trades)), 2),
        "max_drawdown": float(metrics.get("max_drawdown", 0.0)),
        "average_hold_time": 0.0,
        "rejected_trades_count": int(len(rejected_rows)),
        "validation_fail_counts": dict(rejection_counts),
        "duplicate_trade_rate": duplicate_trade_rate,
        "invalid_schema_count": invalid_schema_count,
        "summary": "PASS: supply and demand strategy meets current paper-readiness thresholds." if passed else "FAIL: supply and demand strategy is still weak or unsafe for paper promotion.",
    }


def summarize_structure_metrics(
    zones: Sequence[SupplyDemandZone],
    trades: Sequence[dict[str, Any]] | None = None,
    rejects: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    structure_names = ("RBR", "DBR", "RBD", "DBD")
    zone_list = list(zones or [])
    trade_list = list(trades or [])
    reject_list = list(rejects or [])
    zone_map = {str(zone.zone_id): str(zone.structure_type).upper() for zone in zone_list}
    summary: dict[str, Any] = {}
    for name in structure_names:
        matching_zones = [zone for zone in zone_list if str(zone.structure_type).upper() == name]
        matching_trades = [row for row in trade_list if str(row.get("setup_type", "")).upper() == name]
        matching_rejects = [row for row in reject_list if zone_map.get(str(row.get("zone_id", "")), "") == name]
        summary[name] = {
            "zones_detected": int(len(matching_zones)),
            "trade_candidates": int(len(matching_trades)),
            "rejections": int(len(matching_rejects)),
            "avg_total_score": round(sum(float(zone.total_score) for zone in matching_zones) / len(matching_zones), 2) if matching_zones else 0.0,
            "avg_strength_score": round(sum(float(zone.strength_score) for zone in matching_zones) / len(matching_zones), 2) if matching_zones else 0.0,
            "avg_freshness_score": round(sum(float(zone.freshness_score) for zone in matching_zones) / len(matching_zones), 2) if matching_zones else 0.0,
            "avg_validation_score": round(sum(float(zone.validation_score) for zone in matching_zones) / len(matching_zones), 2) if matching_zones else 0.0,
        }
    summary["totals"] = {"zones_detected": int(len(zone_list)), "trade_candidates": int(len(trade_list)), "rejections": int(len(reject_list))}
    return summary


def summarize_rejection_analytics(rejects: Sequence[dict[str, Any]] | None = None) -> dict[str, dict[str, int]]:
    reject_list = list(rejects or [])
    analytics = {"by_reason": Counter(), "by_session": Counter(), "by_structure_type": Counter(), "by_zone_type": Counter(), "by_zone_score_bucket": Counter(), "by_validation_score_bucket": Counter()}
    for row in reject_list:
        for reason in row.get("reasons", []) or []:
            analytics["by_reason"][str(reason)] += 1
        analytics["by_session"][str(row.get("session", "UNKNOWN") or "UNKNOWN").upper()] += 1
        analytics["by_structure_type"][str(row.get("structure_type", "UNKNOWN") or "UNKNOWN").upper()] += 1
        analytics["by_zone_type"][str(row.get("zone_type", "UNKNOWN") or "UNKNOWN").upper()] += 1
        analytics["by_zone_score_bucket"][str(row.get("zone_score_bucket", "UNKNOWN") or "UNKNOWN")] += 1
        analytics["by_validation_score_bucket"][str(row.get("validation_score_bucket", "UNKNOWN") or "UNKNOWN")] += 1
    return {name: dict(counter) for name, counter in analytics.items()}


def build_supply_demand_report(
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    existing_trade_rows: Sequence[dict[str, Any]] | None = None,
    executed_trade_rows: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cfg = config or SupplyDemandStrategyConfig()
    trades, rejects, zones = generate_supply_demand_trade_candidates(candles, cfg, existing_trade_rows=existing_trade_rows)
    readiness = evaluate_supply_demand_readiness(executed_trade_rows or trades, rejects)
    structure_metrics = summarize_structure_metrics(zones, trades, rejects)
    rejection_summary = dict(Counter(reason for row in rejects for reason in row.get("reasons", [])))
    rejection_analytics = summarize_rejection_analytics(rejects)
    return {
        "zone_rows": [zone.to_dict() for zone in zones],
        "trade_rows": trades,
        "rejection_summary": rejection_summary,
        "rejection_analytics": rejection_analytics,
        "structure_metrics": structure_metrics,
        "readiness_summary": readiness,
    }


__all__ = [
    "build_supply_demand_report",
    "evaluate_supply_demand_readiness",
    "summarize_rejection_analytics",
    "summarize_structure_metrics",
]
