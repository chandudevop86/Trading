from __future__ import annotations

from typing import Any

import pandas as pd

from src.execution.contracts import normalize_candidate_contract
from src.execution_engine import build_execution_candidates
from src.validation.engine import validate_trade


def prepare_candidates_for_execution(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for trade in trades:
        item = normalize_candidate_contract(dict(trade), symbol=symbol, strategy_name=strategy, timeframe=str(trade.get("timeframe", trade.get("interval", "")) or ""))
        validation = validate_trade(item, candles)
        item["validation_status"] = str(validation.get("decision", "FAIL") or "FAIL").upper()
        item["validation_score"] = round(float(validation.get("score", 0.0) or 0.0), 2)
        item["validation_reasons"] = list(validation.get("reasons", []) or [])
        item["reason_codes"] = list(item.get("reason_codes", []) or []) + item["validation_reasons"]
        item["execution_allowed"] = item["validation_status"] == "PASS"
        item["validation_metrics"] = dict(validation.get("metrics", {}) or {})
        validated.append(item)
    return build_execution_candidates(strategy, validated, symbol)


__all__ = ["prepare_candidates_for_execution"]
