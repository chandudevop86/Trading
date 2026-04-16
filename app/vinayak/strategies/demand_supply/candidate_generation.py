from __future__ import annotations

from typing import Any, Sequence

import pandas as pd

from vinayak.core.statuses import ValidationStatus
from vinayak.strategies.demand_supply.enrichment import prepare_supply_demand_frame
from vinayak.strategies.demand_supply.models import REJECTION_REASON_TEXT, SupplyDemandStrategyConfig, SupplyDemandZone
from vinayak.strategies.demand_supply.trade_validation import (
    existing_zone_ids,
    latest_trade_timestamp,
    trade_side,
    validate_supply_demand_trade,
    validation_score_bucket,
    zone_score_bucket,
)
from vinayak.strategies.demand_supply.zone_scoring import build_supply_demand_zones


def position_size(capital: float, risk_pct: float, risk_per_unit: float) -> int:
    if capital <= 0 or risk_pct <= 0 or risk_per_unit <= 0:
        return 0
    return max(int((capital * risk_pct) / risk_per_unit), 0)


def generate_supply_demand_trade_candidates(
    candles: pd.DataFrame,
    config: SupplyDemandStrategyConfig | None = None,
    *,
    existing_trade_rows: Sequence[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[SupplyDemandZone]]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = prepare_supply_demand_frame(candles)
    zones = build_supply_demand_zones(frame, cfg)
    traded_zone_ids = existing_zone_ids(existing_trade_rows)
    last_trade_at = latest_trade_timestamp(existing_trade_rows)
    trades: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for zone in zones:
        validation = validate_supply_demand_trade(zone, frame, cfg, traded_zone_ids=traded_zone_ids, last_trade_at=last_trade_at)
        if not validation.is_valid:
            rejects.append(
                {
                    "zone_id": zone.zone_id,
                    "symbol": zone.symbol,
                    "zone_type": zone.type,
                    "structure_type": str(zone.structure_type),
                    "session": str(validation.metrics.get("session", "UNKNOWN") or "UNKNOWN").upper(),
                    "zone_score": float(validation.metrics.get("zone_score", zone.total_score)),
                    "zone_score_bucket": str(validation.metrics.get("zone_score_bucket", zone_score_bucket(float(zone.total_score)))),
                    "validation_score": validation.validation_score,
                    "validation_score_bucket": str(validation.metrics.get("validation_score_bucket", validation_score_bucket(float(validation.validation_score)))),
                    "zone_selection_score": float(validation.metrics.get("zone_selection_score", 0.0)),
                    "reasons": list(validation.rejection_reasons),
                    "reason_text": ", ".join(REJECTION_REASON_TEXT.get(reason, reason.replace("_", " ")) for reason in validation.rejection_reasons),
                }
            )
            continue
        quantity = position_size(float(cfg.capital), float(cfg.risk_pct), float(validation.metrics["risk_per_unit"]))
        if quantity <= 0:
            rejects.append(
                {
                    "zone_id": zone.zone_id,
                    "symbol": zone.symbol,
                    "zone_type": zone.type,
                    "structure_type": str(zone.structure_type),
                    "session": str(validation.metrics.get("session", "UNKNOWN") or "UNKNOWN").upper(),
                    "zone_score": float(validation.metrics.get("zone_score", zone.total_score)),
                    "zone_score_bucket": str(validation.metrics.get("zone_score_bucket", zone_score_bucket(float(zone.total_score)))),
                    "validation_score": validation.validation_score,
                    "validation_score_bucket": str(validation.metrics.get("validation_score_bucket", validation_score_bucket(float(validation.validation_score)))),
                    "zone_selection_score": float(validation.metrics.get("zone_selection_score", 0.0)),
                    "reasons": ["invalid_rr"],
                    "reason_text": "quantity sizing produced zero quantity",
                }
            )
            continue
        side = trade_side(zone)
        timestamp_text = str(validation.metrics["entry_timestamp"])
        candidate = {
            "trade_id": f"{zone.zone_id}_{side}_{timestamp_text.replace(':', '').replace(' ', '_')}",
            "symbol": zone.symbol,
            "timestamp": timestamp_text,
            "trade_time": timestamp_text,
            "strategy_name": str(cfg.strategy_name),
            "strategy": str(cfg.strategy_name),
            "zone_id": zone.zone_id,
            "setup_type": str(zone.structure_type),
            "side": side,
            "entry_price": float(validation.metrics["entry_price"]),
            "entry": float(validation.metrics["entry_price"]),
            "stop_loss": float(validation.metrics["stop_loss"]),
            "stoploss": float(validation.metrics["stop_loss"]),
            "target_price": float(validation.metrics["target_price"]),
            "target": float(validation.metrics["target_price"]),
            "risk_per_unit": float(validation.metrics["risk_per_unit"]),
            "quantity": int(quantity),
            "capital": float(cfg.capital),
            "risk_pct": float(cfg.risk_pct),
            "rr_ratio": float(validation.metrics["rr_ratio"]),
            "validation_score": float(validation.validation_score),
            "validation_status": ValidationStatus.PASS,
            "validation_reasons": [],
            "execution_allowed": True,
            "notes": f"{zone.type} zone from {zone.structure_type} scored {zone.total_score:.2f}",
            "zone_low": float(zone.zone_low),
            "zone_high": float(zone.zone_high),
            "zone_type": zone.type,
            "total_score": float(zone.total_score),
            "strength_score": float(zone.strength_score),
            "freshness_score": float(zone.freshness_score),
            "cleanliness_score": float(zone.cleanliness_score),
            "zone_selection_score": float(validation.metrics["zone_selection_score"]),
            "strict_validation_score": int(validation.metrics["strict_validation_score"]),
            "retest_confirmed": bool(validation.metrics["retest_confirmed"]),
            "retest_touch_count": int(validation.metrics["retest_touch_count"]),
            "retest_touch_time": str(validation.metrics["retest_touch_timestamp"]),
            "retest_confirmation_time": str(validation.metrics["retest_confirmation_timestamp"]),
            "rejection_strength": float(validation.metrics["rejection_strength"]),
            "structure_clarity": float(validation.metrics["structure_clarity"]),
        }
        trades.append(candidate)
        traded_zone_ids.add(zone.zone_id)
        last_trade_at = pd.Timestamp(timestamp_text).to_pydatetime()
    return trades, rejects, zones


__all__ = ["generate_supply_demand_trade_candidates", "position_size"]
