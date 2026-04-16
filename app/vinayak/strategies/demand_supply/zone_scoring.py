from __future__ import annotations

import pandas as pd

from vinayak.strategies.demand_supply.enrichment import prepare_supply_demand_frame
from vinayak.strategies.demand_supply.models import StructureRecord, SupplyDemandStrategyConfig, SupplyDemandZone
from vinayak.strategies.demand_supply.structure_detection import detect_supply_demand_structures


def zone_bounds(frame: pd.DataFrame, structure: StructureRecord) -> tuple[str, float, float]:
    base = frame.iloc[structure.base_start_index : structure.base_end_index + 1]
    if structure.structure_type in {"DBR", "RBR"}:
        zone_type = "demand"
        zone_low = float(base["low"].min())
        zone_high = float(base[["open", "close", "high"]].max().min())
    else:
        zone_type = "supply"
        zone_low = float(base[["open", "close", "low"]].min().max())
        zone_high = float(base["high"].max())
    if zone_high <= zone_low:
        zone_low = float(base["low"].min())
        zone_high = float(base["high"].max())
    return zone_type, round(zone_low, 4), round(zone_high, 4)


def score_supply_demand_zone(frame: pd.DataFrame, structure: StructureRecord, config: SupplyDemandStrategyConfig | None = None) -> SupplyDemandZone:
    cfg = config or SupplyDemandStrategyConfig()
    zone_type, zone_low, zone_high = zone_bounds(frame, structure)
    symbol = str(frame["symbol"].iloc[-1]) if "symbol" in frame.columns and str(frame["symbol"].iloc[-1]).strip() else str(cfg.symbol or "UNKNOWN")
    created_at = pd.Timestamp(frame.iloc[structure.base_end_index]["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
    future = frame.iloc[structure.base_end_index + 1 :].copy()
    touched = (future["low"] <= zone_high) & (future["high"] >= zone_low) if not future.empty else pd.Series(dtype=bool)
    test_count = int(touched.sum()) if not future.empty else 0
    if zone_type == "demand":
        violated = bool((future["close"] < zone_low).any()) if not future.empty else False
        trend_ok = bool(frame.iloc[structure.base_end_index]["ema_21"] >= frame.iloc[structure.base_end_index]["ema_50"])
        vwap_ok = bool(frame.iloc[structure.base_end_index]["close"] >= frame.iloc[structure.base_end_index].get("vwap", frame.iloc[structure.base_end_index]["close"]))
    else:
        violated = bool((future["close"] > zone_high).any()) if not future.empty else False
        trend_ok = bool(frame.iloc[structure.base_end_index]["ema_21"] <= frame.iloc[structure.base_end_index]["ema_50"])
        vwap_ok = bool(frame.iloc[structure.base_end_index]["close"] <= frame.iloc[structure.base_end_index].get("vwap", frame.iloc[structure.base_end_index]["close"]))
    move_away_score = min(100.0, (structure.departure_ratio / max(float(cfg.min_departure_ratio), 1e-6)) * 40.0)
    base_tightness_score = max(0.0, (1.0 - min(structure.base_range_pct / max(float(cfg.max_base_range_pct), 1e-6), 1.0)) * 100.0)
    freshness_score = max(0.0, 100.0 - float(test_count) * 35.0 - (30.0 if violated else 0.0))
    volume_ratio = float(frame.iloc[structure.base_end_index + 1]["volume_ratio"]) if structure.base_end_index + 1 < len(frame) else 1.0
    volume_score = min(100.0, volume_ratio / max(float(cfg.min_volume_expansion), 1e-6) * 70.0)
    structure_clarity_score = min(100.0, (structure.impulse_strength / max(float(cfg.min_impulse_strength), 1e-6)) * 55.0 + (1.0 - min(test_count, 2) / 2.0) * 45.0)
    trend_score = 100.0 if trend_ok else 35.0
    vwap_score = 100.0 if vwap_ok else 40.0
    strength_score = round((move_away_score * 0.45) + (structure_clarity_score * 0.35) + (volume_score * 0.20), 2)
    cleanliness_score = round((base_tightness_score * 0.55) + (structure_clarity_score * 0.45), 2)
    validation_score = round((strength_score * 0.35) + (freshness_score * 0.25) + (cleanliness_score * 0.20) + (trend_score * 0.10) + (vwap_score * 0.10), 2)
    total_score = round((strength_score * 0.30) + (freshness_score * 0.25) + (cleanliness_score * 0.20) + (volume_score * 0.10) + (trend_score * 0.10) + (vwap_score * 0.05), 2)
    zone_id = f"{symbol}_{structure.structure_type}_{structure.base_start_index}_{structure.base_end_index}"
    notes: list[str] = []
    if test_count > 0:
        notes.append(f"zone already tested {test_count} time(s)")
    if violated:
        notes.append("zone invalidated by close through boundary")
    return SupplyDemandZone(
        zone_id=zone_id,
        symbol=symbol,
        type=zone_type,
        structure_type=structure.structure_type,
        created_at=created_at,
        zone_low=zone_low,
        zone_high=zone_high,
        base_start_index=structure.base_start_index,
        base_end_index=structure.base_end_index,
        strength_score=round(strength_score, 2),
        freshness_score=round(freshness_score, 2),
        tested=test_count > 0,
        test_count=test_count,
        violated=violated,
        impulse_strength=structure.impulse_strength,
        departure_ratio=structure.departure_ratio,
        volume_score=round(volume_score, 2),
        trend_score=round(trend_score, 2),
        cleanliness_score=round(cleanliness_score, 2),
        validation_score=round(validation_score, 2),
        total_score=round(total_score, 2),
        structure_clarity_score=round(structure_clarity_score, 2),
        move_away_score=round(move_away_score, 2),
        base_tightness_score=round(base_tightness_score, 2),
        higher_timeframe_alignment_score=round(trend_score, 2),
        vwap_score=round(vwap_score, 2),
        notes=notes,
    )


def build_supply_demand_zones(candles: pd.DataFrame, config: SupplyDemandStrategyConfig | None = None) -> list[SupplyDemandZone]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = prepare_supply_demand_frame(candles)
    return [score_supply_demand_zone(frame, structure, cfg) for structure in detect_supply_demand_structures(frame, cfg)]


__all__ = ["build_supply_demand_zones", "score_supply_demand_zone", "zone_bounds"]
