from __future__ import annotations

import pandas as pd

from vinayak.strategies.demand_supply.enrichment import prepare_supply_demand_frame
from vinayak.strategies.demand_supply.models import STRUCTURE_MAP, StructureRecord, SupplyDemandStrategyConfig


def direction_from_move(value: float) -> str:
    if value > 0:
        return "rally"
    if value < 0:
        return "drop"
    return "base"


def detect_supply_demand_structures(candles: pd.DataFrame, config: SupplyDemandStrategyConfig | None = None) -> list[StructureRecord]:
    cfg = config or SupplyDemandStrategyConfig()
    frame = prepare_supply_demand_frame(candles)
    structures: list[StructureRecord] = []
    for base_len in range(int(cfg.min_base_candles), int(cfg.max_base_candles) + 1):
        for start in range(1, len(frame) - base_len - 1):
            end = start + base_len - 1
            base = frame.iloc[start : end + 1]
            prev_row = frame.iloc[start - 1]
            next_row = frame.iloc[end + 1]
            base_range = float(base["high"].max() - base["low"].min())
            base_mid = float((base["high"].max() + base["low"].min()) / 2.0)
            base_range_pct = base_range / max(abs(base_mid), 1e-6)
            overlap_values: list[float] = []
            for idx in range(start + 1, end + 1):
                left = frame.iloc[idx - 1]
                right = frame.iloc[idx]
                overlap = max(0.0, min(float(left["high"]), float(right["high"])) - max(float(left["low"]), float(right["low"])))
                union = max(float(max(left["high"], right["high"]) - min(left["low"], right["low"])), 1e-6)
                overlap_values.append(overlap / union)
            overlap_ratio = sum(overlap_values) / max(len(overlap_values), 1)
            if base_range_pct > float(cfg.max_base_range_pct):
                continue
            if overlap_ratio > float(cfg.max_base_overlap_ratio):
                continue
            if not bool((base["body_fraction"] <= 0.55).all()):
                continue
            leg_in_move = float(prev_row["close"] - frame.iloc[max(start - 2, 0)]["close"])
            leg_out_move = float(next_row["close"] - base.iloc[-1]["close"])
            leg_in_direction = direction_from_move(leg_in_move)
            leg_out_direction = direction_from_move(leg_out_move)
            structure_type = STRUCTURE_MAP.get((leg_in_direction, leg_out_direction))
            if structure_type is None:
                continue
            leg_in_size = abs(leg_in_move)
            leg_out_size = abs(leg_out_move)
            departure_ratio = leg_out_size / max(base_range, 1e-6)
            impulse_strength = leg_out_size / max(float(next_row["atr_14"] or 0.0), 1e-6)
            if departure_ratio < float(cfg.min_departure_ratio) or impulse_strength < float(cfg.min_impulse_strength):
                continue
            structures.append(
                StructureRecord(
                    structure_type=structure_type,
                    base_start_index=start,
                    base_end_index=end,
                    base_candles_count=base_len,
                    leg_in_size=round(leg_in_size, 4),
                    leg_out_size=round(leg_out_size, 4),
                    impulse_strength=round(impulse_strength, 4),
                    base_range=round(base_range, 4),
                    base_range_pct=round(base_range_pct, 6),
                    departure_ratio=round(departure_ratio, 4),
                    leg_in_direction=leg_in_direction,
                    leg_out_direction=leg_out_direction,
                )
            )
    deduped: list[StructureRecord] = []
    seen: set[tuple[int, int, str]] = set()
    for item in structures:
        key = (item.base_start_index, item.base_end_index, item.structure_type)
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


__all__ = ["detect_supply_demand_structures"]
