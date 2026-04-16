from __future__ import annotations

import pandas as pd

from vinayak.strategies.demand_supply.candidate_generation import generate_supply_demand_trade_candidates
from vinayak.strategies.demand_supply.enrichment import prepare_supply_demand_frame
from vinayak.strategies.demand_supply.models import SupplyDemandStrategyConfig
from vinayak.strategies.demand_supply.normalization import normalize_ohlcv_for_supply_demand
from vinayak.strategies.demand_supply.reporting import build_supply_demand_report
from vinayak.strategies.demand_supply.structure_detection import detect_supply_demand_structures
from vinayak.strategies.demand_supply.zone_scoring import build_supply_demand_zones


def _sample_frame() -> pd.DataFrame:
    rows = [
        {"timestamp": "2026-04-01 09:15:00", "open": 110.0, "high": 110.5, "low": 108.8, "close": 109.0, "volume": 1000},
        {"timestamp": "2026-04-01 09:20:00", "open": 109.0, "high": 109.2, "low": 108.5, "close": 108.6, "volume": 1200},
        {"timestamp": "2026-04-01 09:25:00", "open": 108.6, "high": 108.9, "low": 108.3, "close": 108.5, "volume": 900},
        {"timestamp": "2026-04-01 09:30:00", "open": 108.5, "high": 108.8, "low": 108.2, "close": 108.45, "volume": 850},
        {"timestamp": "2026-04-01 09:35:00", "open": 108.45, "high": 111.2, "low": 108.4, "close": 110.9, "volume": 2200},
        {"timestamp": "2026-04-01 09:40:00", "open": 110.9, "high": 111.4, "low": 109.7, "close": 110.1, "volume": 1500},
        {"timestamp": "2026-04-01 09:45:00", "open": 110.1, "high": 111.8, "low": 109.9, "close": 111.5, "volume": 1900},
    ]
    return pd.DataFrame(rows)


def test_demand_supply_decomposition_preserves_candidate_path() -> None:
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)

    normalized = normalize_ohlcv_for_supply_demand(_sample_frame())
    prepared = prepare_supply_demand_frame(normalized)
    structures = detect_supply_demand_structures(prepared, cfg)
    zones = build_supply_demand_zones(prepared, cfg)
    trades, rejects, generated_zones = generate_supply_demand_trade_candidates(prepared, cfg)

    assert not normalized.empty
    assert not prepared.empty
    assert structures
    assert zones
    assert generated_zones
    assert trades
    assert rejects == []
    assert generated_zones[0].zone_id == zones[0].zone_id


def test_demand_supply_reporting_module_keeps_readiness_payload_shape() -> None:
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)

    report = build_supply_demand_report(_sample_frame(), cfg)

    assert set(report) == {
        'zone_rows',
        'trade_rows',
        'rejection_summary',
        'rejection_analytics',
        'structure_metrics',
        'readiness_summary',
    }
    assert report['trade_rows']
    assert report['readiness_summary']['status'] in {'PASS', 'FAIL'}
