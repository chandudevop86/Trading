import pandas as pd
import pytest

from vinayak.data.cleaner import OHLCVValidationError, coerce_ohlcv
from vinayak.strategies.demand_supply.service import SupplyDemandStrategyConfig, build_supply_demand_zones, validate_supply_demand_trade


def _sample_frame() -> pd.DataFrame:
    return pd.DataFrame([
        {"timestamp": "2026-04-01 09:15:00", "open": 110.0, "high": 110.5, "low": 108.8, "close": 109.0, "volume": 1000},
        {"timestamp": "2026-04-01 09:20:00", "open": 109.0, "high": 109.2, "low": 108.5, "close": 108.6, "volume": 1200},
        {"timestamp": "2026-04-01 09:25:00", "open": 108.6, "high": 108.9, "low": 108.3, "close": 108.5, "volume": 900},
        {"timestamp": "2026-04-01 09:30:00", "open": 108.5, "high": 108.8, "low": 108.2, "close": 108.45, "volume": 850},
        {"timestamp": "2026-04-01 09:35:00", "open": 108.45, "high": 111.2, "low": 108.4, "close": 110.9, "volume": 2200},
        {"timestamp": "2026-04-01 09:40:00", "open": 110.9, "high": 111.4, "low": 109.7, "close": 110.1, "volume": 1500},
        {"timestamp": "2026-04-01 09:45:00", "open": 110.1, "high": 111.8, "low": 109.9, "close": 111.5, "volume": 1900},
    ])


def test_coerce_ohlcv_rejects_invalid_timestamp() -> None:
    raw = pd.DataFrame([{"timestamp": "bad-ts", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000}])
    with pytest.raises(OHLCVValidationError):
        coerce_ohlcv(raw)


def test_coerce_ohlcv_rejects_broken_ohlc_row() -> None:
    raw = pd.DataFrame([{"timestamp": "2026-04-01 09:15:00", "open": 100, "high": 99, "low": 101, "close": 100, "volume": 1000}])
    with pytest.raises(OHLCVValidationError):
        coerce_ohlcv(raw)


def test_validate_supply_demand_trade_rejects_violated_zone() -> None:
    frame = _sample_frame()
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
    zones = build_supply_demand_zones(frame, cfg)
    assert zones
    zone = zones[0]
    violated = pd.concat([
        frame,
        pd.DataFrame([
            {
                "timestamp": "2026-04-01 09:50:00",
                "open": 111.5,
                "high": 111.6,
                "low": zone.zone_low - 1.0,
                "close": zone.zone_low - 0.5,
                "volume": 2500,
            }
        ]),
    ], ignore_index=True)
    violated_zones = build_supply_demand_zones(violated, cfg)
    decision = validate_supply_demand_trade(violated_zones[0], violated, cfg)
    assert decision.is_valid is False
    assert 'violated_zone' in decision.rejection_reasons
