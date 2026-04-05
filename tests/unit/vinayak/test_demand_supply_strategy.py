from datetime import datetime

import pandas as pd

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.demand_supply.service import (
    SupplyDemandStrategyConfig,
    build_supply_demand_report,
    generate_supply_demand_trade_candidates,
    normalize_ohlcv_for_supply_demand,
    run_demand_supply_strategy,
)


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


def _sample_candles() -> list[Candle]:
    return [
        Candle(pd.Timestamp(row["timestamp"]).to_pydatetime(), row["open"], row["high"], row["low"], row["close"], row["volume"])
        for row in _sample_frame().to_dict("records")
    ]


def test_demand_supply_returns_standardized_signal() -> None:
    signals = run_demand_supply_strategy(
        candles=_sample_candles(),
        symbol='^NSEI',
        capital=100000,
        risk_pct=0.01,
        rr_ratio=2.0,
    )

    assert signals, 'Expected at least one strict demand-supply signal'
    signal = signals[0]
    assert signal.strategy_name == 'Demand Supply'
    assert signal.symbol == '^NSEI'
    assert signal.side == 'BUY'
    assert signal.entry_price > 0
    assert signal.stop_loss > 0
    assert signal.target_price > signal.entry_price
    assert signal.metadata['rr_ratio'] >= 2.0


def test_normalization_maps_aliases_and_drops_duplicate_timestamps() -> None:
    raw = pd.DataFrame([
        {"Date": "2026-04-01", "Time": "09:15:00", "O": 100, "H": 101, "L": 99, "C": 100.5, "Vol": 1000},
        {"Date": "2026-04-01", "Time": "09:15:00", "O": 100.1, "H": 101.2, "L": 99.1, "C": 100.6, "Vol": 1100},
        {"Date": "2026-04-01", "Time": "09:20:00", "O": 100.6, "H": 101.4, "L": 100.2, "C": 101.1, "Vol": 1200},
    ])

    cleaned = normalize_ohlcv_for_supply_demand(raw)

    assert list(cleaned.columns[:6]) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert len(cleaned) == 2
    assert str(cleaned.iloc[0]["timestamp"]) == "2026-04-01 09:15:00"


def test_duplicate_zone_is_blocked() -> None:
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
    first_trades, _, _ = generate_supply_demand_trade_candidates(_sample_frame(), cfg)
    existing = [{"zone_id": row["zone_id"], "trade_time": row["trade_time"]} for row in first_trades]
    second_trades, second_rejects, _ = generate_supply_demand_trade_candidates(_sample_frame(), cfg, existing_trade_rows=existing)

    assert len(second_trades) == 0
    assert any("duplicate_zone" in row["reasons"] for row in second_rejects)



def test_generate_candidates_exposes_strict_validation_score() -> None:
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
    trades, rejects, _zones = generate_supply_demand_trade_candidates(_sample_frame(), cfg)

    assert rejects == []
    assert trades
    assert trades[0]["strict_validation_score"] >= 7
    assert trades[0]["retest_confirmed"] is True
    assert trades[0]["zone_selection_score"] >= 5.0


def test_generate_candidates_rejects_touch_without_retest_confirmation() -> None:
    cfg = SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0)
    weak_retest = _sample_frame().iloc[:-1].copy()

    trades, rejects, _zones = generate_supply_demand_trade_candidates(weak_retest, cfg)

    assert trades == []
    assert rejects
    assert any("retest_not_confirmed" in row["reasons"] for row in rejects)
def test_report_contains_readiness_and_rejection_summary() -> None:
    report = build_supply_demand_report(
        _sample_frame(),
        SupplyDemandStrategyConfig(min_base_candles=2, max_base_candles=3, min_departure_ratio=1.2, min_total_score=20.0),
    )

    assert "zone_rows" in report
    assert "trade_rows" in report
    assert "rejection_summary" in report
    assert "structure_metrics" in report
    assert "DBR" in report["structure_metrics"]
    assert "RBR" in report["structure_metrics"]
    assert "RBD" in report["structure_metrics"]
    assert "DBD" in report["structure_metrics"]
    assert "readiness_summary" in report
