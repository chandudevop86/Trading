from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


REQUIRED_FIELDS = ["timestamp", "open", "high", "low", "close", "volume"]
OPTIONAL_FIELDS = ["vwap", "ema_9", "ema_20", "ema_21", "ema_50", "ema_200", "rsi", "macd", "macd_signal", "macd_hist", "session", "symbol"]
ALIAS_MAP = {
    "datetime": "timestamp",
    "date": "date",
    "time": "time",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "vol": "volume",
}
STRUCTURE_MAP = {
    ("rally", "rally"): "RBR",
    ("drop", "rally"): "DBR",
    ("rally", "drop"): "RBD",
    ("drop", "drop"): "DBD",
}
REJECTION_REASON_TEXT = {
    "duplicate_zone": "zone already traded recently",
    "entry_after_cutoff": "entry was after the configured cutoff time",
    "invalid_rr": "reward to risk was below threshold",
    "max_retests_exceeded": "zone had too many retests",
    "rejection_candle_weak": "rejection candle was too weak",
    "retest_not_confirmed": "zone touch did not produce a confirmed retest entry",
    "session_filter_failed": "trade was outside the allowed session",
    "trend_alignment_failed": "trend alignment filter failed",
    "violated_zone": "zone was already violated",
    "vwap_alignment_failed": "VWAP alignment filter failed",
    "weak_departure": "departure from the base was too weak",
    "weak_structure_clarity": "structure around the zone was not clean enough",
    "weak_zone_score": "zone score was below the minimum threshold",
}


@dataclass(slots=True)
class SupplyDemandStrategyConfig:
    min_base_candles: int = 2
    max_base_candles: int = 5
    min_departure_ratio: float = 1.8
    min_total_score: float = 65.0
    max_retests: int = 1
    stop_buffer_pct: float = 0.001
    min_rr_ratio: float = 2.0
    risk_pct: float = 0.01
    capital: float = 100000.0
    entry_cutoff_time: str = "14:45"
    cooldown_minutes: int = 30
    use_vwap_filter: bool = True
    use_trend_filter: bool = True
    use_volume_filter: bool = True
    use_session_filter: bool = False
    use_volatility_filter: bool = True
    allowed_sessions: tuple[str, ...] = ("OPENING", "MORNING")
    min_impulse_strength: float = 1.2
    min_body_fraction: float = 0.45
    max_base_range_pct: float = 0.012
    max_base_overlap_ratio: float = 0.85
    min_rejection_body_ratio: float = 0.45
    min_rejection_wick_ratio: float = 0.2
    min_volume_expansion: float = 1.1
    volatility_floor_pct: float = 0.12
    session_start: str = "09:15"
    session_end: str = "15:30"
    duplicate_bucket_minutes: int = 5
    strategy_name: str = "DEMAND_SUPPLY"
    symbol: str = ""
    mode: str = "Balanced"
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    max_trades_per_day: int = 2
    duplicate_signal_cooldown_bars: int = 12
    require_vwap_alignment: bool = True
    require_trend_bias: bool = True
    require_market_structure: bool = True
    max_retest_bars: int = 4
    min_reaction_strength: float = 0.75
    min_zone_selection_score: float = 5.0
    min_structure_clarity_score: float = 60.0
    min_confirmation_body_ratio: float = 0.6
    zone_departure_buffer_pct: float = 0.0006
    vwap_reclaim_buffer_pct: float = 0.0005
    allow_afternoon_session: bool = False
    avoid_midday: bool = True
    require_retest_confirmation: bool = True


@dataclass(slots=True)
class StructureRecord:
    structure_type: str
    base_start_index: int
    base_end_index: int
    base_candles_count: int
    leg_in_size: float
    leg_out_size: float
    impulse_strength: float
    base_range: float
    base_range_pct: float
    departure_ratio: float
    leg_in_direction: str
    leg_out_direction: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SupplyDemandZone:
    zone_id: str
    symbol: str
    type: str
    structure_type: str
    created_at: str
    zone_low: float
    zone_high: float
    base_start_index: int
    base_end_index: int
    strength_score: float
    freshness_score: float
    tested: bool
    test_count: int
    violated: bool
    impulse_strength: float
    departure_ratio: float
    volume_score: float
    trend_score: float
    cleanliness_score: float
    validation_score: float
    total_score: float
    structure_clarity_score: float
    move_away_score: float
    base_tightness_score: float
    higher_timeframe_alignment_score: float
    vwap_score: float
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["notes"] = list(self.notes)
        return payload


@dataclass(slots=True)
class ValidationDecision:
    is_valid: bool
    rejection_reasons: list[str]
    validation_score: float
    entry_ready: bool
    metrics: dict[str, Any]


__all__ = [
    "ALIAS_MAP",
    "OPTIONAL_FIELDS",
    "REJECTION_REASON_TEXT",
    "REQUIRED_FIELDS",
    "STRUCTURE_MAP",
    "StructureRecord",
    "SupplyDemandStrategyConfig",
    "SupplyDemandZone",
    "ValidationDecision",
]
