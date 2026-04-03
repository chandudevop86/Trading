from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.breakout_bot import Candle, _coerce_candles, add_intraday_vwap
from src.demand_supply_validation import ZoneValidationConfig, candles_to_dataframe, validate_zone
from src.strategy_common import session_allowed, session_window
from src.trading_core import ScoringConfig, ScoreThresholds, StandardTrade, safe_quantity


@dataclass(frozen=True, slots=True)
class Zone:
    kind: str
    low: float
    high: float
    idx: int
    reaction_strength: float
    pattern: str = 'UNKNOWN'


@dataclass(frozen=True, slots=True)
class ZoneQualityMetrics:
    """Container for detailed zone-quality diagnostics."""

    freshness_weight_raw: float
    freshness_weight_pct: float
    freshness_label: str
    retest_count: int
    bars_since_creation: int
    time_in_zone_bars: int
    departure_ratio: float
    departure_speed_bars: int
    strong_departure_candles: int
    post_departure_overlap_ratio: float
    move_away_score: float
    move_away_label: str
    base_candle_count: int
    zone_width_pct: float
    internal_overlap_ratio: float
    pivot_cleanliness_score: float
    edge_clarity_score: float
    structure_clarity_score: float
    structure_label: str
    freshness_weight_component: float
    move_away_component: float
    structure_clarity_component: float
    zone_quality_score: float
    zone_quality_label: str

    def to_dict(self) -> dict[str, object]:
        return {
            'freshness_weight_raw': round(self.freshness_weight_raw, 2),
            'freshness_weight_pct': round(self.freshness_weight_pct, 2),
            'freshness_label': self.freshness_label,
            'retest_count': int(self.retest_count),
            'bars_since_creation': int(self.bars_since_creation),
            'time_in_zone_bars': int(self.time_in_zone_bars),
            'departure_ratio': round(self.departure_ratio, 4),
            'departure_speed_bars': int(self.departure_speed_bars),
            'strong_departure_candles': int(self.strong_departure_candles),
            'post_departure_overlap_ratio': round(self.post_departure_overlap_ratio, 4),
            'move_away_score': round(self.move_away_score, 2),
            'move_away_label': self.move_away_label,
            'base_candle_count': int(self.base_candle_count),
            'zone_width_pct': round(self.zone_width_pct, 4),
            'internal_overlap_ratio': round(self.internal_overlap_ratio, 4),
            'pivot_cleanliness_score': round(self.pivot_cleanliness_score, 2),
            'edge_clarity_score': round(self.edge_clarity_score, 2),
            'structure_clarity_score': round(self.structure_clarity_score, 2),
            'structure_label': self.structure_label,
            'freshness_weight_component': round(self.freshness_weight_component, 2),
            'move_away_component': round(self.move_away_component, 2),
            'structure_clarity_component': round(self.structure_clarity_component, 2),
            'zone_quality_score': round(self.zone_quality_score, 2),
            'zone_quality_label': self.zone_quality_label,
        }


@dataclass(slots=True)
class DemandSupplyConfig:
    """Production-oriented Nifty 5m demand/supply configuration."""

    mode: str = 'Balanced'
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    touch_tolerance_pct: float = 0.0018
    max_trades_per_day: int = 1
    duplicate_signal_cooldown_bars: int = 24
    max_retest_bars: int = 4
    retest_confirmation_bars: int = 2
    opening_range_minutes: int = 15
    atr_window: int = 8
    min_volatility_ratio: float = 1.05
    zone_freshness_bars: int = 20
    min_reaction_strength: float = 0.75
    min_zone_selection_score: float = 6.00
    minimum_take_score: float = 7.0
    min_confirmation_body_ratio: float = 0.60
    min_rejection_wick_ratio: float = 0.50
    zone_buffer_atr_fraction: float = 0.12
    zone_buffer_price_fraction: float = 0.0008
    zone_departure_buffer_pct: float = 0.0006
    vwap_reclaim_buffer_pct: float = 0.0005
    require_vwap_alignment: bool = True
    require_trend_bias: bool = True
    require_market_structure: bool = True
    structure_swing_window: int = 2
    base_candle_min: int = 2
    base_candle_max: int = 6
    base_range_threshold_factor: float = 1.0
    impulse_multiplier_threshold: float = 2.0
    volume_spike_multiplier: float = 1.5
    fast_base_candle_limit: int = 3
    strict_rejection_wick_body_ratio: float = 2.5
    strict_wick_dominance_ratio: float = 1.5
    strict_close_position_buy: float = 0.70
    strict_close_position_sell: float = 0.30
    strict_rejection_expansion_ratio: float = 1.2
    min_penetration_ratio: float = 0.10
    max_penetration_ratio: float = 0.50
    max_zone_width_pct: float = 0.20
    max_base_candles: int = 2
    min_departure_ratio: float = 3.0
    max_retest_count: int = 0
    max_time_in_zone_bars: int = 2
    min_rejection_score: float = 7.0
    min_zone_quality_score: float = 8.0
    min_a_grade_score: float = 8.5
    avoid_midday: bool = True
    morning_session_start: str = '09:35'
    morning_session_end: str = '10:45'
    afternoon_session_start: str = '13:46'
    afternoon_session_end: str = '14:45'
    allow_afternoon_session: bool = False
    midday_start: str = '11:16'
    midday_end: str = '13:45'
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    def __post_init__(self) -> None:
        self.scoring.mode = self.mode
        self.scoring.thresholds = ScoreThresholds(conservative=9.0, balanced=7.8, aggressive=6.8)


_SCORE_WEIGHTS: dict[str, float] = {
    'freshness': 2.0,
    'reaction_strength': 2.0,
    'structure_clarity': 1.6,
    'base_quality': 1.0,
    'impulse_quality': 1.0,
    'volume_quality': 0.8,
    'trend_alignment': 0.8,
    'vwap_alignment': 1.2,
    'retest_confirmation': 1.8,
    'volatility_quality': 0.8,
    'session_quality': 0.8,
}


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    by_day: dict[object, list[Candle]] = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _intraday_range(candle: Candle) -> float:
    return max(float(candle.high) - float(candle.low), 0.0001)


def _body_ratio(candle: Candle) -> float:
    return abs(float(candle.close) - float(candle.open)) / _intraday_range(candle)


def _lower_wick_ratio(candle: Candle) -> float:
    wick = min(float(candle.open), float(candle.close)) - float(candle.low)
    return max(wick, 0.0) / _intraday_range(candle)


def _upper_wick_ratio(candle: Candle) -> float:
    wick = float(candle.high) - max(float(candle.open), float(candle.close))
    return max(wick, 0.0) / _intraday_range(candle)


def calculate_vwap(candles: list[Candle]) -> None:
    add_intraday_vwap(candles)


def _midday_restricted(candle: Candle, config: DemandSupplyConfig) -> bool:
    return bool(config.avoid_midday) and session_window(
        candle.timestamp,
        morning_start=config.morning_session_start,
        morning_end=config.morning_session_end,
        midday_start=config.midday_start,
        midday_end=config.midday_end,
        allow_afternoon_session=bool(config.allow_afternoon_session),
        afternoon_start=config.afternoon_session_start,
        afternoon_end=config.afternoon_session_end,
    ) == 'MIDDAY_BLOCKED'


def _session_allowed(candle: Candle, config: DemandSupplyConfig) -> bool:
    return session_allowed(
        candle.timestamp,
        morning_start=config.morning_session_start,
        morning_end=config.morning_session_end,
        midday_start=config.midday_start,
        midday_end=config.midday_end,
        allow_afternoon_session=bool(config.allow_afternoon_session),
        afternoon_start=config.afternoon_session_start,
        afternoon_end=config.afternoon_session_end,
    ) and not _midday_restricted(candle, config)


def session_filter(candle: Candle, config: DemandSupplyConfig) -> bool:
    return _session_allowed(candle, config)


def _reaction_strength(day_candles: list[Candle], idx: int, side: str) -> float:
    candle = day_candles[idx]
    body_ratio = _body_ratio(candle)
    wick_ratio = _lower_wick_ratio(candle) if side == 'BUY' else _upper_wick_ratio(candle)
    follow_through = 0.0
    if idx + 1 < len(day_candles):
        next_candle = day_candles[idx + 1]
        if side == 'BUY':
            follow_through = max(float(next_candle.close) - float(candle.close), 0.0) / _intraday_range(candle)
        else:
            follow_through = max(float(candle.close) - float(next_candle.close), 0.0) / _intraday_range(candle)
    return round(min(max(body_ratio * 0.45 + wick_ratio * 0.35 + follow_through * 0.20, 0.0), 1.5), 4)


def _find_zones(day_candles: list[Candle], pivot_window: int) -> list[Zone]:
    zones: list[Zone] = []
    width = max(1, int(pivot_window or 1))
    for index in range(width, len(day_candles) - width):
        candle = day_candles[index]
        lows = [day_candles[pos].low for pos in range(index - width, index + width + 1) if pos != index]
        highs = [day_candles[pos].high for pos in range(index - width, index + width + 1) if pos != index]
        if lows and float(candle.low) < min(float(value) for value in lows):
            zones.append(
                Zone(
                    kind='demand',
                    low=float(candle.low),
                    high=float(max(candle.open, candle.close)),
                    idx=index,
                    reaction_strength=_reaction_strength(day_candles, index, 'BUY'),
                    pattern='DBR',
                )
            )
        if highs and float(candle.high) > max(float(value) for value in highs):
            zones.append(
                Zone(
                    kind='supply',
                    low=float(min(candle.open, candle.close)),
                    high=float(candle.high),
                    idx=index,
                    reaction_strength=_reaction_strength(day_candles, index, 'SELL'),
                    pattern='RBD',
                )
            )
    return zones


def _trend_ok(day_candles: list[Candle], idx: int, side: str) -> bool:
    if idx < 3:
        return False
    fast = sum(float(c.close) for c in day_candles[max(0, idx - 2): idx + 1]) / min(3, idx + 1)
    slow = sum(float(c.close) for c in day_candles[max(0, idx - 7): idx + 1]) / min(8, idx + 1)
    close = float(day_candles[idx].close)
    if side == 'BUY':
        return close >= fast >= slow
    return close <= fast <= slow


def _recent_swings(day_candles: list[Candle], idx: int, swing_window: int) -> tuple[list[float], list[float]]:
    highs: list[float] = []
    lows: list[float] = []
    window = max(1, int(swing_window or 1))
    upper_bound = min(len(day_candles) - window - 1, idx)
    for center in range(window, upper_bound + 1):
        candle = day_candles[center]
        left = day_candles[center - window:center]
        right = day_candles[center + 1:center + window + 1]
        if len(right) < window:
            continue
        if all(float(candle.high) >= float(item.high) for item in left + right):
            highs.append(float(candle.high))
        if all(float(candle.low) <= float(item.low) for item in left + right):
            lows.append(float(candle.low))
    return highs[-2:], lows[-2:]


def _market_structure(day_candles: list[Candle], idx: int, side: str, config: DemandSupplyConfig) -> tuple[bool, str]:
    highs, lows = _recent_swings(day_candles, idx, config.structure_swing_window)
    if len(highs) < 2 or len(lows) < 2:
        return False, 'INSUFFICIENT'
    if side == 'BUY':
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            return True, 'HH_HL'
        return False, 'STRUCTURE_WEAK'
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        return True, 'LH_LL'
    return False, 'STRUCTURE_WEAK'


def _higher_tf_bias(day_candles: list[Candle], idx: int) -> str:
    if idx < 4:
        return 'NEUTRAL'
    closes = [float(c.close) for c in day_candles[: idx + 1]]
    fast = sum(closes[-3:]) / 3.0
    slow = sum(closes[-5:]) / 5.0
    slope = closes[-1] - closes[-4]
    tolerance = max(0.02, closes[-1] * 0.0005)
    if fast >= slow - tolerance and slope > 0:
        return 'BULLISH'
    if fast <= slow + tolerance and slope < 0:
        return 'BEARISH'
    return 'NEUTRAL'


def _zone_mid(zone: Zone) -> float:
    return (float(zone.low) + float(zone.high)) / 2.0


def _avg_range(day_candles: list[Candle], idx: int, window: int) -> float:
    start = max(0, idx - max(1, int(window)) + 1)
    sample = day_candles[start: idx + 1]
    if not sample:
        return 0.0
    return sum(_intraday_range(candle) for candle in sample) / len(sample)


def _base_candle_profile(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> tuple[float, int, float]:
    min_candles = max(2, int(config.base_candle_min or 2))
    max_candles = max(min_candles, int(config.base_candle_max or 6))
    avg_range = max(_avg_range(day_candles, idx, config.atr_window), 0.0001)
    threshold = avg_range * max(float(config.base_range_threshold_factor or 1.0), 0.1)
    best_range = 0.0
    best_count = 0
    for candle_count in range(min_candles, max_candles + 1):
        start = idx - candle_count + 1
        if start < 0:
            continue
        sample = day_candles[start:idx + 1]
        if len(sample) != candle_count:
            continue
        base_range = max(float(c.high) for c in sample) - min(float(c.low) for c in sample)
        if best_count == 0 or base_range < best_range:
            best_range = round(base_range, 4)
            best_count = candle_count
    if best_count == 0:
        return 1.0, 0, round(threshold, 4)
    base_score = 2.0 if best_range < threshold else 1.0
    return base_score, best_count, round(threshold, 4)


def _impulse_score(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> tuple[float, float, float]:
    avg_range = max(_avg_range(day_candles, idx, config.atr_window), 0.0001)
    impulse = _intraday_range(day_candles[idx])
    threshold = avg_range * max(float(config.impulse_multiplier_threshold or 2.0), 0.1)
    score = 3.0 if impulse > threshold else 1.0
    return score, round(impulse, 4), round(avg_range, 4)


def _volume_component(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> tuple[float, bool, float, float]:
    current_volume = max(float(getattr(day_candles[idx], 'volume', 0.0) or 0.0), 0.0)
    left = max(0, idx - 5)
    history = [max(float(getattr(candle, 'volume', 0.0) or 0.0), 0.0) for candle in day_candles[left:idx]]
    avg_volume = sum(history) / len(history) if history else current_volume
    volume_spike = avg_volume > 0 and current_volume > avg_volume * max(float(config.volume_spike_multiplier or 1.5), 0.1)
    normalized = 1.0 if volume_spike else 0.0
    return round(normalized * _SCORE_WEIGHTS['volume_quality'], 4), volume_spike, round(current_volume, 4), round(avg_volume, 4)


def _touch_count(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> int:
    count = 0
    for candle in day_candles[max(zone.idx + 1, 0):max(idx, zone.idx + 1)]:
        if _touches_zone(candle, zone, side, float(config.touch_tolerance_pct)):
            count += 1
    return count


def _fresh_touch_score(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> tuple[float, int]:
    touch_count = _touch_count(day_candles, zone, side, idx, config)
    raw_score = 3.0 if touch_count == 0 else 1.0
    normalized = 1.0 if raw_score >= 3.0 else 0.33
    return round(normalized * _SCORE_WEIGHTS['freshness'], 4), touch_count


def _time_component(base_candle_count: int, config: DemandSupplyConfig) -> tuple[float, float]:
    raw_score = 2.0 if 0 < int(base_candle_count) <= int(config.fast_base_candle_limit) else 1.0
    normalized = 1.0 if raw_score >= 2.0 else 0.5
    return round(normalized * _SCORE_WEIGHTS['base_quality'], 4), raw_score


def _trend_component(bias_aligned: bool, trend_ok: bool) -> tuple[float, float]:
    raw_score = 2.0 if bias_aligned and trend_ok else 0.0
    normalized = 1.0 if raw_score >= 2.0 else 0.0
    return round(normalized * _SCORE_WEIGHTS['trend_alignment'], 4), raw_score


def _retest_hold_score(confirmation_candle: Candle, zone: Zone, side: str, retest_confirmed: bool) -> float:
    if not retest_confirmed:
        return 0.0
    if side == 'BUY' and float(confirmation_candle.close) >= float(zone.high):
        return 3.0
    if side == 'SELL' and float(confirmation_candle.close) <= float(zone.low):
        return 3.0
    return 0.0


def _score_interpretation(total_score: float) -> str:
    if total_score >= 10.0:
        return 'STRONG_TRADE'
    if total_score >= 7.0:
        return 'MEDIUM_OPTIONAL'
    return 'SKIP'


def _volatility_ratio(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> float:
    current_range = _intraday_range(day_candles[idx])
    avg_range = _avg_range(day_candles, idx, config.atr_window)
    if avg_range <= 0:
        return 0.0
    return round(current_range / avg_range, 4)


def _zone_freshness_ratio(idx: int, zone: Zone, config: DemandSupplyConfig) -> float:
    age = max(idx - zone.idx, 0)
    if config.zone_freshness_bars <= 0:
        return 0.0
    return round(min(max((float(config.zone_freshness_bars) - float(age)) / float(config.zone_freshness_bars), 0.0), 1.0), 4)


def _vwap_aligned(candle: Candle, side: str, buffer_pct: float = 0.0) -> bool:
    close = float(candle.close)
    vwap = float(candle.vwap)
    if side == 'BUY':
        return close >= vwap * (1.0 + max(float(buffer_pct), 0.0))
    return close <= vwap * (1.0 - max(float(buffer_pct), 0.0))


def _zone_broken(day_candles: list[Candle], zone: Zone, side: str, start_idx: int, end_idx: int, tolerance_pct: float) -> bool:
    for idx in range(max(zone.idx + 1, start_idx), min(end_idx, len(day_candles))):
        candle = day_candles[idx]
        if side == 'BUY' and float(candle.close) < float(zone.low) * (1.0 - tolerance_pct):
            return True
        if side == 'SELL' and float(candle.close) > float(zone.high) * (1.0 + tolerance_pct):
            return True
    return False


def _touches_zone(candle: Candle, zone: Zone, side: str, tolerance_pct: float) -> bool:
    if side == 'BUY':
        return float(candle.low) <= float(zone.high) * (1.0 + tolerance_pct) and float(candle.close) >= float(zone.low) * (1.0 - tolerance_pct)
    return float(candle.high) >= float(zone.low) * (1.0 - tolerance_pct) and float(candle.close) <= float(zone.high) * (1.0 + tolerance_pct)


def _wick_body_ratio(candle: Candle, side: str) -> float:
    body = max(abs(float(candle.close) - float(candle.open)), 0.01)
    wick = max(min(float(candle.open), float(candle.close)) - float(candle.low), 0.0) if side == 'BUY' else max(float(candle.high) - max(float(candle.open), float(candle.close)), 0.0)
    return round(wick / body, 4)


def _wick_dominance_ratio(candle: Candle, side: str) -> float:
    dominant = max(min(float(candle.open), float(candle.close)) - float(candle.low), 0.0) if side == 'BUY' else max(float(candle.high) - max(float(candle.open), float(candle.close)), 0.0)
    opposite = max(float(candle.high) - max(float(candle.open), float(candle.close)), 0.01) if side == 'BUY' else max(min(float(candle.open), float(candle.close)) - float(candle.low), 0.01)
    return round(dominant / opposite, 4)


def _close_position(candle: Candle) -> float:
    return round((float(candle.close) - float(candle.low)) / _intraday_range(candle), 4)


def _penetration_ratio(candle: Candle, zone: Zone, side: str) -> float:
    zone_width = max(float(zone.high) - float(zone.low), 0.01)
    if side == 'BUY':
        return round((float(zone.high) - float(candle.low)) / zone_width, 4)
    return round((float(candle.high) - float(zone.low)) / zone_width, 4)


def _rejection_expansion_ratio(day_candles: list[Candle], idx: int, candle: Candle, config: DemandSupplyConfig) -> float:
    avg_range = max(_avg_range(day_candles, idx, config.atr_window), 0.01)
    return round(_intraday_range(candle) / avg_range, 4)


def _zone_width_pct(zone: Zone, reference_price: float) -> float:
    if reference_price <= 0:
        return 999.0
    return round(((float(zone.high) - float(zone.low)) / reference_price) * 100.0, 4)


def _departure_ratio(day_candles: list[Candle], zone: Zone, side: str, idx: int) -> float:
    zone_width = max(float(zone.high) - float(zone.low), 0.01)
    end = min(len(day_candles), zone.idx + 4, idx + 1)
    sample = day_candles[zone.idx:end]
    if not sample:
        return 0.0
    if side == 'BUY':
        departure_move = max(float(c.high) for c in sample) - float(zone.high)
    else:
        departure_move = float(zone.low) - min(float(c.low) for c in sample)
    return round(max(departure_move, 0.0) / zone_width, 4)


def _time_in_zone_bars(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> int:
    count = 0
    started = False
    for candle in day_candles[zone.idx:min(idx + 1, len(day_candles))]:
        touches = _touches_zone(candle, zone, side, float(config.touch_tolerance_pct))
        if touches:
            count += 1
            started = True
            continue
        if started:
            break
    return count


def _retest_count_after_departure(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> int:
    departed = False
    count = 0
    for candle in day_candles[zone.idx + 1:min(idx, len(day_candles))]:
        if not departed:
            if _zone_departed(candle, zone, side, config):
                departed = True
            continue
        if _touches_zone(candle, zone, side, float(config.touch_tolerance_pct)):
            count += 1
    return count


def _rejection_score(day_candles: list[Candle], idx: int, candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> tuple[float, dict[str, float]]:
    wick_body_ratio = _wick_body_ratio(candle, side)
    wick_dominance = _wick_dominance_ratio(candle, side)
    close_pos = _close_position(candle)
    expansion_ratio = _rejection_expansion_ratio(day_candles, idx, candle, config)
    penetration = _penetration_ratio(candle, zone, side)
    score = 0.0
    if wick_body_ratio >= 2.5:
        score += 2.0
    elif wick_body_ratio >= 2.0:
        score += 1.0
    if wick_dominance >= 1.5:
        score += 2.0
    if side == 'BUY':
        if close_pos >= 0.75:
            score += 2.0
        elif close_pos >= 0.65:
            score += 1.0
    else:
        if close_pos <= 0.25:
            score += 2.0
        elif close_pos <= 0.35:
            score += 1.0
    if expansion_ratio >= 1.5:
        score += 2.0
    elif expansion_ratio >= 1.2:
        score += 1.0
    if 0.1 <= penetration <= 0.5:
        score += 2.0
    elif 0.1 <= penetration <= 0.7:
        score += 1.0
    diagnostics = {
        'wick_body_ratio': round(wick_body_ratio, 4),
        'wick_dominance_ratio': round(wick_dominance, 4),
        'close_position': round(close_pos, 4),
        'rejection_expansion_ratio': round(expansion_ratio, 4),
        'penetration_ratio': round(penetration, 4),
    }
    return round(score, 2), diagnostics


def _base_sample(day_candles: list[Candle], zone: Zone, config: DemandSupplyConfig) -> list[Candle]:
    _, base_candle_count, _ = _base_candle_profile(day_candles, min(max(zone.idx, 0), len(day_candles) - 1), config)
    if base_candle_count <= 0:
        return [day_candles[min(max(zone.idx, 0), len(day_candles) - 1)]]
    start = max(0, zone.idx - base_candle_count + 1)
    return day_candles[start:zone.idx + 1]


def _count_meaningful_retests(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> int:
    """Count real revisits after departure, ignoring tiny overlap noise."""
    departed = False
    active_retest = False
    retest_count = 0
    for candle in day_candles[zone.idx + 1:min(idx, len(day_candles))]:
        if not departed:
            if _zone_departed(candle, zone, side, config):
                departed = True
            continue
        penetration = _penetration_ratio(candle, zone, side)
        touches = _touches_zone(candle, zone, side, float(config.touch_tolerance_pct))
        meaningful_touch = touches and penetration >= float(config.min_penetration_ratio)
        if meaningful_touch and not active_retest:
            retest_count += 1
            active_retest = True
        elif not touches:
            active_retest = False
    return retest_count


def _freshness_label(component: float) -> str:
    if component >= 8.5:
        return 'FRESH'
    if component >= 6.0:
        return 'USABLE'
    if component >= 3.5:
        return 'STALE'
    return 'EXHAUSTED'


def _freshness_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    retest_count = _count_meaningful_retests(day_candles, zone, side, idx, config)
    bars_since_creation = max(idx - zone.idx, 0)
    freshness_raw = 5.0 if retest_count == 0 else 2.0 if retest_count == 1 else 0.5 if retest_count == 2 else 0.0
    age_score = 3.0 if bars_since_creation <= 15 else 2.0 if bars_since_creation <= 35 else 1.0 if bars_since_creation <= 60 else 0.0
    raw = freshness_raw + age_score
    component = round((raw / 8.0) * 10.0, 2)
    diagnostics = {
        'retest_count': int(retest_count),
        'bars_since_creation': int(bars_since_creation),
        'freshness_weight_raw': round(raw, 2),
        'freshness_weight_pct': round((raw / 8.0) * 100.0, 2),
        'freshness_label': _freshness_label(component),
        'freshness_weight_component': component,
    }
    return component, diagnostics


def _strong_departure_candle(candle: Candle, side: str) -> bool:
    body_ratio = _body_ratio(candle)
    close_pos = _close_position(candle)
    if side == 'BUY':
        opposite_wick = _upper_wick_ratio(candle)
        return float(candle.close) > float(candle.open) and body_ratio >= 0.55 and opposite_wick <= 0.25 and close_pos >= 0.70
    opposite_wick = _lower_wick_ratio(candle)
    return float(candle.close) < float(candle.open) and body_ratio >= 0.55 and opposite_wick <= 0.25 and close_pos <= 0.30


def _departure_speed_bars(day_candles: list[Candle], zone: Zone, side: str, idx: int) -> int:
    for offset in range(1, min(7, idx - zone.idx + 1)):
        probe_idx = zone.idx + offset
        if probe_idx > idx:
            break
        if _departure_ratio(day_candles, zone, side, probe_idx) >= 1.2:
            return offset
    return max(min(idx - zone.idx, 6), 0)


def _strong_departure_count(day_candles: list[Candle], zone: Zone, side: str, idx: int) -> int:
    end = min(len(day_candles), zone.idx + 5, idx + 1)
    count = 0
    for candle in day_candles[zone.idx + 1:end]:
        if _strong_departure_candle(candle, side):
            count += 1
    return count


def _post_departure_overlap_ratio(day_candles: list[Candle], zone: Zone, idx: int) -> float:
    sample = day_candles[zone.idx + 1:min(len(day_candles), zone.idx + 5, idx + 1)]
    if len(sample) < 2:
        return 0.0
    overlap_values: list[float] = []
    for left, right in zip(sample, sample[1:]):
        overlap = max(0.0, min(float(left.high), float(right.high)) - max(float(left.low), float(right.low)))
        footprint = max(max(float(left.high), float(right.high)) - min(float(left.low), float(right.low)), 0.0001)
        overlap_values.append(overlap / footprint)
    if not overlap_values:
        return 0.0
    return round(sum(overlap_values) / len(overlap_values), 4)


def _move_away_label(component: float) -> str:
    if component >= 9.0:
        return 'EXPLOSIVE'
    if component >= 7.0:
        return 'STRONG'
    if component >= 5.0:
        return 'OK'
    return 'WEAK'


def _move_away_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str) -> tuple[float, dict[str, object]]:
    departure_ratio = _departure_ratio(day_candles, zone, side, idx)
    departure_speed_bars = _departure_speed_bars(day_candles, zone, side, idx)
    strong_departure_candles = _strong_departure_count(day_candles, zone, side, idx)
    post_departure_overlap_ratio = _post_departure_overlap_ratio(day_candles, zone, idx)
    raw = 0.0
    if departure_ratio >= 3.0:
        raw += 4.0
    elif departure_ratio >= 2.0:
        raw += 2.5
    elif departure_ratio >= 1.2:
        raw += 1.0
    if departure_speed_bars <= 2:
        raw += 2.5
    elif departure_speed_bars <= 4:
        raw += 1.0
    if strong_departure_candles >= 3:
        raw += 2.0
    elif strong_departure_candles >= 2:
        raw += 1.0
    if post_departure_overlap_ratio <= 0.15:
        raw += 1.5
    elif post_departure_overlap_ratio <= 0.25:
        raw += 0.5
    component = round(min(raw, 10.0), 2)
    diagnostics = {
        'departure_ratio': round(departure_ratio, 4),
        'departure_speed_bars': int(departure_speed_bars),
        'strong_departure_candles': int(strong_departure_candles),
        'post_departure_overlap_ratio': round(post_departure_overlap_ratio, 4),
        'move_away_score': component,
        'move_away_label': _move_away_label(component),
        'move_away_component': component,
    }
    return component, diagnostics


def _internal_overlap_ratio(base_candles: list[Candle]) -> float:
    if len(base_candles) < 2:
        return 0.0
    overlap_values: list[float] = []
    for left, right in zip(base_candles, base_candles[1:]):
        overlap = max(0.0, min(float(left.high), float(right.high)) - max(float(left.low), float(right.low)))
        footprint = max(max(float(left.high), float(right.high)) - min(float(left.low), float(right.low)), 0.0001)
        overlap_values.append(overlap / footprint)
    return round(sum(overlap_values) / len(overlap_values), 4) if overlap_values else 0.0


def _pivot_cleanliness_score(base_candles: list[Candle]) -> float:
    if len(base_candles) <= 1:
        return 2.0
    directions = [1 if float(c.close) >= float(c.open) else -1 for c in base_candles]
    alternations = sum(1 for left, right in zip(directions, directions[1:]) if left != right)
    micro_pivots = 0
    for idx in range(1, len(base_candles) - 1):
        candle = base_candles[idx]
        if (float(candle.high) > float(base_candles[idx - 1].high) and float(candle.high) > float(base_candles[idx + 1].high)) or (float(candle.low) < float(base_candles[idx - 1].low) and float(candle.low) < float(base_candles[idx + 1].low)):
            micro_pivots += 1
    noise = alternations + micro_pivots
    if noise <= 1:
        return 2.0
    if noise <= 3:
        return 1.0
    return 0.0


def _edge_clarity_score(base_candles: list[Candle], zone: Zone) -> float:
    zone_width = max(float(zone.high) - float(zone.low), 0.0001)
    high_variation = max(float(c.high) for c in base_candles) - min(float(c.high) for c in base_candles)
    low_variation = max(float(c.low) for c in base_candles) - min(float(c.low) for c in base_candles)
    edge_variation_ratio = max(high_variation, low_variation) / zone_width
    if edge_variation_ratio <= 0.30:
        return 2.0
    if edge_variation_ratio <= 0.55:
        return 1.0
    return 0.0


def _structure_label(component: float) -> str:
    if component >= 8.5:
        return 'CLEAN'
    if component >= 6.5:
        return 'DECENT'
    if component >= 4.5:
        return 'MESSY'
    return 'INVALID'


def _structure_clarity_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str, base_candle_count: int, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    reference_price = max(abs(float(day_candles[idx].close)), 0.01)
    zone_width_pct = _zone_width_pct(zone, reference_price)
    base_candles = _base_sample(day_candles, zone, config)
    effective_base_candle_count = len(base_candles) or base_candle_count
    internal_overlap_ratio = _internal_overlap_ratio(base_candles)
    pivot_cleanliness_score = _pivot_cleanliness_score(base_candles)
    edge_clarity_score = _edge_clarity_score(base_candles, zone)
    raw = 0.0
    if effective_base_candle_count <= 2:
        raw += 2.5
    elif effective_base_candle_count <= 3:
        raw += 1.5
    elif effective_base_candle_count <= 4:
        raw += 0.5
    if zone_width_pct <= 0.20:
        raw += 2.5
    elif zone_width_pct <= 0.30:
        raw += 1.5
    elif zone_width_pct <= 0.40:
        raw += 0.5
    if internal_overlap_ratio <= 0.25:
        raw += 2.0
    elif internal_overlap_ratio <= 0.40:
        raw += 1.0
    raw += pivot_cleanliness_score
    raw += edge_clarity_score
    component = round(min(raw, 10.0), 2)
    diagnostics = {
        'base_candle_count': int(effective_base_candle_count),
        'zone_width_pct': round(zone_width_pct, 4),
        'internal_overlap_ratio': round(internal_overlap_ratio, 4),
        'pivot_cleanliness_score': round(pivot_cleanliness_score, 2),
        'edge_clarity_score': round(edge_clarity_score, 2),
        'structure_clarity_score': component,
        'structure_label': _structure_label(component),
        'structure_clarity_component': component,
    }
    return component, diagnostics


def _zone_quality_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str, base_candle_count: int, config: DemandSupplyConfig) -> ZoneQualityMetrics:
    freshness_component, freshness = _freshness_metrics(day_candles, idx, zone, side, config)
    move_away_component, move_away = _move_away_metrics(day_candles, idx, zone, side)
    structure_component, structure = _structure_clarity_metrics(day_candles, idx, zone, side, base_candle_count, config)
    time_in_zone_bars = _time_in_zone_bars(day_candles, zone, side, idx, config)
    final_score = round((freshness_component * 0.25) + (move_away_component * 0.40) + (structure_component * 0.35), 2)
    if final_score >= 8.8:
        final_label = 'A'
    elif final_score >= 7.4:
        final_label = 'B'
    elif final_score >= 6.0:
        final_label = 'C'
    else:
        final_label = 'REJECT'
    return ZoneQualityMetrics(
        freshness_weight_raw=float(freshness['freshness_weight_raw']),
        freshness_weight_pct=float(freshness['freshness_weight_pct']),
        freshness_label=str(freshness['freshness_label']),
        retest_count=int(freshness['retest_count']),
        bars_since_creation=int(freshness['bars_since_creation']),
        time_in_zone_bars=int(time_in_zone_bars),
        departure_ratio=float(move_away['departure_ratio']),
        departure_speed_bars=int(move_away['departure_speed_bars']),
        strong_departure_candles=int(move_away['strong_departure_candles']),
        post_departure_overlap_ratio=float(move_away['post_departure_overlap_ratio']),
        move_away_score=float(move_away['move_away_score']),
        move_away_label=str(move_away['move_away_label']),
        base_candle_count=int(structure['base_candle_count']),
        zone_width_pct=float(structure['zone_width_pct']),
        internal_overlap_ratio=float(structure['internal_overlap_ratio']),
        pivot_cleanliness_score=float(structure['pivot_cleanliness_score']),
        edge_clarity_score=float(structure['edge_clarity_score']),
        structure_clarity_score=float(structure['structure_clarity_score']),
        structure_label=str(structure['structure_label']),
        freshness_weight_component=float(freshness_component),
        move_away_component=float(move_away_component),
        structure_clarity_component=float(structure_component),
        zone_quality_score=float(final_score),
        zone_quality_label=final_label,
    )


def _zone_quality_score(day_candles: list[Candle], idx: int, zone: Zone, side: str, touch_count: int, base_candle_count: int, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    metrics = _zone_quality_metrics(day_candles, idx, zone, side, base_candle_count, config)
    return metrics.zone_quality_score, metrics.to_dict()


def _zone_departed(candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> bool:
    buffer_pct = max(float(config.zone_departure_buffer_pct), float(config.touch_tolerance_pct) * 0.25)
    if side == 'BUY':
        return float(candle.close) >= float(zone.high) * (1.0 + buffer_pct)
    return float(candle.close) <= float(zone.low) * (1.0 - buffer_pct)


def rejection_candle(candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> bool:
    return _rejection_candle(candle, zone, side, config)


def mark_zone_retest_state(state: dict[str, object], *, event: str, candle_idx: int) -> dict[str, object]:
    updated = dict(state)
    updated['last_event'] = event
    updated[f'{event}_idx'] = candle_idx
    if event == 'trade':
        updated['trade_created'] = True
    return updated


def is_retest(day_candles: list[Candle], zone: Zone, side: str, state: dict[str, object], retest_idx: int, config: DemandSupplyConfig) -> bool:
    departure_idx = int(state.get('departure_idx', -1) or -1)
    if departure_idx < 0 or retest_idx <= departure_idx:
        return False
    if retest_idx - departure_idx > int(config.max_retest_bars):
        return False
    candle = day_candles[retest_idx]
    if not _touches_zone(candle, zone, side, float(config.touch_tolerance_pct)):
        return False
    if not rejection_candle(candle, zone, side, config):
        return False
    return _vwap_aligned(candle, side)


def _rejection_candle(candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> bool:
    zone_mid = _zone_mid(zone)
    wick_ok = _lower_wick_ratio(candle) >= float(config.min_rejection_wick_ratio) if side == 'BUY' else _upper_wick_ratio(candle) >= float(config.min_rejection_wick_ratio)
    dominance_ok = _wick_dominance_ratio(candle, side) >= 1.0
    penetration = _penetration_ratio(candle, zone, side)
    if side == 'BUY':
        close_ok = float(candle.close) >= zone_mid
    else:
        close_ok = float(candle.close) <= zone_mid
    return wick_ok and dominance_ok and close_ok and float(config.min_penetration_ratio) <= penetration <= 1.0


def _confirmation_candle(candle: Candle, touch_candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> bool:
    body_ok = _body_ratio(candle) >= float(config.min_confirmation_body_ratio)
    if side == 'BUY':
        return body_ok and float(candle.close) > float(candle.open) and float(candle.close) > max(float(touch_candle.open), float(touch_candle.close), float(zone.high))
    return body_ok and float(candle.close) < float(candle.open) and float(candle.close) < min(float(touch_candle.open), float(touch_candle.close), float(zone.low))


def detect_retest(day_candles: list[Candle], zone: Zone, side: str, start_idx: int, config: DemandSupplyConfig) -> tuple[int, int, dict[str, object]] | None:
    search_start = max(zone.idx + 1, start_idx, zone.idx + 1)
    search_end = min(len(day_candles), zone.idx + 1 + max(int(config.zone_freshness_bars), int(config.max_retest_bars) * 3, 6))
    state: dict[str, object] = {
        'zone_idx': zone.idx,
        'side': side,
        'first_touch_idx': -1,
        'departure_idx': -1,
        'retest_idx': -1,
        'trade_created': False,
        'last_event': 'idle',
    }

    for idx in range(search_start, search_end):
        candle = day_candles[idx]
        if _zone_broken(day_candles, zone, side, zone.idx + 1, idx + 1, float(config.touch_tolerance_pct) * 0.75):
            return None
        if not session_filter(candle, config):
            continue

        if int(state.get('departure_idx', -1)) < 0:
            if _zone_departed(candle, zone, side, config):
                state = mark_zone_retest_state(state, event='departure', candle_idx=idx)
            continue

        if idx - int(state.get('departure_idx', -1)) > int(config.max_retest_bars):
            return None

        if not is_retest(day_candles, zone, side, state, idx, config):
            continue

        if int(state.get('first_touch_idx', -1)) < 0:
            state = mark_zone_retest_state(state, event='first_touch', candle_idx=idx)
        state = mark_zone_retest_state(state, event='retest', candle_idx=idx)
        touch_candle = day_candles[idx]
        confirmation_limit = min(len(day_candles), idx + 1 + max(1, int(config.retest_confirmation_bars)))
        for confirmation_idx in range(idx + 1, confirmation_limit):
            confirmation_candle = day_candles[confirmation_idx]
            if not session_filter(confirmation_candle, config):
                continue
            if config.require_vwap_alignment and not _vwap_aligned(confirmation_candle, side, float(config.vwap_reclaim_buffer_pct)):
                continue
            if _confirmation_candle(confirmation_candle, touch_candle, zone, side, config):
                state = mark_zone_retest_state(state, event='trade', candle_idx=confirmation_idx)
                return idx, confirmation_idx, state
        return None
    return None


def _freshness_component(idx: int, zone: Zone, config: DemandSupplyConfig) -> float:
    return round(_zone_freshness_ratio(idx, zone, config) * _SCORE_WEIGHTS['freshness'], 4)


def _reaction_component(zone: Zone, day_candles: list[Candle], idx: int, side: str) -> tuple[float, float]:
    reaction_metric = max(float(zone.reaction_strength), _reaction_strength(day_candles, idx, side))
    normalized = min(max(reaction_metric / 1.10, 0.0), 1.0)
    return round(normalized * _SCORE_WEIGHTS['reaction_strength'], 4), round(reaction_metric, 4)


def _structure_component(day_candles: list[Candle], idx: int, zone: Zone, side: str, bias_aligned: bool, structure_ok: bool) -> tuple[float, float]:
    avg_range = max(_avg_range(day_candles, idx, 8), 0.0001)
    zone_width = max(float(zone.high) - float(zone.low), 0.0001)
    width_ratio = zone_width / avg_range
    width_score = 1.0 if 0.18 <= width_ratio <= 1.05 else 0.7 if width_ratio <= 1.35 else 0.35 if width_ratio <= 1.80 else 0.0
    location_score = 1.0 if (side == 'BUY' and float(day_candles[idx].close) >= _zone_mid(zone)) or (side == 'SELL' and float(day_candles[idx].close) <= _zone_mid(zone)) else 0.0
    bias_score = 1.0 if bias_aligned else 0.0
    structure_pattern_score = 1.0 if structure_ok else 0.0
    structure_ratio = (width_score * 0.30) + (location_score * 0.15) + (bias_score * 0.20) + (structure_pattern_score * 0.35)
    return round(structure_ratio * _SCORE_WEIGHTS['structure_clarity'], 4), round(structure_ratio, 4)

def _vwap_component(candle: Candle, side: str) -> tuple[float, bool]:
    aligned = _vwap_aligned(candle, side)
    return (_SCORE_WEIGHTS['vwap_alignment'] if aligned else 0.0), aligned


def _retest_component(touch_candle: Candle, confirmation_candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> tuple[float, float]:
    rejection_quality = min(max(((_lower_wick_ratio(touch_candle) if side == 'BUY' else _upper_wick_ratio(touch_candle)) / max(float(config.min_rejection_wick_ratio), 0.0001)), 0.0), 1.0)
    confirmation_quality = min(max((_body_ratio(confirmation_candle) / max(float(config.min_confirmation_body_ratio), 0.0001)), 0.0), 1.0)
    zone_mid = _zone_mid(zone)
    close_location = float(confirmation_candle.close)
    location_quality = 1.0 if (side == 'BUY' and close_location >= max(zone_mid, float(zone.high))) or (side == 'SELL' and close_location <= min(zone_mid, float(zone.low))) else 0.5
    ratio = min(max((rejection_quality * 0.35) + (confirmation_quality * 0.45) + (location_quality * 0.20), 0.0), 1.0)
    return round(ratio * _SCORE_WEIGHTS['retest_confirmation'], 4), round(ratio, 4)


def _volatility_component(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> tuple[float, float]:
    ratio = _volatility_ratio(day_candles, idx, config)
    normalized = min(max(ratio / max(float(config.min_volatility_ratio), 0.0001), 0.0), 1.0)
    return round(normalized * _SCORE_WEIGHTS['volatility_quality'], 4), round(ratio, 4)


def _session_component(candle: Candle, config: DemandSupplyConfig) -> tuple[float, str]:
    window = session_window(
        candle.timestamp,
        morning_start=config.morning_session_start,
        morning_end=config.morning_session_end,
        midday_start=config.midday_start,
        midday_end=config.midday_end,
        allow_afternoon_session=bool(config.allow_afternoon_session),
        afternoon_start=config.afternoon_session_start,
        afternoon_end=config.afternoon_session_end,
    )
    return (_SCORE_WEIGHTS['session_quality'], window) if window == 'MORNING' else (0.0, window or 'BLOCKED')


def _zone_selection_score(day_candles: list[Candle], idx: int, zone: Zone, side: str, config: DemandSupplyConfig) -> float:
    freshness = _freshness_component(idx, zone, config)
    reaction_component, _ = _reaction_component(zone, day_candles, idx, side)
    bias = _higher_tf_bias(day_candles, idx)
    structure_ok, _ = _market_structure(day_candles, idx, side, config)
    bias_aligned = bias == ('BULLISH' if side == 'BUY' else 'BEARISH')
    structure_component, _ = _structure_component(day_candles, idx, zone, side, bias_aligned, structure_ok)
    base_score, base_candle_count, _ = _base_candle_profile(day_candles, idx, config)
    impulse_score, _, _ = _impulse_score(day_candles, idx, config)
    volume_component, _, _, _ = _volume_component(day_candles, idx, config)
    fresh_touch_component, _ = _fresh_touch_score(day_candles, zone, side, idx, config)
    time_component, _ = _time_component(base_candle_count, config)
    trend_component, _ = _trend_component(bias_aligned, structure_ok)
    zone_quality_component = round((_zone_quality_metrics(day_candles, idx, zone, side, base_candle_count, config).zone_quality_score / 10.0) * 1.6, 4)
    base_component = round((1.0 if base_score >= 2.0 else 0.5) * _SCORE_WEIGHTS['base_quality'], 4)
    impulse_component = round((1.0 if impulse_score >= 3.0 else 0.33) * _SCORE_WEIGHTS['impulse_quality'], 4)
    return round(
        freshness
        + reaction_component
        + structure_component
        + base_component
        + impulse_component
        + volume_component
        + fresh_touch_component
        + time_component
        + trend_component
        + zone_quality_component,
        4,
    )

def _quality_score(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch: bool,
    retest_confirmed: bool,
    touch_idx: int | None = None,
) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    if not touch or not retest_confirmed or touch_idx is None:
        return None

    candle = day_candles[idx]
    touch_candle = day_candles[touch_idx]
    trend_bias = _higher_tf_bias(day_candles, idx)
    bias_aligned = trend_bias == ('BULLISH' if side == 'BUY' else 'BEARISH')
    trend_ok = _trend_ok(day_candles, idx, side)
    structure_ok, structure_label = _market_structure(day_candles, idx, side, config)
    freshness_component = _freshness_component(idx, zone, config)
    reaction_component, reaction_metric = _reaction_component(zone, day_candles, idx, side)
    structure_component, structure_ratio = _structure_component(day_candles, idx, zone, side, bias_aligned, structure_ok)
    base_score, base_candle_count, base_range_threshold = _base_candle_profile(day_candles, idx, config)
    impulse_score, impulse_range, avg_candle_range = _impulse_score(day_candles, idx, config)
    volume_component, volume_spike, breakout_volume, avg_volume = _volume_component(day_candles, idx, config)
    fresh_touch_component, touch_count = _fresh_touch_score(day_candles, zone, side, idx, config)
    time_component, time_score = _time_component(base_candle_count, config)
    trend_component, trend_score = _trend_component(bias_aligned, trend_ok)
    vwap_component, vwap_ok = _vwap_component(candle, side)
    retest_component, retest_ratio = _retest_component(touch_candle, candle, zone, side, config)
    retest_score = _retest_hold_score(candle, zone, side, retest_confirmed)
    volatility_component, volatility_ratio = _volatility_component(day_candles, idx, config)
    session_component, session_name = _session_component(candle, config)
    zone_selection_score = _zone_selection_score(day_candles, idx, zone, side, config)
    rejection_score, rejection_diagnostics = _rejection_score(day_candles, touch_idx, touch_candle, zone, side, config)
    zone_quality_score, zone_quality_diagnostics = _zone_quality_score(day_candles, idx, zone, side, touch_count, base_candle_count, config)

    if config.require_vwap_alignment and not _vwap_aligned(candle, side, float(config.vwap_reclaim_buffer_pct)):
        return None
    if config.require_trend_bias and (not bias_aligned or not trend_ok):
        return None
    if config.require_market_structure and not structure_ok:
        return None
    if reaction_metric < float(config.min_reaction_strength):
        return None
    if session_component <= 0 or session_name != 'MORNING':
        return None
    if zone_selection_score < max(float(config.min_zone_selection_score), 5.0):
        return None
    expected_pattern = 'DBR' if side == 'BUY' else 'RBD'
    if str(zone.pattern or 'UNKNOWN').upper() != expected_pattern:
        return None
    if retest_ratio < 0.72:
        return None
    if volatility_ratio < float(config.min_volatility_ratio):
        return None
    if not _vwap_aligned(touch_candle, side):
        return None
    if rejection_score < float(config.min_rejection_score):
        return None
    if float(zone_quality_diagnostics['zone_width_pct']) > float(config.max_zone_width_pct):
        return None
    if int(zone_quality_diagnostics['base_candle_count']) > int(config.max_base_candles):
        return None
    if float(zone_quality_diagnostics['departure_ratio']) < float(config.min_departure_ratio):
        return None
    if int(zone_quality_diagnostics['retest_count']) > int(config.max_retest_count):
        return None
    if int(zone_quality_diagnostics['time_in_zone_bars']) > int(config.max_time_in_zone_bars):
        return None
    if zone_quality_score < float(config.min_zone_quality_score):
        return None

    components = {
        'freshness': freshness_component,
        'touch_freshness': fresh_touch_component,
        'reaction_strength': reaction_component,
        'structure_clarity': structure_component,
        'base_quality': round((1.0 if base_score >= 2.0 else 0.5) * _SCORE_WEIGHTS['base_quality'], 4),
        'impulse_quality': round((1.0 if impulse_score >= 3.0 else 0.33) * _SCORE_WEIGHTS['impulse_quality'], 4),
        'volume_quality': volume_component,
        'trend_alignment': trend_component,
        'time_quality': time_component,
        'vwap_alignment': round(vwap_component, 4),
        'retest_confirmation': retest_component,
        'volatility_quality': volatility_component,
        'session_quality': round(session_component, 4),
    }
    raw_total_score = round(base_score + impulse_score + (2.0 if volume_spike else 0.0) + (3.0 if touch_count == 0 else 1.0) + time_score + trend_score + retest_score, 2)
    total_score = round((raw_total_score + rejection_score + zone_quality_score) / 3.0, 2)
    score_interpretation = 'A_GRADE_TRADE' if rejection_score >= float(config.min_a_grade_score) and zone_quality_score >= float(config.min_a_grade_score) else _score_interpretation(raw_total_score)
    threshold = 10.0
    if total_score < threshold:
        return None

    diagnostics = {
        'trend_bias': trend_bias,
        'bias_aligned': bias_aligned,
        'vwap_aligned': vwap_ok,
        'trend_ok': trend_ok,
        'market_structure_ok': structure_ok,
        'zone_pattern': str(zone.pattern or 'UNKNOWN'),
        'market_structure_label': structure_label,
        'session_window': session_name,
        'freshness_ratio': round(_zone_freshness_ratio(idx, zone, config), 4),
        'reaction_score': reaction_metric,
        'structure_score': round(structure_ratio, 4),
        'retest_ratio': retest_ratio,
        'retest_score': round(retest_score, 2),
        'volatility_ratio': volatility_ratio,
        'volume_spike': volume_spike,
        'volume_score': 2 if volume_spike else 0,
        'breakout_volume': breakout_volume,
        'avg_volume': avg_volume,
        'touch_count': int(touch_count),
        'fresh_score': 3 if touch_count == 0 else 1,
        'base_score': round(base_score, 2),
        'base_candle_count': int(base_candle_count),
        'base_range_threshold': round(base_range_threshold, 4),
        'time_in_base': int(base_candle_count),
        'time_score': round(time_score, 2),
        'impulse_score': round(impulse_score, 2),
        'impulse_range': round(impulse_range, 4),
        'avg_candle_range': round(avg_candle_range, 4),
        'trend_score': round(trend_score, 2),
        'zone_strength_score': total_score,
        'raw_total_score': raw_total_score,
        'rejection_score': round(rejection_score, 2),
        'zone_quality_score': round(zone_quality_score, 2),
        'score_interpretation': score_interpretation,
        'zone_selection_score': round(zone_selection_score, 2),
        'score_threshold': threshold,
        **rejection_diagnostics,
        **zone_quality_diagnostics,
    }
    reason = (
        f'{side.lower()} demand_supply retest score={total_score:.2f} {score_interpretation.lower()} '
        f'rejection={rejection_score:.2f} zone={zone_quality_score:.2f} '
        f'freshness={components["freshness"]:.2f} reaction={components["reaction_strength"]:.2f} '
        f'structure={components["structure_clarity"]:.2f} {structure_label.lower()} retest={components["retest_confirmation"]:.2f}'
    )
    return total_score, components, reason, '', diagnostics

def score_zone(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch: bool,
    retest_confirmed: bool,
) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    if day_candles and any(getattr(candle, 'vwap', 0.0) in (0, 0.0, None) for candle in day_candles):
        calculate_vwap(day_candles)
    touch_idx = idx - 1 if idx > 0 else None
    return _quality_score(day_candles, idx, zone, side, config, touch=touch, retest_confirmed=retest_confirmed, touch_idx=touch_idx)


def _entry_levels(candle: Candle, zone: Zone, side: str, rr_ratio: float, avg_range: float, config: DemandSupplyConfig) -> tuple[float, float, float, float]:
    entry = float(candle.close)
    buffer = max(float(avg_range) * float(config.zone_buffer_atr_fraction), entry * float(config.zone_buffer_price_fraction))
    if side == 'BUY':
        stop = float(zone.low) - buffer
        if stop >= entry:
            stop = entry - max(buffer, float(avg_range) * 0.25)
        target = entry + (entry - stop) * float(rr_ratio or 2.0)
    else:
        stop = float(zone.high) + buffer
        if stop <= entry:
            stop = entry + max(buffer, float(avg_range) * 0.25)
        target = entry - (stop - entry) * float(rr_ratio or 2.0)
    return entry, stop, target, buffer


def _simulate_intraday_exit(day_candles: list[Candle], entry_idx: int, side: str, stop: float, target: float, trailing_sl_pct: float) -> tuple[float, Any, str, float]:
    trail_stop = float(stop)
    exit_price = float(day_candles[-1].close)
    exit_time = day_candles[-1].timestamp
    exit_reason = 'EOD'
    for follow_idx in range(entry_idx + 1, len(day_candles)):
        follow = day_candles[follow_idx]
        if trailing_sl_pct > 0:
            if side == 'BUY':
                trail_stop = max(trail_stop, float(follow.high) * (1.0 - trailing_sl_pct))
            else:
                trail_stop = min(trail_stop, float(follow.low) * (1.0 + trailing_sl_pct))
        if side == 'BUY':
            if float(follow.low) <= trail_stop:
                return float(trail_stop), follow.timestamp, 'TRAILING_STOP' if trail_stop > stop else 'STOP_LOSS', float(trail_stop)
            if float(follow.high) >= target:
                return float(target), follow.timestamp, 'TARGET', float(trail_stop)
        else:
            if float(follow.high) >= trail_stop:
                return float(trail_stop), follow.timestamp, 'TRAILING_STOP' if trail_stop < stop else 'STOP_LOSS', float(trail_stop)
            if float(follow.low) <= target:
                return float(target), follow.timestamp, 'TARGET', float(trail_stop)
    return exit_price, exit_time, exit_reason, float(trail_stop)


def generate_trades(
    df: Any,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: DemandSupplyConfig | None = None,
    *,
    trailing_sl_pct: float = 0.0,
    pivot_window: int = 2,
    touch_tolerance_pct: float = 0.006,
    max_trades_per_day: int = 1,
) -> list[dict[str, object]]:
    """Generate strict retest-only Nifty 5m demand/supply trades."""

    cfg = config or DemandSupplyConfig()
    if config is None:
        cfg.trailing_sl_pct = float(trailing_sl_pct)
        cfg.pivot_window = int(pivot_window)
        cfg.touch_tolerance_pct = float(touch_tolerance_pct)
        cfg.max_trades_per_day = int(max_trades_per_day)

    candles = _coerce_candles(df)
    if not candles:
        return []
    calculate_vwap(candles)
    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        day_candles = sorted(by_day[day], key=lambda candle: candle.timestamp)
        day_frame = candles_to_dataframe(day_candles)
        if len(day_candles) < (cfg.pivot_window * 2 + 7):
            continue
        zones = _find_zones(day_candles, cfg.pivot_window)
        if not zones:
            continue

        trades_taken = 0
        last_direction_bar: dict[str, int] = {'BUY': -10000, 'SELL': -10000}
        used_zone_keys: set[tuple[str, int, str]] = set()
        used_signal_keys: set[tuple[str, str, str, str, int]] = set()
        ranked_zones = sorted(
            zones,
            key=lambda zone: (_zone_selection_score(day_candles, min(len(day_candles) - 1, zone.idx + 2), zone, 'BUY' if zone.kind == 'demand' else 'SELL', cfg), zone.reaction_strength),
            reverse=True,
        )

        for zone in ranked_zones:
            if trades_taken >= int(cfg.max_trades_per_day or 1):
                break
            side = 'BUY' if zone.kind == 'demand' else 'SELL'
            zone_key = (str(day), zone.idx, side)
            if zone_key in used_zone_keys:
                continue
            if zone.idx + 2 >= len(day_candles):
                continue
            prequal_idx = min(len(day_candles) - 1, zone.idx + 2)
            zone_selection_score = _zone_selection_score(day_candles, prequal_idx, zone, side, cfg)
            if zone_selection_score < float(cfg.min_zone_selection_score):
                continue

            retest = detect_retest(day_candles, zone, side, zone.idx + 2, cfg)
            if retest is None:
                continue
            touch_idx, confirmation_idx, retest_state = retest
            if bool(retest_state.get('trade_created')) and zone_key in used_zone_keys:
                continue
            if confirmation_idx - last_direction_bar[side] < int(cfg.duplicate_signal_cooldown_bars):
                continue

            entry_candle = day_candles[confirmation_idx]
            touch_candle = day_candles[touch_idx]
            if not session_filter(entry_candle, cfg):
                continue

            score_result = _quality_score(
                day_candles,
                confirmation_idx,
                zone,
                side,
                cfg,
                touch=True,
                retest_confirmed=True,
                touch_idx=touch_idx,
            )
            if score_result is None:
                continue

            signal_key = ('DEMAND_SUPPLY', str(day), entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'), side, zone.idx)
            if signal_key in used_signal_keys:
                continue

            avg_range = max(_avg_range(day_candles, confirmation_idx, cfg.atr_window), 0.0001)
            entry, stop, target, buffer = _entry_levels(entry_candle, zone, side, rr_ratio, avg_range, cfg)
            validation_result = validate_zone(
                day_frame,
                zone_type=zone.kind,
                zone_low=float(zone.low),
                zone_high=float(zone.high),
                created_idx=int(zone.idx),
                touch_idx=int(touch_idx),
                entry_idx=int(confirmation_idx),
                entry_price=float(entry),
                stop_loss=float(stop),
                target_price=float(target),
                symbol='^NSEI',
                config=ZoneValidationConfig(
                    require_sweep=False,
                    allowed_sessions=('OPENING', 'OPENING_BUFFER', 'MORNING') if not cfg.allow_afternoon_session else ('OPENING', 'OPENING_BUFFER', 'MORNING', 'AFTERNOON'),
                    soft_score_threshold=6.0,
                    min_move_away_atr=0.8,
                    min_impulsive_candles=1,
                    max_base_departure_ratio=2.0,
                    max_touch_count=3,
                    min_imbalance_score=4.5,
                    min_rr_ratio=1.95,
                    max_penetration_pct=0.9,
                    max_zone_width_atr=1.4,
                    max_chop_score=0.70,
                    max_retest_delay=max(int(cfg.max_retest_bars) * 3, 12),
                ),
            )
            if str(validation_result.get('status', 'FAIL')).upper() != 'PASS':
                continue
            quantity = safe_quantity(capital=capital, risk_pct=risk_pct, entry=entry, stop_loss=stop)
            if quantity <= 0:
                continue

            score_value, components, reason, rejection_reason, diagnostics = score_result
            exit_price, exit_time, exit_reason, trail_stop = _simulate_intraday_exit(day_candles, confirmation_idx, side, stop, target, float(cfg.trailing_sl_pct or 0.0))
            pnl = (exit_price - entry) * quantity if side == 'BUY' else (entry - exit_price) * quantity
            trade = StandardTrade(
                timestamp=entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                side=side,
                entry=entry,
                stop_loss=stop,
                target=target,
                strategy='DEMAND_SUPPLY',
                reason=reason,
                score=score_value,
                entry_price=entry,
                target_price=target,
                risk_per_unit=abs(entry - stop),
                quantity=int(quantity),
                zone_type=zone.kind,
                extra={
                    'setup_type': 'retest',
                    'retest_only_entry': 'YES',
                    'entry_policy': 'RETEST_ONLY',
                    'symbol': '^NSEI',
                    'timeframe': '5m',
                    'day': day.isoformat(),
                    'zone_kind': zone.kind,
                    'zone_pattern': str(zone.pattern or 'UNKNOWN'),
                    'zone_low': round(float(zone.low), 4),
                    'zone_high': round(float(zone.high), 4),
                    'zone_buffer': round(buffer, 4),
                    'zone_reaction_strength': round(float(zone.reaction_strength), 4),
                    'score_threshold': round(float(cfg.min_zone_selection_score), 2),
                    'freshness_score': round(float(components['freshness']), 2),
                    'reaction_component': round(float(components['reaction_strength']), 2),
                    'structure_component': round(float(components['structure_clarity']), 2),
                    'base_component': round(float(components['base_quality']), 2),
                    'impulse_component': round(float(components['impulse_quality']), 2),
                    'vwap_component': round(float(components['vwap_alignment']), 2),
                    'retest_component': round(float(components['retest_confirmation']), 2),
                    'volatility_component': round(float(components['volatility_quality']), 2),
                    'session_component': round(float(components['session_quality']), 2),
                    'zone_strength_score': round(float(diagnostics['zone_strength_score']), 2),
                    'total_score': round(float(diagnostics.get('total_zone_score', diagnostics['raw_total_score'])), 2),
                    'zone_status': str(diagnostics.get('zone_status', 'PASS')),
                    'zone_fail_reasons': ','.join(str(item) for item in diagnostics.get('zone_fail_reasons', []) or []),
                    'validation_status': str(validation_result.get('status', 'PASS')),
                    'validation_score': round(float(validation_result.get('validation_score', 0.0)), 2),
                    'validation_fail_reasons': ','.join(str(item) for item in validation_result.get('fail_reasons', []) or []),
                    'validation_explanation': str(validation_result.get('explanation', '')),
                    'score_interpretation': str(diagnostics['score_interpretation']),
                    'zone_selection_score': round(float(zone_selection_score), 2),
                    'zone_gate_score': round(float(zone_selection_score), 2),
                    'zone_prequalification_floor': round(float(cfg.min_zone_selection_score), 2),
                    'zone_gate_threshold': round(float(cfg.min_zone_selection_score), 2),
                    'zone_prequalified': 'YES',
                    'structure_score': round(float(diagnostics['structure_score']), 4),
                    'rejection_score': round(float(diagnostics['rejection_score']), 2),
                    'zone_quality_score': round(float(diagnostics['zone_quality_score']), 2),
                    'zone_quality_label': str(diagnostics['zone_quality_label']),
                    'freshness_weight_raw': round(float(diagnostics['freshness_weight_raw']), 2),
                    'freshness_weight_pct': round(float(diagnostics['freshness_weight_pct']), 2),
                    'freshness_label': str(diagnostics['freshness_label']),
                    'bars_since_creation': int(diagnostics['bars_since_creation']),
                    'move_away_score': round(float(diagnostics['move_away_score']), 2),
                    'move_away_label': str(diagnostics['move_away_label']),
                    'departure_speed_bars': int(diagnostics['departure_speed_bars']),
                    'strong_departure_candles': int(diagnostics['strong_departure_candles']),
                    'post_departure_overlap_ratio': round(float(diagnostics['post_departure_overlap_ratio']), 4),
                    'internal_overlap_ratio': round(float(diagnostics['internal_overlap_ratio']), 4),
                    'pivot_cleanliness_score': round(float(diagnostics['pivot_cleanliness_score']), 2),
                    'edge_clarity_score': round(float(diagnostics['edge_clarity_score']), 2),
                    'structure_clarity_score': round(float(diagnostics['structure_clarity_score']), 2),
                    'structure_label': str(diagnostics['structure_label']),
                    'base_score': round(float(diagnostics['base_score']), 2),
                    'base_candle_count': int(diagnostics['base_candle_count']),
                    'base_range_threshold': round(float(diagnostics['base_range_threshold']), 4),
                    'impulse_score': round(float(diagnostics['impulse_score']), 2),
                    'impulse_range': round(float(diagnostics['impulse_range']), 4),
                    'avg_candle_range': round(float(diagnostics['avg_candle_range']), 4),
                    'volume_score': round(float(diagnostics['volume_score']), 2),
                    'zone_width_pct': round(float(diagnostics['zone_width_pct']), 4),
                    'departure_ratio': round(float(diagnostics['departure_ratio']), 4),
                    'retest_count': int(diagnostics['retest_count']),
                    'time_in_zone_bars': int(diagnostics['time_in_zone_bars']),
                    'wick_body_ratio': round(float(diagnostics['wick_body_ratio']), 4),
                    'wick_dominance_ratio': round(float(diagnostics['wick_dominance_ratio']), 4),
                    'close_position': round(float(diagnostics['close_position']), 4),
                    'penetration_ratio': round(float(diagnostics['penetration_ratio']), 4),
                    'rejection_expansion_ratio': round(float(diagnostics['rejection_expansion_ratio']), 4),
                    'volume_spike': 'YES' if bool(diagnostics['volume_spike']) else 'NO',
                    'breakout_volume': round(float(diagnostics['breakout_volume']), 4),
                    'avg_volume': round(float(diagnostics['avg_volume']), 4),
                    'touch_count': int(diagnostics['touch_count']),
                    'fresh_score': round(float(diagnostics['fresh_score']), 2),
                    'time_in_base': int(diagnostics['time_in_base']),
                    'time_score': round(float(diagnostics['time_score']), 2),
                    'trend_score': round(float(diagnostics['trend_score']), 2),
                    'retest_score': round(float(diagnostics['retest_score']), 2),
                    'freshness_ratio': round(float(diagnostics['freshness_ratio']), 4),
                    'reaction_score': round(float(diagnostics['reaction_score']), 4),
                    'retest_ratio': round(float(diagnostics['retest_ratio']), 4),
                    'volatility_ratio': round(float(diagnostics['volatility_ratio']), 4),
                    'trend_bias': str(diagnostics['trend_bias']),
                    'bias_aligned': 'YES' if bool(diagnostics['bias_aligned']) else 'NO',
                    'vwap_aligned': 'YES' if bool(diagnostics['vwap_aligned']) else 'NO',
                    'vwap_gate': 'PASS' if bool(diagnostics['vwap_aligned']) else 'FAIL',
                    'trend_alignment': 'YES' if bool(diagnostics['trend_ok']) else 'NO',
                    'market_structure_ok': 'YES' if bool(diagnostics['market_structure_ok']) else 'NO',
                    'market_structure_label': str(diagnostics['market_structure_label']),
                    'session_window': str(diagnostics['session_window']),
                    'session_allowed': 'YES',
                    'session_gate': 'PASS',
                    'session_filter_mode': 'MORNING_ONLY',
                    'retest_touch_time': touch_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'retest_confirmation_time': entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'first_touch_time': day_candles[int(retest_state.get('first_touch_idx', touch_idx))].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'zone_departure_time': day_candles[int(retest_state.get('departure_idx', touch_idx))].timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'retest_cycle_state': str(retest_state.get('last_event', 'trade')).upper(),
                    'first_touch_entry_allowed': 'NO',
                    'duplicate_signal_cooldown_bars': int(cfg.duplicate_signal_cooldown_bars),
                    'max_retest_bars': int(cfg.max_retest_bars),
                    'trailing_stop_loss': round(float(trail_stop), 4),
                    'exit_time': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'exit_price': round(float(exit_price), 4),
                    'exit_reason': exit_reason,
                    'pnl': round(float(pnl), 2),
                    'rejection_reason': rejection_reason,
                },
            ).to_dict()
            trades.append(trade)
            trades_taken += 1
            last_direction_bar[side] = confirmation_idx
            used_zone_keys.add(zone_key)
            used_signal_keys.add(signal_key)

    return trades





























def _cfg_float(config: DemandSupplyConfig, name: str, default: float) -> float:
    return float(getattr(config, name, default))


def _cfg_int(config: DemandSupplyConfig, name: str, default: int) -> int:
    return int(getattr(config, name, default))


def _meaningful_retest_indices(day_candles: list[Candle], zone: Zone, side: str, idx: int, config: DemandSupplyConfig) -> list[int]:
    departed = False
    active_retest = False
    indices: list[int] = []
    min_penetration = _cfg_float(config, 'min_retest_penetration_pct', 0.12)
    for candle_idx, candle in enumerate(day_candles[zone.idx + 1:min(idx, len(day_candles))], start=zone.idx + 1):
        if not departed:
            if _zone_departed(candle, zone, side, config):
                departed = True
            continue
        penetration = _penetration_ratio(candle, zone, side)
        touches = _touches_zone(candle, zone, side, float(config.touch_tolerance_pct))
        meaningful_touch = touches and penetration >= min_penetration
        if meaningful_touch and not active_retest:
            indices.append(candle_idx)
            active_retest = True
        elif not touches:
            active_retest = False
    return indices


def _alternating_direction_count(base_candles: list[Candle]) -> int:
    if len(base_candles) <= 1:
        return 0
    directions = [1 if float(c.close) >= float(c.open) else -1 for c in base_candles]
    return sum(1 for left, right in zip(directions, directions[1:]) if left != right)


def _structure_compactness_score(base_candle_count: int, zone_width_pct: float, internal_overlap_ratio: float, alternating_direction_count: int) -> float:
    raw = 0.0
    if base_candle_count <= 2:
        raw += 3.0
    elif base_candle_count <= 3:
        raw += 2.0
    elif base_candle_count <= 4:
        raw += 1.0
    if zone_width_pct <= 0.20:
        raw += 3.0
    elif zone_width_pct <= 0.30:
        raw += 2.0
    elif zone_width_pct <= 0.40:
        raw += 1.0
    if internal_overlap_ratio <= 0.25:
        raw += 2.5
    elif internal_overlap_ratio <= 0.40:
        raw += 1.5
    elif internal_overlap_ratio <= 0.55:
        raw += 0.5
    if alternating_direction_count <= 1:
        raw += 1.5
    elif alternating_direction_count <= 2:
        raw += 0.5
    return round(min(raw, 10.0), 2)


def _weak_zone_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str, base_candle_count: int, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    reference_price = max(abs(float(day_candles[idx].close)), 0.01)
    zone_width_pct = _zone_width_pct(zone, reference_price)
    departure_ratio = _departure_ratio(day_candles, zone, side, idx)
    time_in_zone_bars = _time_in_zone_bars(day_candles, zone, side, idx, config)
    post_departure_overlap_ratio = _post_departure_overlap_ratio(day_candles, zone, idx)
    raw = 0.0
    if zone_width_pct <= 0.20:
        raw += 2.5
    elif zone_width_pct <= 0.30:
        raw += 2.0
    elif zone_width_pct <= 0.40:
        raw += 1.0
    if departure_ratio >= 3.0:
        raw += 2.5
    elif departure_ratio >= 2.0:
        raw += 2.0
    elif departure_ratio >= 1.2:
        raw += 1.0
    if base_candle_count <= 2:
        raw += 2.0
    elif base_candle_count <= 3:
        raw += 1.5
    elif base_candle_count <= 4:
        raw += 0.5
    if time_in_zone_bars <= 2:
        raw += 1.5
    elif time_in_zone_bars <= 3:
        raw += 1.0
    elif time_in_zone_bars <= 4:
        raw += 0.5
    if post_departure_overlap_ratio <= 0.15:
        raw += 1.5
    elif post_departure_overlap_ratio <= 0.30:
        raw += 1.0
    elif post_departure_overlap_ratio <= 0.45:
        raw += 0.5
    score = round(min(raw, 10.0), 2)
    return score, {
        'zone_width_pct': round(zone_width_pct, 4),
        'departure_ratio': round(departure_ratio, 4),
        'base_candle_count': int(base_candle_count),
        'time_in_zone_bars': int(time_in_zone_bars),
        'post_departure_overlap_ratio': round(post_departure_overlap_ratio, 4),
        'weak_zone_score': score,
    }


def _retest_quality_metrics(day_candles: list[Candle], zone: Zone, side: str, touch_idx: int, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    touch_candle = day_candles[touch_idx]
    retest_indices = _meaningful_retest_indices(day_candles, zone, side, touch_idx, config)
    retest_count = len(retest_indices)
    penetration = _penetration_ratio(touch_candle, zone, side)
    if penetration <= 0.35:
        depth_score = 3.0
    elif penetration <= 0.55:
        depth_score = 2.0
    elif penetration <= 0.75:
        depth_score = 1.0
    else:
        depth_score = 0.0
    departure_idx = next((i for i in range(zone.idx + 1, touch_idx + 1) if _zone_departed(day_candles[i], zone, side, config)), zone.idx)
    reference_idx = retest_indices[-1] if retest_indices else departure_idx
    spacing_bars = max(touch_idx - reference_idx, 0)
    if spacing_bars >= 6:
        spacing_score = 2.0
    elif spacing_bars >= 3:
        spacing_score = 1.0
    else:
        spacing_score = 0.0
    freshness_penalty = 0.0 if retest_count == 0 else 1.5 if retest_count == 1 else 3.5 if retest_count == 2 else 5.0
    freshness_base = 5.0 if retest_count == 0 else 3.0 if retest_count == 1 else 1.0 if retest_count == 2 else 0.0
    score = round(max(min(freshness_base + depth_score + spacing_score - freshness_penalty, 10.0), 0.0), 2)
    if retest_count == 0 and score >= 7.5:
        label = 'FRESH'
    elif retest_count <= 1 and score >= 5.5:
        label = 'TESTED'
    elif retest_count <= 2 and score >= 3.0:
        label = 'WEAKENING'
    else:
        label = 'EXHAUSTED'
    return score, {
        'retest_count': int(retest_count),
        'retest_depth_score': round(depth_score, 2),
        'retest_spacing_bars': int(spacing_bars),
        'retest_freshness_penalty': round(freshness_penalty, 2),
        'retest_quality_score': score,
        'retest_label': label,
        'retest_penetration_pct': round(penetration, 4),
    }


def _rejection_score(day_candles: list[Candle], idx: int, candle: Candle, zone: Zone, side: str, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    body_size = max(abs(float(candle.close) - float(candle.open)), SMALL_VALUE)
    candle_range = max(float(candle.high) - float(candle.low), SMALL_VALUE)
    upper_wick = max(float(candle.high) - max(float(candle.open), float(candle.close)), 0.0)
    lower_wick = max(min(float(candle.open), float(candle.close)) - float(candle.low), 0.0)
    wick_to_body_ratio = (lower_wick / body_size) if side == 'BUY' else (upper_wick / body_size)
    wick_dominance_ratio = (lower_wick / max(upper_wick, SMALL_VALUE)) if side == 'BUY' else (upper_wick / max(lower_wick, SMALL_VALUE))
    close_position = _close_position(candle)
    close_strength_value = close_position if side == 'BUY' else 1.0 - close_position
    if side == 'BUY':
        close_strength_score = 2.0 if close_position >= 0.70 else 1.0 if close_position >= 0.60 else 0.0
    else:
        close_strength_score = 2.0 if close_position <= 0.30 else 1.0 if close_position <= 0.40 else 0.0
    candle_expansion_ratio = _rejection_expansion_ratio(day_candles, idx, candle, config)
    rejection_penetration = _penetration_ratio(candle, zone, side)
    opposite_side_failure = (upper_wick / candle_range) > 0.25 if side == 'BUY' else (lower_wick / candle_range) > 0.25
    score = 0.0
    if wick_to_body_ratio >= 2.5:
        score += 2.0
    elif wick_to_body_ratio >= 2.0:
        score += 1.0
    if wick_dominance_ratio >= 1.5:
        score += 2.0
    score += close_strength_score
    if candle_expansion_ratio >= 1.5:
        score += 2.0
    elif candle_expansion_ratio >= 1.2:
        score += 1.0
    if 0.10 <= rejection_penetration <= 0.50:
        score += 2.0
    elif 0.10 <= rejection_penetration <= 0.70:
        score += 1.0
    if opposite_side_failure:
        score = max(score - 1.0, 0.0)
    score = round(min(score, 10.0), 2)
    if score >= 8.0:
        label = 'STRONG'
    elif score >= 6.0:
        label = 'OK'
    elif score >= 4.0:
        label = 'WEAK'
    else:
        label = 'INVALID'
    return score, {
        'body_size': round(body_size, 4),
        'candle_range': round(candle_range, 4),
        'upper_wick': round(upper_wick, 4),
        'lower_wick': round(lower_wick, 4),
        'wick_to_body_ratio': round(wick_to_body_ratio, 4),
        'wick_body_ratio': round(wick_to_body_ratio, 4),
        'wick_dominance_ratio': round(wick_dominance_ratio, 4),
        'close_position': round(close_position, 4),
        'close_strength_score': round(close_strength_score, 2),
        'close_strength_value': round(close_strength_value, 4),
        'candle_expansion_ratio': round(candle_expansion_ratio, 4),
        'rejection_penetration': round(rejection_penetration, 4),
        'opposite_side_failure': bool(opposite_side_failure),
        'rejection_score': score,
        'rejection_label': label,
        'rejection_expansion_ratio': round(candle_expansion_ratio, 4),
        'penetration_ratio': round(rejection_penetration, 4),
    }


def _structure_clarity_metrics(day_candles: list[Candle], idx: int, zone: Zone, side: str, base_candle_count: int, config: DemandSupplyConfig) -> tuple[float, dict[str, object]]:
    reference_price = max(abs(float(day_candles[idx].close)), 0.01)
    zone_width_pct = _zone_width_pct(zone, reference_price)
    base_candles = _base_sample(day_candles, zone, config)
    effective_base_candle_count = len(base_candles) or base_candle_count
    internal_overlap_ratio = _internal_overlap_ratio(base_candles)
    alternating_direction_count = _alternating_direction_count(base_candles)
    pivot_cleanliness_score = _pivot_cleanliness_score(base_candles)
    edge_clarity_score = _edge_clarity_score(base_candles, zone)
    structure_compactness_score = _structure_compactness_score(effective_base_candle_count, zone_width_pct, internal_overlap_ratio, alternating_direction_count)
    raw = structure_compactness_score * 0.45 + pivot_cleanliness_score * 1.4 + edge_clarity_score * 1.6
    score = round(min(raw, 10.0), 2)
    if score >= 8.0:
        label = 'CLEAN'
    elif score >= 6.0:
        label = 'ACCEPTABLE'
    elif score >= 4.0:
        label = 'MESSY'
    else:
        label = 'INVALID'
    return score, {
        'base_candle_count': int(effective_base_candle_count),
        'zone_width_pct': round(zone_width_pct, 4),
        'internal_overlap_ratio': round(internal_overlap_ratio, 4),
        'alternating_direction_count': int(alternating_direction_count),
        'pivot_cleanliness_score': round(pivot_cleanliness_score, 2),
        'edge_clarity_score': round(edge_clarity_score, 2),
        'structure_compactness_score': round(structure_compactness_score, 2),
        'structure_clarity_score': score,
        'structure_label': label,
    }


def _zone_grade(zone_final_score: float, hard_reject: bool) -> str:
    if hard_reject or zone_final_score < 5.5:
        return 'REJECT'
    if zone_final_score >= 8.5:
        return 'A_GRADE'
    if zone_final_score >= 7.0:
        return 'B_GRADE'
    return 'C_GRADE'


def _quality_score(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch: bool,
    retest_confirmed: bool,
    touch_idx: int | None = None,
) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    if not touch or not retest_confirmed or touch_idx is None:
        return None

    candle = day_candles[idx]
    touch_candle = day_candles[touch_idx]
    trend_bias = _higher_tf_bias(day_candles, idx)
    bias_aligned = trend_bias == ('BULLISH' if side == 'BUY' else 'BEARISH')
    trend_ok = _trend_ok(day_candles, idx, side)
    structure_ok, structure_label = _market_structure(day_candles, idx, side, config)
    freshness_component = _freshness_component(idx, zone, config)
    reaction_component, reaction_metric = _reaction_component(zone, day_candles, idx, side)
    structure_component, structure_ratio = _structure_component(day_candles, idx, zone, side, bias_aligned, structure_ok)
    base_score, base_candle_count, base_range_threshold = _base_candle_profile(day_candles, idx, config)
    impulse_score, impulse_range, avg_candle_range = _impulse_score(day_candles, idx, config)
    volume_component, volume_spike, breakout_volume, avg_volume = _volume_component(day_candles, idx, config)
    fresh_touch_component, touch_count = _fresh_touch_score(day_candles, zone, side, idx, config)
    time_component, time_score = _time_component(base_candle_count, config)
    trend_component, trend_score = _trend_component(bias_aligned, trend_ok)
    vwap_component, vwap_ok = _vwap_component(candle, side)
    retest_component, retest_ratio = _retest_component(touch_candle, candle, zone, side, config)
    retest_score = _retest_hold_score(candle, zone, side, retest_confirmed)
    volatility_component, volatility_ratio = _volatility_component(day_candles, idx, config)
    session_component, session_name = _session_component(candle, config)
    zone_selection_score = _zone_selection_score(day_candles, idx, zone, side, config)
    rejection_score, rejection_diagnostics = _rejection_score(day_candles, touch_idx, touch_candle, zone, side, config)
    weak_zone_score, weak_zone_diagnostics = _weak_zone_metrics(day_candles, idx, zone, side, base_candle_count, config)
    retest_quality_score, retest_quality_diagnostics = _retest_quality_metrics(day_candles, zone, side, touch_idx, config)
    structure_clarity_score, structure_clarity_diagnostics = _structure_clarity_metrics(day_candles, idx, zone, side, base_candle_count, config)

    zone_final_score = round((weak_zone_score * 0.30) + (retest_quality_score * 0.20) + (rejection_score * 0.30) + (structure_clarity_score * 0.20), 2)
    rejection_reasons: list[str] = []

    if config.require_vwap_alignment and not _vwap_aligned(candle, side, float(config.vwap_reclaim_buffer_pct)):
        rejection_reasons.append('reject: vwap alignment failed')
    if config.require_trend_bias and (not bias_aligned or not trend_ok):
        rejection_reasons.append('reject: trend bias not aligned')
    if config.require_market_structure and not structure_ok:
        rejection_reasons.append('reject: market structure weak')
    if reaction_metric < float(config.min_reaction_strength):
        rejection_reasons.append('reject: reaction strength too low')
    if session_component <= 0 or session_name != 'MORNING':
        rejection_reasons.append('reject: invalid session window')
    if zone_selection_score < max(float(config.min_zone_selection_score), 5.0):
        rejection_reasons.append('reject: zone prequalification too weak')
    expected_pattern = 'DBR' if side == 'BUY' else 'RBD'
    if str(zone.pattern or 'UNKNOWN').upper() != expected_pattern:
        rejection_reasons.append('reject: wrong zone pattern')
    if retest_ratio < 0.72:
        rejection_reasons.append('reject: retest confirmation too weak')
    if volatility_ratio < float(config.min_volatility_ratio):
        rejection_reasons.append('reject: volatility too low')
    if not _vwap_aligned(touch_candle, side):
        rejection_reasons.append('reject: retest candle lost vwap alignment')

    max_zone_width_pct = _cfg_float(config, 'max_zone_width_pct', 0.20)
    min_departure_ratio = _cfg_float(config, 'min_departure_ratio', 3.0)
    max_base_candles = _cfg_int(config, 'max_base_candles', 2)
    max_time_in_zone_bars = _cfg_int(config, 'max_time_in_zone_bars', 2)
    max_allowed_retests = _cfg_int(config, 'max_allowed_retests', _cfg_int(config, 'max_retest_count', 1))
    min_retest_penetration_pct = _cfg_float(config, 'min_retest_penetration_pct', 0.12)
    min_wick_to_body_ratio = _cfg_float(config, 'min_wick_to_body_ratio', 2.0)
    min_wick_dominance_ratio = _cfg_float(config, 'min_wick_dominance_ratio', 1.5)
    min_candle_expansion_ratio = _cfg_float(config, 'min_candle_expansion_ratio', 1.2)
    max_internal_overlap_ratio = _cfg_float(config, 'max_internal_overlap_ratio', 0.55)
    max_alternating_direction_count = _cfg_int(config, 'max_alternating_direction_count', 2)
    min_edge_clarity_score = _cfg_float(config, 'min_edge_clarity_score', 1.0)
    min_structure_clarity_score = _cfg_float(config, 'min_structure_clarity_score', 6.0)
    min_zone_final_score = _cfg_float(config, 'min_zone_final_score', 7.0)

    if float(weak_zone_diagnostics['zone_width_pct']) > max_zone_width_pct:
        rejection_reasons.append('reject: zone too wide')
    if float(weak_zone_diagnostics['departure_ratio']) < min_departure_ratio:
        rejection_reasons.append('reject: departure too weak')
    if int(weak_zone_diagnostics['base_candle_count']) > max_base_candles:
        rejection_reasons.append('reject: base has too many candles')
    if int(weak_zone_diagnostics['time_in_zone_bars']) > max_time_in_zone_bars:
        rejection_reasons.append('reject: too much time spent in zone')
    if float(weak_zone_diagnostics['post_departure_overlap_ratio']) > 0.45:
        rejection_reasons.append('reject: too much post-departure overlap')

    if int(retest_quality_diagnostics['retest_count']) > max_allowed_retests:
        rejection_reasons.append('reject: too many meaningful retests')
    if float(retest_quality_diagnostics['retest_penetration_pct']) < min_retest_penetration_pct:
        rejection_reasons.append('reject: retest penetration too shallow')
    if float(retest_quality_diagnostics['retest_penetration_pct']) > _cfg_float(config, 'max_penetration_ratio', 0.85):
        rejection_reasons.append('reject: retest too deep')
    if int(retest_quality_diagnostics['retest_spacing_bars']) <= 1 and int(retest_quality_diagnostics['retest_count']) >= 1:
        rejection_reasons.append('reject: retests are too compressed')

    if float(rejection_diagnostics['wick_to_body_ratio']) < min_wick_to_body_ratio:
        rejection_reasons.append('reject: rejection candle weak')
    if float(rejection_diagnostics['wick_dominance_ratio']) < min_wick_dominance_ratio:
        rejection_reasons.append('reject: rejection wick not dominant')
    if float(rejection_diagnostics['candle_expansion_ratio']) < min_candle_expansion_ratio:
        rejection_reasons.append('reject: rejection candle too small')
    if float(rejection_diagnostics['close_strength_score']) <= 0:
        rejection_reasons.append('reject: candle close not strong enough')
    if bool(rejection_diagnostics['opposite_side_failure']):
        rejection_reasons.append('reject: opposite wick too large')
    if float(rejection_diagnostics['rejection_penetration']) > _cfg_float(config, 'max_penetration_ratio', 0.85):
        rejection_reasons.append('reject: rejection penetration too deep')

    if float(structure_clarity_diagnostics['internal_overlap_ratio']) > max_internal_overlap_ratio:
        rejection_reasons.append('reject: too much base overlap')
    if int(structure_clarity_diagnostics['alternating_direction_count']) > max_alternating_direction_count:
        rejection_reasons.append('reject: structure too messy')
    if float(structure_clarity_diagnostics['edge_clarity_score']) < min_edge_clarity_score:
        rejection_reasons.append('reject: edge clarity too weak')
    if float(structure_clarity_diagnostics['structure_clarity_score']) < min_structure_clarity_score:
        rejection_reasons.append('reject: structure clarity too low')

    hard_reject = len(rejection_reasons) > 0
    zone_grade = _zone_grade(zone_final_score, hard_reject)
    if zone_final_score < min_zone_final_score and zone_grade != 'REJECT':
        rejection_reasons.append('reject: final zone score below threshold')
        zone_grade = 'REJECT'
        hard_reject = True

    components = {
        'freshness': freshness_component,
        'touch_freshness': fresh_touch_component,
        'reaction_strength': reaction_component,
        'structure_clarity': structure_component,
        'base_quality': round((1.0 if base_score >= 2.0 else 0.5) * _SCORE_WEIGHTS['base_quality'], 4),
        'impulse_quality': round((1.0 if impulse_score >= 3.0 else 0.33) * _SCORE_WEIGHTS['impulse_quality'], 4),
        'volume_quality': volume_component,
        'trend_alignment': trend_component,
        'time_quality': time_component,
        'vwap_alignment': round(vwap_component, 4),
        'retest_confirmation': retest_component,
        'volatility_quality': volatility_component,
        'session_quality': round(session_component, 4),
    }
    raw_total_score = round(base_score + impulse_score + (2.0 if volume_spike else 0.0) + (3.0 if touch_count == 0 else 1.0) + time_score + trend_score + retest_score, 2)
    total_score = round((raw_total_score + rejection_score + zone_final_score) / 3.0, 2)
    score_interpretation = zone_grade if zone_grade != 'REJECT' else 'REJECT'
    threshold = max(10.0, min_zone_final_score)
    if hard_reject or total_score < threshold:
        return None

    diagnostics = {
        'trend_bias': trend_bias,
        'bias_aligned': bias_aligned,
        'vwap_aligned': vwap_ok,
        'trend_ok': trend_ok,
        'market_structure_ok': structure_ok,
        'zone_pattern': str(zone.pattern or 'UNKNOWN'),
        'market_structure_label': structure_label,
        'session_window': session_name,
        'freshness_ratio': round(_zone_freshness_ratio(idx, zone, config), 4),
        'reaction_score': reaction_metric,
        'structure_score': round(structure_ratio, 4),
        'retest_ratio': retest_ratio,
        'retest_score': round(retest_score, 2),
        'volatility_ratio': volatility_ratio,
        'volume_spike': volume_spike,
        'volume_score': 2 if volume_spike else 0,
        'breakout_volume': breakout_volume,
        'avg_volume': avg_volume,
        'touch_count': int(touch_count),
        'fresh_score': 3 if touch_count == 0 else 1,
        'base_score': round(base_score, 2),
        'base_candle_count': int(base_candle_count),
        'base_range_threshold': round(base_range_threshold, 4),
        'time_in_base': int(base_candle_count),
        'time_score': round(time_score, 2),
        'impulse_score': round(impulse_score, 2),
        'impulse_range': round(impulse_range, 4),
        'avg_candle_range': round(avg_candle_range, 4),
        'trend_score': round(trend_score, 2),
        'zone_strength_score': total_score,
        'raw_total_score': raw_total_score,
        'zone_final_score': round(zone_final_score, 2),
        'zone_grade': zone_grade,
        'zone_decision': 'ALLOW',
        'zone_quality_score': round(zone_final_score, 2),
        'zone_quality_label': zone_grade,
        'weak_zone_score': round(weak_zone_score, 2),
        'retest_quality_score': round(retest_quality_score, 2),
        'rejection_score': round(rejection_score, 2),
        'structure_clarity_score': round(structure_clarity_score, 2),
        'score_interpretation': score_interpretation,
        'zone_selection_score': round(zone_selection_score, 2),
        'score_threshold': threshold,
        'rejection_reasons': [],
        'zone_evaluation_summary': (
            f'Zone Evaluation Summary | Zone Width: {float(weak_zone_diagnostics["zone_width_pct"]):.2f}% | '
            f'Departure Ratio: {float(weak_zone_diagnostics["departure_ratio"]):.2f} | '
            f'Retests: {int(retest_quality_diagnostics["retest_count"])} | '
            f'Retest Quality: {float(retest_quality_score):.2f} | '
            f'Rejection Score: {float(rejection_score):.2f} | '
            f'Structure Clarity: {float(structure_clarity_score):.2f} | '
            f'Final Grade: {zone_grade}'
        ),
        **weak_zone_diagnostics,
        **retest_quality_diagnostics,
        **rejection_diagnostics,
        **structure_clarity_diagnostics,
    }
    reason = (
        f'{side.lower()} demand_supply retest score={total_score:.2f} {zone_grade.lower()} '
        f'weak_zone={weak_zone_score:.2f} retest={retest_quality_score:.2f} '
        f'rejection={rejection_score:.2f} structure={structure_clarity_score:.2f}'
    )
    return total_score, components, reason, '', diagnostics

ZONE_SMALL_VALUE = 1e-4
SMALL_VALUE = ZONE_SMALL_VALUE



FAIL_SCORE_THRESHOLD = 24.0


def _zone_bos(day_candles: list[Candle], idx: int, zone: Zone, side: str, config: DemandSupplyConfig) -> bool:
    highs, lows = _recent_swings(day_candles, idx, config.structure_swing_window)
    if side == 'BUY':
        reference = max(highs) if highs else max(float(c.high) for c in day_candles[max(zone.idx - 3, 0):zone.idx + 1])
        return any(float(c.high) > float(reference) for c in day_candles[zone.idx + 1:min(idx + 1, len(day_candles))])
    reference = min(lows) if lows else min(float(c.low) for c in day_candles[max(zone.idx - 3, 0):zone.idx + 1])
    return any(float(c.low) < float(reference) for c in day_candles[zone.idx + 1:min(idx + 1, len(day_candles))])



def _liquidity_sweep(day_candles: list[Candle], touch_idx: int, side: str) -> bool:
    if touch_idx <= 0:
        return False
    candle = day_candles[touch_idx]
    lookback = day_candles[max(0, touch_idx - 3):touch_idx]
    if not lookback:
        return False
    if side == 'BUY':
        return float(candle.low) < min(float(item.low) for item in lookback)
    return float(candle.high) > max(float(item.high) for item in lookback)



def _imbalance_present(day_candles: list[Candle], zone: Zone, side: str, idx: int) -> bool:
    sample = day_candles[zone.idx:min(len(day_candles), zone.idx + 4, idx + 1)]
    if len(sample) < 3:
        return False
    for left, middle, right in zip(sample, sample[1:], sample[2:]):
        if side == 'BUY' and float(right.low) > float(left.high):
            return True
        if side == 'SELL' and float(right.high) < float(left.low):
            return True
    return False



def _zone_failure_evaluation(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch_idx: int,
    rr_ratio: float,
) -> tuple[list[str], dict[str, object]]:
    candle = day_candles[idx]
    touch_candle = day_candles[touch_idx]
    trend_bias = _higher_tf_bias(day_candles, idx)
    bias_aligned = trend_bias == ('BULLISH' if side == 'BUY' else 'BEARISH')
    trend_ok = _trend_ok(day_candles, idx, side)
    structure_ok, market_structure_label = _market_structure(day_candles, idx, side, config)
    base_score, base_candle_count, base_range_threshold = _base_candle_profile(day_candles, idx, config)
    impulse_score, impulse_range, avg_candle_range = _impulse_score(day_candles, idx, config)
    volume_component, volume_spike, breakout_volume, avg_volume = _volume_component(day_candles, idx, config)
    fresh_touch_component, touch_count = _fresh_touch_score(day_candles, zone, side, idx, config)
    time_component, time_score = _time_component(base_candle_count, config)
    trend_component, trend_score = _trend_component(bias_aligned, trend_ok)
    vwap_component, vwap_ok = _vwap_component(candle, side)
    retest_component, retest_ratio = _retest_component(touch_candle, candle, zone, side, config)
    retest_score = _retest_hold_score(candle, zone, side, True)
    volatility_component, volatility_ratio = _volatility_component(day_candles, idx, config)
    session_component, session_name = _session_component(candle, config)
    zone_selection_score = _zone_selection_score(day_candles, idx, zone, side, config)
    rejection_score, rejection_diagnostics = _rejection_score(day_candles, touch_idx, touch_candle, zone, side, config)
    zone_quality_score, zone_quality_diagnostics = _zone_quality_score(day_candles, idx, zone, side, touch_count, base_candle_count, config)
    move_strength = max(float(zone_quality_diagnostics.get('move_away_score', 0.0)), 0.0)
    rejection_strength = float(rejection_score)
    structure_score = float(zone_quality_diagnostics.get('structure_clarity_score', 0.0))
    bos = _zone_bos(day_candles, idx, zone, side, config)
    liquidity_sweep = _liquidity_sweep(day_candles, touch_idx, side)
    imbalance_present = _imbalance_present(day_candles, zone, side, idx)
    zone_width_pct = float(zone_quality_diagnostics.get('zone_width_pct', 999.0))
    internal_overlap_ratio = float(zone_quality_diagnostics.get('internal_overlap_ratio', 1.0))
    vwap_extension_pct = abs(float(candle.close) - float(candle.vwap)) / max(abs(float(candle.vwap)), SMALL_VALUE)
    meaningful_retests = int(zone_quality_diagnostics.get('retest_count', 0) or 0)
    freshness_points = 10.0 if meaningful_retests == 0 else 6.0 if meaningful_retests == 1 else 2.0 if meaningful_retests == 2 else 0.0
    total_zone_score = move_strength + freshness_points + rejection_strength + structure_score + (8.0 if bias_aligned and trend_ok else 0.0)

    fail_reasons: list[str] = []
    departure_ratio = float(zone_quality_diagnostics.get('departure_ratio', 0.0))
    strong_departure_candles = int(zone_quality_diagnostics.get('strong_departure_candles', 0) or 0)

    if move_strength < 1.25 and departure_ratio < 0.8 and strong_departure_candles < 2 and not imbalance_present:
        fail_reasons.append('weak_move_away')
    if not bos:
        fail_reasons.append('no_bos')
    if int(base_candle_count) > 5 or internal_overlap_ratio > 0.55:
        fail_reasons.append('dirty_base')
    if int(zone_quality_diagnostics.get('retest_count', 0)) >= 2:
        fail_reasons.append('zone_not_fresh')
    if rejection_strength < 4.0:
        fail_reasons.append('weak_rejection')
    if structure_score < 4.5 or (not structure_ok and str(market_structure_label or '') not in {'INSUFFICIENT'}):
        fail_reasons.append('unclear_structure')
    if not bias_aligned or not trend_ok:
        fail_reasons.append('trend_misaligned')
    if not liquidity_sweep and (move_strength < 1.25 or rejection_strength < 4.0 or structure_score < 4.5):
        fail_reasons.append('no_liquidity_sweep')
    if not imbalance_present and departure_ratio < 1.0 and strong_departure_candles < 2:
        fail_reasons.append('no_imbalance')
    if volatility_ratio < float(config.min_volatility_ratio):
        fail_reasons.append('low_volatility')
    if float(rr_ratio) < 2.0:
        fail_reasons.append('bad_rr')
    if vwap_extension_pct > 0.025:
        fail_reasons.append('extended_move')

    diagnostics = {
        'move_strength': round(move_strength, 2),
        'break_of_structure': bool(bos),
        'rejection_strength': round(rejection_strength, 2),
        'structure_score': round(structure_score, 2),
        'trend_misaligned': not (bias_aligned and trend_ok),
        'liquidity_sweep': bool(liquidity_sweep),
        'imbalance_present': bool(imbalance_present),
        'rr_ratio_gate': round(float(rr_ratio), 2),
        'vwap_extension_pct': round(vwap_extension_pct * 100.0, 2),
        'total_zone_score': round(total_zone_score, 2),
        'zone_status': 'FAIL' if fail_reasons or total_zone_score < FAIL_SCORE_THRESHOLD else 'PASS',
        'zone_fail_reasons': fail_reasons,
        'zone_fail_count': len(fail_reasons),
        'zone_evaluation_summary': (
            f"Zone status={('FAIL' if fail_reasons or total_zone_score < FAIL_SCORE_THRESHOLD else 'PASS')} | "
            f"move={move_strength:.2f} bos={('YES' if bos else 'NO')} retests={meaningful_retests} "
            f"rejection={rejection_strength:.2f} structure={structure_score:.2f} rr={float(rr_ratio):.2f}"
        ),
        'trend_bias': trend_bias,
        'bias_aligned': bias_aligned,
        'vwap_aligned': vwap_ok,
        'trend_ok': trend_ok,
        'market_structure_ok': structure_ok,
        'market_structure_label': market_structure_label,
        'session_window': session_name,
        'reaction_score': round(float(zone.reaction_strength), 4),
        'retest_ratio': round(float(retest_ratio), 4),
        'retest_score': round(float(retest_score), 2),
        'volatility_ratio': round(float(volatility_ratio), 4),
        'volume_spike': volume_spike,
        'volume_score': round(float(volume_component), 2),
        'breakout_volume': round(float(breakout_volume), 4),
        'avg_volume': round(float(avg_volume), 4),
        'touch_count': int(touch_count),
        'fresh_score': round(float(fresh_touch_component), 2),
        'freshness_ratio': round(float(fresh_touch_component) / 10.0, 4),
        'base_score': round(float(base_score), 2),
        'base_candle_count': int(base_candle_count),
        'time_in_base': int(base_candle_count),
        'base_range_threshold': round(float(base_range_threshold), 4),
        'impulse_score': round(float(impulse_score), 2),
        'impulse_range': round(float(impulse_range), 4),
        'avg_candle_range': round(float(avg_candle_range), 4),
        'time_score': round(float(time_score), 2),
        'trend_score': round(float(trend_score), 2),
        'zone_strength_score': round(float(zone_selection_score), 2),
        'zone_selection_score': round(float(zone_selection_score), 2),
        'raw_total_score': round(float(zone_selection_score), 2),
        'zone_quality_score': round(float(zone_quality_score), 2),
        'zone_quality_label': str(zone_quality_diagnostics.get('zone_quality_label', 'REJECT')),
        'score_interpretation': 'REJECT' if fail_reasons or total_zone_score < FAIL_SCORE_THRESHOLD else 'A_GRADE_TRADE' if total_zone_score >= 42 else 'B_GRADE_TRADE',
        'score_threshold': float(FAIL_SCORE_THRESHOLD),
        'rejection_score': round(float(rejection_score), 2),
        'structure_score': round(float(structure_score), 4),
        **rejection_diagnostics,
        **zone_quality_diagnostics,
    }
    if total_zone_score < FAIL_SCORE_THRESHOLD and 'score_below_threshold' not in fail_reasons:
        fail_reasons.append('score_below_threshold')
        diagnostics['zone_fail_reasons'] = fail_reasons
        diagnostics['zone_fail_count'] = len(fail_reasons)
        diagnostics['zone_status'] = 'FAIL'
    return fail_reasons, diagnostics



def evaluate_zone_status(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch_idx: int,
    rr_ratio: float = 2.0,
) -> dict[str, object]:
    fail_reasons, diagnostics = _zone_failure_evaluation(day_candles, idx, zone, side, config, touch_idx=touch_idx, rr_ratio=rr_ratio)
    return dict(diagnostics, zone_fail_reasons=list(fail_reasons), zone_status='FAIL' if fail_reasons else str(diagnostics.get('zone_status', 'PASS')))



def _quality_score(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch: bool,
    retest_confirmed: bool,
    touch_idx: int | None = None,
) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    if not touch or not retest_confirmed or touch_idx is None:
        return None
    fail_reasons, diagnostics = _zone_failure_evaluation(day_candles, idx, zone, side, config, touch_idx=touch_idx, rr_ratio=2.0)
    if fail_reasons:
        return None
    components = {
        'freshness': round(float(diagnostics.get('freshness_weight_component', 0.0)), 4),
        'reaction_strength': round(min(float(diagnostics.get('reaction_score', 0.0)) / 1.1, 1.0) * _SCORE_WEIGHTS['reaction_strength'], 4),
        'structure_clarity': round(min(float(diagnostics.get('structure_clarity_score', 0.0)) / 10.0, 1.0) * _SCORE_WEIGHTS['structure_clarity'], 4),
        'base_quality': round((1.0 if float(diagnostics.get('base_score', 0.0)) >= 2.0 else 0.5) * _SCORE_WEIGHTS['base_quality'], 4),
        'impulse_quality': round((1.0 if float(diagnostics.get('impulse_score', 0.0)) >= 3.0 else 0.33) * _SCORE_WEIGHTS['impulse_quality'], 4),
        'volume_quality': round(float(diagnostics.get('volume_score', 0.0)), 4),
        'trend_alignment': round((1.0 if bool(diagnostics.get('bias_aligned')) and bool(diagnostics.get('trend_ok')) else 0.0) * _SCORE_WEIGHTS['trend_alignment'], 4),
        'time_quality': round(float(diagnostics.get('time_score', 0.0)) * 0.5, 4),
        'vwap_alignment': round((_SCORE_WEIGHTS['vwap_alignment'] if bool(diagnostics.get('vwap_aligned')) else 0.0), 4),
        'retest_confirmation': round(min(float(diagnostics.get('retest_ratio', 0.0)), 1.0) * _SCORE_WEIGHTS['retest_confirmation'], 4),
        'volatility_quality': round(min(float(diagnostics.get('volatility_ratio', 0.0)) / max(float(config.min_volatility_ratio), SMALL_VALUE), 1.0) * _SCORE_WEIGHTS['volatility_quality'], 4),
        'session_quality': round((_SCORE_WEIGHTS['session_quality'] if str(diagnostics.get('session_window')) == 'MORNING' else 0.0), 4),
    }
    total_score = round(float(diagnostics.get('total_zone_score', 0.0)), 2)
    reason = (
        f"{side.lower()} demand_supply zone_status=PASS score={total_score:.2f} "
        f"move={float(diagnostics.get('move_strength', 0.0)):.2f} fresh={max(0, 10 - int(diagnostics.get('touch_count', 0)) * 4):.2f} "
        f"rejection={float(diagnostics.get('rejection_strength', 0.0)):.2f} structure={float(diagnostics.get('structure_score', 0.0)):.2f}"
    )
    return total_score, components, reason, '', diagnostics


