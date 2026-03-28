from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.breakout_bot import Candle, _coerce_candles, add_intraday_vwap
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
    min_zone_selection_score: float = 5.00
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
                    pattern='DBR' if float(candle.close) >= float(candle.open) else 'UNKNOWN',
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
                    pattern='RBD' if float(candle.close) <= float(candle.open) else 'UNKNOWN',
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
    body_ok = _body_ratio(candle) >= float(config.min_confirmation_body_ratio) * 0.70
    if side == 'BUY':
        return _lower_wick_ratio(candle) >= float(config.min_rejection_wick_ratio) and body_ok and float(candle.close) >= zone_mid
    return _upper_wick_ratio(candle) >= float(config.min_rejection_wick_ratio) and body_ok and float(candle.close) <= zone_mid


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

        if int(state.get('first_touch_idx', -1)) < 0:
            if _touches_zone(candle, zone, side, float(config.touch_tolerance_pct)):
                state = mark_zone_retest_state(state, event='first_touch', candle_idx=idx)
            continue

        if int(state.get('departure_idx', -1)) < 0:
            if _zone_departed(candle, zone, side, config):
                state = mark_zone_retest_state(state, event='departure', candle_idx=idx)
            continue

        if idx - int(state.get('departure_idx', -1)) > int(config.max_retest_bars):
            return None

        if not is_retest(day_candles, zone, side, state, idx, config):
            continue

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
        + trend_component,
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
    score_interpretation = _score_interpretation(raw_total_score)
    total_score = raw_total_score
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
        'raw_total_score': total_score,
        'score_interpretation': score_interpretation,
        'zone_selection_score': round(zone_selection_score, 2),
        'score_threshold': threshold,
    }
    reason = (
        f'{side.lower()} demand_supply retest score={total_score:.2f} {score_interpretation.lower()} '
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
                    'score_threshold': round(float(cfg.scoring.threshold()), 2),
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
                    'total_score': round(float(diagnostics['raw_total_score']), 2),
                    'score_interpretation': str(diagnostics['score_interpretation']),
                    'zone_selection_score': round(float(zone_selection_score), 2),
                    'zone_gate_score': round(float(zone_selection_score), 2),
                    'zone_prequalification_floor': round(float(cfg.min_zone_selection_score), 2),
                    'zone_gate_threshold': round(float(cfg.min_zone_selection_score), 2),
                    'zone_prequalified': 'YES',
                    'structure_score': round(float(diagnostics['structure_score']), 4),
                    'base_score': round(float(diagnostics['base_score']), 2),
                    'base_candle_count': int(diagnostics['base_candle_count']),
                    'base_range_threshold': round(float(diagnostics['base_range_threshold']), 4),
                    'impulse_score': round(float(diagnostics['impulse_score']), 2),
                    'impulse_range': round(float(diagnostics['impulse_range']), 4),
                    'avg_candle_range': round(float(diagnostics['avg_candle_range']), 4),
                    'volume_score': round(float(diagnostics['volume_score']), 2),
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



























