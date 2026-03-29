
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REQUIRED_COLUMNS = ('timestamp', 'open', 'high', 'low', 'close', 'volume')
FAIL_REASON_MESSAGES = {
    'invalid_input': 'market data was invalid or incomplete',
    'weak_move_away': 'departure from the zone was too weak',
    'no_break_of_structure': 'the zone did not break meaningful structure',
    'dirty_base': 'the base was too noisy or too wide',
    'not_fresh': 'the zone had already been consumed by prior touches',
    'weak_rejection': 'the retest reaction was too weak',
    'bad_structure': 'market structure around the zone was messy',
    'trend_misaligned': 'the setup fought higher-timeframe direction without enough reversal quality',
    'no_liquidity_sweep': 'the retest did not sweep nearby liquidity first',
    'no_imbalance': 'the zone did not create enough imbalance or displacement',
    'low_volatility': 'the market was too quiet for a clean zone reaction',
    'bad_rr': 'reward relative to risk was below the minimum',
    'late_retest': 'the retest happened too late after zone creation',
    'deep_penetration': 'the retest penetrated too deeply into the zone',
    'oversized_zone': 'the zone was too wide for efficient stop placement',
    'session_fail': 'entry timing was outside the allowed session window',
    'low_validation_score': 'the composite validation score was too low',
}


@dataclass(slots=True)
class ZoneValidationConfig:
    """Configurable thresholds for strict demand/supply zone validation."""

    atr_window: int = 14
    departure_window: int = 5
    large_candle_body_pct: float = 0.60
    impulsive_body_pct: float = 0.55
    impulsive_range_atr: float = 0.90
    min_move_away_atr: float = 1.5
    min_impulsive_candles: int = 2
    min_avg_body_pct: float = 0.50
    max_base_candles: int = 6
    max_base_departure_ratio: float = 0.45
    max_base_wick_ratio: float = 1.35
    max_base_overlap_ratio: float = 0.60
    max_touch_count: int = 1
    min_rejection_score: float = 6.0
    min_structure_score: float = 6.0
    max_chop_score: float = 0.58
    require_sweep: bool = False
    min_imbalance_score: float = 5.0
    min_atr_pct: float = 0.18
    min_volatility_percentile: float = 0.35
    min_rr_ratio: float = 2.0
    max_retest_delay: int = 12
    max_penetration_pct: float = 0.70
    max_zone_width_atr: float = 1.2
    allowed_sessions: tuple[str, ...] = ('OPENING', 'OPENING_BUFFER', 'MORNING')
    soft_score_threshold: float = 7.0
    reversal_score_threshold: float = 7.5
    swing_window: int = 2
    chop_lookback: int = 12
    ema_fast: int = 5
    ema_slow: int = 13
    volatility_lookback: int = 20
    logger_csv_path: str | None = None
    weights: dict[str, float] = field(default_factory=lambda: {
        'move_away_score': 0.20,
        'freshness_score': 0.15,
        'rejection_score': 0.20,
        'structure_score': 0.15,
        'trend_score': 0.10,
        'imbalance_score': 0.10,
        'rr_score': 0.10,
    })


@dataclass(slots=True)
class ZoneValidationLogger:
    rows: list[dict[str, object]] = field(default_factory=list)

    def log(self, result: dict[str, Any]) -> None:
        if str(result.get('status', '')).upper() == 'FAIL':
            self.rows.append(validation_result_to_log_row(result))

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=['timestamp', 'symbol', 'zone_type', 'status', 'validation_score', 'fail_reasons'])

    def write_csv(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.to_frame().to_csv(target, index=False)
        return target

    def clear(self) -> None:
        self.rows.clear()


_DEFAULT_LOGGER = ZoneValidationLogger()


def clear_rejected_zone_log() -> None:
    _DEFAULT_LOGGER.clear()


def get_rejected_zone_log_frame() -> pd.DataFrame:
    return _DEFAULT_LOGGER.to_frame()


def write_rejected_zone_log(path: str | Path) -> Path:
    return _DEFAULT_LOGGER.write_csv(path)


def candles_to_dataframe(candles: Iterable[Any]) -> pd.DataFrame:
    rows = [{
        'timestamp': getattr(candle, 'timestamp', None),
        'open': getattr(candle, 'open', None),
        'high': getattr(candle, 'high', None),
        'low': getattr(candle, 'low', None),
        'close': getattr(candle, 'close', None),
        'volume': getattr(candle, 'volume', None),
    } for candle in candles]
    return pd.DataFrame(rows, columns=list(REQUIRED_COLUMNS))


def _validation_fail(zone_type: str, symbol: str, fail_reasons: list[str], explanation: str, *, timestamp: Any = None, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    result = {
        'zone_type': str(zone_type),
        'status': 'FAIL',
        'validation_score': 0.0,
        'fail_reasons': list(fail_reasons),
        'metrics': metrics or {},
        'explanation': explanation,
        'timestamp': '' if timestamp is None else str(timestamp),
        'symbol': str(symbol or ''),
    }
    _DEFAULT_LOGGER.log(result)
    return result


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float = 0.0, high: float = 10.0) -> float:
    return max(low, min(high, float(value)))


def _normalize_ohlcv_frame(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise TypeError('validate_zone expects a pandas DataFrame')
    if df.empty:
        raise ValueError('empty dataframe')
    frame = df.copy()
    frame.columns = [str(column).strip().lower() for column in frame.columns]
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f'missing required columns: {missing}')
    frame = frame.loc[:, list(REQUIRED_COLUMNS)]
    frame['timestamp'] = pd.to_datetime(frame['timestamp'], errors='coerce')
    if frame['timestamp'].isna().any():
        raise ValueError('timestamp contains unparseable values')
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
    if frame[list(REQUIRED_COLUMNS[1:])].isna().any().any():
        raise ValueError('numeric OHLCV columns contain invalid values')
    if (frame[['open', 'high', 'low', 'close']] <= 0).any().any() or (frame['volume'] < 0).any():
        raise ValueError('OHLCV data contains non-positive prices or negative volume')
    if not frame['timestamp'].is_monotonic_increasing:
        raise ValueError('timestamps must be sorted ascending')
    return frame.reset_index(drop=True)


def _true_range(frame: pd.DataFrame) -> pd.Series:
    prev_close = frame['close'].shift(1)
    parts = pd.concat([
        frame['high'] - frame['low'],
        (frame['high'] - prev_close).abs(),
        (frame['low'] - prev_close).abs(),
    ], axis=1)
    return parts.max(axis=1).fillna(frame['high'] - frame['low'])


def _atr_series(frame: pd.DataFrame, window: int) -> pd.Series:
    return _true_range(frame).rolling(max(int(window), 1), min_periods=1).mean().clip(lower=1e-6)


def _body_pct(frame: pd.DataFrame) -> pd.Series:
    price_range = (frame['high'] - frame['low']).clip(lower=1e-6)
    return (frame['close'] - frame['open']).abs() / price_range


def _wick_ratio(frame: pd.DataFrame) -> pd.Series:
    total_wick = (frame['high'] - frame[['open', 'close']].max(axis=1)) + (frame[['open', 'close']].min(axis=1) - frame['low'])
    body = (frame['close'] - frame['open']).abs().clip(lower=1e-6)
    return (total_wick / body).clip(lower=0.0)


def _range_overlap_ratio(left: pd.Series, right: pd.Series) -> float:
    overlap = max(0.0, min(float(left['high']), float(right['high'])) - max(float(left['low']), float(right['low'])))
    union = max(float(max(left['high'], right['high']) - min(left['low'], right['low'])), 1e-6)
    return overlap / union


def _session_tag(timestamp: pd.Timestamp) -> str:
    minute_value = timestamp.hour * 60 + timestamp.minute
    if 555 <= minute_value < 560:
        return 'OPENING'
    if 560 <= minute_value < 575:
        return 'OPENING_BUFFER'
    if 575 <= minute_value <= 645:
        return 'MORNING'
    if 646 <= minute_value <= 825:
        return 'MIDDAY'
    if 826 <= minute_value <= 885:
        return 'AFTERNOON'
    return 'OFF_HOURS'


def _slice(frame: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    if end < start:
        return frame.iloc[0:0].copy()
    return frame.iloc[max(start, 0): min(end + 1, len(frame))].copy()

def _infer_base_end_index(frame: pd.DataFrame, created_idx: int, entry_idx: int, zone_type: str, zone_low: float, zone_high: float, config: ZoneValidationConfig) -> int:
    max_idx = min(entry_idx - 1, created_idx + config.max_base_candles - 1)
    edge = zone_high if zone_type == 'demand' else zone_low
    for idx in range(created_idx, max_idx + 1):
        close = float(frame.iloc[idx]['close'])
        if zone_type == 'demand' and close > edge:
            return idx
        if zone_type == 'supply' and close < edge:
            return idx
    return max_idx


def _departure_slice(frame: pd.DataFrame, base_end_idx: int, entry_idx: int, config: ZoneValidationConfig) -> pd.DataFrame:
    return _slice(frame, base_end_idx + 1, min(entry_idx, base_end_idx + config.departure_window))


def score_move_away(frame: pd.DataFrame, *, zone_type: str, zone_low: float, zone_high: float, created_idx: int, entry_idx: int, atr_series: pd.Series, config: ZoneValidationConfig) -> tuple[float, dict[str, float]]:
    base_end_idx = _infer_base_end_index(frame, created_idx, entry_idx, zone_type, zone_low, zone_high, config)
    departure = _departure_slice(frame, base_end_idx, entry_idx, config)
    if departure.empty:
        return 0.0, {'move_away_atr': 0.0, 'impulsive_candles': 0, 'avg_body_pct': 0.0, 'large_candle_pct': 0.0, 'move_away_speed': 999.0, 'base_end_idx': base_end_idx, 'departure_start_idx': base_end_idx + 1, 'departure_range': 0.0}
    atr_ref = max(_safe_float(atr_series.iloc[min(entry_idx, len(atr_series) - 1)], 1e-6), 1e-6)
    body_pct = _body_pct(departure)
    candle_range = departure['high'] - departure['low']
    move_distance = float(departure['high'].max()) - float(zone_high) if zone_type == 'demand' else float(zone_low) - float(departure['low'].min())
    move_away_atr = max(move_distance, 0.0) / atr_ref
    impulsive_mask = (body_pct >= config.impulsive_body_pct) & ((candle_range / atr_ref) >= config.impulsive_range_atr)
    impulsive_candles = int(impulsive_mask.sum())
    avg_body_pct = float(body_pct.mean()) if not body_pct.empty else 0.0
    large_candle_pct = float((body_pct >= config.large_candle_body_pct).mean()) if len(body_pct) else 0.0
    max_idx = int(departure['high'].idxmax() if zone_type == 'demand' else departure['low'].idxmin())
    move_away_speed = max(1, max_idx - base_end_idx)
    score = 0.0
    score += _clamp((move_away_atr / max(config.min_move_away_atr, 1e-6)) * 4.0)
    score += _clamp((impulsive_candles / max(config.min_impulsive_candles, 1)) * 2.5)
    score += _clamp((avg_body_pct / max(config.min_avg_body_pct, 1e-6)) * 1.5)
    score += _clamp((large_candle_pct / 0.50) * 1.0)
    score += _clamp((3.0 / float(move_away_speed)) * 1.0)
    return _clamp(score), {
        'move_away_atr': round(move_away_atr, 4),
        'impulsive_candles': impulsive_candles,
        'avg_body_pct': round(avg_body_pct, 4),
        'large_candle_pct': round(large_candle_pct, 4),
        'move_away_speed': float(move_away_speed),
        'base_end_idx': base_end_idx,
        'departure_start_idx': base_end_idx + 1,
        'departure_range': round(max(move_distance, 0.0), 4),
    }


def detect_bos(frame: pd.DataFrame, *, zone_type: str, created_idx: int, entry_idx: int, swing_window: int = 2) -> bool:
    prior = frame.iloc[max(created_idx - max(int(swing_window), 1) * 3, 0): created_idx + 1]
    after = frame.iloc[created_idx + 1: entry_idx + 1]
    if prior.empty or after.empty:
        return False
    return float(after['high'].max()) > float(prior['high'].max()) if zone_type == 'demand' else float(after['low'].min()) < float(prior['low'].min())


def score_freshness(frame: pd.DataFrame, *, zone_low: float, zone_high: float, created_idx: int, entry_idx: int, config: ZoneValidationConfig) -> tuple[float, dict[str, float]]:
    touch_count = 0
    for idx in range(created_idx + 1, entry_idx):
        candle = frame.iloc[idx]
        if float(candle['low']) <= zone_high and float(candle['high']) >= zone_low:
            touch_count += 1
    candles_since_created = max(entry_idx - created_idx, 0)
    score = 10.0 if touch_count == 0 else 7.0 if touch_count == 1 else 3.0 if touch_count == 2 else 0.0
    if candles_since_created > config.max_retest_delay:
        score = max(0.0, score - 2.0)
    return score, {'touch_count': touch_count, 'candles_since_zone_created': candles_since_created}


def _engulfing_confirmation(frame: pd.DataFrame, entry_idx: int, zone_type: str) -> bool:
    if entry_idx <= 0:
        return False
    current = frame.iloc[entry_idx]
    previous = frame.iloc[entry_idx - 1]
    if zone_type == 'demand':
        return float(current['close']) > float(current['open']) and float(current['close']) >= float(previous['high'])
    return float(current['close']) < float(current['open']) and float(current['close']) <= float(previous['low'])


def score_rejection(frame: pd.DataFrame, *, zone_type: str, zone_low: float, zone_high: float, touch_idx: int, entry_idx: int) -> tuple[float, dict[str, float | bool]]:
    candle = frame.iloc[entry_idx]
    price_range = max(float(candle['high']) - float(candle['low']), 1e-6)
    body = abs(float(candle['close']) - float(candle['open']))
    upper_wick = float(candle['high']) - max(float(candle['open']), float(candle['close']))
    lower_wick = min(float(candle['open']), float(candle['close'])) - float(candle['low'])
    wick_body_ratio = (lower_wick / max(body, 1e-6)) if zone_type == 'demand' else (upper_wick / max(body, 1e-6))
    body_strength = body / price_range
    edge = zone_high if zone_type == 'demand' else zone_low
    close_distance = (float(candle['close']) - edge) / max(abs(zone_high - zone_low), 1e-6) if zone_type == 'demand' else (edge - float(candle['close'])) / max(abs(zone_high - zone_low), 1e-6)
    engulfing = _engulfing_confirmation(frame, entry_idx, zone_type)
    strong = wick_body_ratio >= 1.2 and body_strength >= 0.35 and close_distance >= 0.20
    score = _clamp((wick_body_ratio / 2.0) * 4.0 + (body_strength / 0.60) * 2.0 + (2.0 if engulfing else 0.0) + (close_distance / 0.60) * 2.0)
    return score, {
        'rejection_wick_body_ratio': round(wick_body_ratio, 4),
        'rejection_body_strength': round(body_strength, 4),
        'engulf_confirmed': engulfing,
        'close_away_pct': round(close_distance, 4),
        'strong_rejection_candle': strong,
        'touch_idx': int(touch_idx),
    }


def score_structure(frame: pd.DataFrame, *, zone_type: str, created_idx: int, entry_idx: int, config: ZoneValidationConfig) -> tuple[float, dict[str, float]]:
    lookback = _slice(frame, max(0, created_idx - config.chop_lookback), min(len(frame) - 1, entry_idx + config.swing_window))
    if lookback.empty:
        return 0.0, {'structure_score': 0.0, 'swing_clarity_score': 0.0, 'overlap_ratio': 1.0, 'chop_score': 1.0}
    closes = lookback['close']
    directional = closes.diff().dropna()
    total_path = directional.abs().sum()
    net_change = abs(float(closes.iloc[-1] - closes.iloc[0]))
    chop_score = 1.0 if total_path <= 1e-6 else 1.0 - min(net_change / total_path, 1.0)
    overlap_values = [_range_overlap_ratio(lookback.iloc[idx - 1], lookback.iloc[idx]) for idx in range(1, len(lookback))]
    overlap_ratio = float(sum(overlap_values) / len(overlap_values)) if overlap_values else 0.0
    highs = lookback['high'].rolling(3, min_periods=3).max().dropna()
    lows = lookback['low'].rolling(3, min_periods=3).min().dropna()
    swing_clarity = 0.0
    if len(highs) >= 2 and len(lows) >= 2:
        if zone_type == 'demand':
            swing_clarity = 1.0 if float(highs.iloc[-1]) >= float(highs.iloc[-2]) and float(lows.iloc[-1]) >= float(lows.iloc[-2]) else 0.35
        else:
            swing_clarity = 1.0 if float(highs.iloc[-1]) <= float(highs.iloc[-2]) and float(lows.iloc[-1]) <= float(lows.iloc[-2]) else 0.35
    score = _clamp(swing_clarity * 4.0 + (1.0 - overlap_ratio) * 3.0 + (1.0 - chop_score) * 3.0)
    return score, {'structure_score': round(score, 4), 'swing_clarity_score': round(swing_clarity * 10.0, 4), 'overlap_ratio': round(overlap_ratio, 4), 'chop_score': round(chop_score, 4)}

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=max(int(span), 1), adjust=False).mean()


def _infer_htf_trend(frame: pd.DataFrame, entry_idx: int, config: ZoneValidationConfig) -> str:
    sample = frame.iloc[: entry_idx + 1]
    fast = _ema(sample['close'], config.ema_fast)
    slow = _ema(sample['close'], config.ema_slow)
    if len(fast) < 2 or len(slow) < 2:
        return 'NEUTRAL'
    fast_slope = float(fast.iloc[-1] - fast.iloc[-2])
    if float(fast.iloc[-1]) >= float(slow.iloc[-1]) and fast_slope >= 0:
        return 'UP'
    if float(fast.iloc[-1]) <= float(slow.iloc[-1]) and fast_slope <= 0:
        return 'DOWN'
    return 'NEUTRAL'


def _score_trend(frame: pd.DataFrame, *, zone_type: str, entry_idx: int, move_away_score: float, rejection_score: float, config: ZoneValidationConfig, htf_trend: str | None = None) -> tuple[float, dict[str, float | str]]:
    trend = str(htf_trend or _infer_htf_trend(frame, entry_idx, config)).upper()
    trade_direction = 'UP' if zone_type == 'demand' else 'DOWN'
    ema_fast = _ema(frame['close'].iloc[: entry_idx + 1], config.ema_fast)
    ema_slope = float(ema_fast.iloc[-1] - ema_fast.iloc[-2]) if len(ema_fast) >= 2 else 0.0
    swing_alignment = 1.0 if (trade_direction == 'UP' and ema_slope >= 0) or (trade_direction == 'DOWN' and ema_slope <= 0) else 0.0
    reversal_score = round((move_away_score + rejection_score) / 2.0, 4)
    aligned = trend in {trade_direction, 'NEUTRAL'} and swing_alignment > 0
    score = 8.5 if aligned else 3.0 if reversal_score >= config.reversal_score_threshold else 0.0
    return score, {
        'htf_trend': trend,
        'ema_slope': round(ema_slope, 6),
        'swing_alignment': round(swing_alignment, 4),
        'reversal_score': reversal_score,
        'trend_aligned': aligned,
    }


def detect_liquidity_sweep(frame: pd.DataFrame, *, zone_type: str, touch_idx: int, lookback: int = 4) -> bool:
    prior = frame.iloc[max(0, touch_idx - max(int(lookback), 1)):touch_idx]
    if prior.empty:
        return False
    candle = frame.iloc[touch_idx]
    return float(candle['low']) < float(prior['low'].min()) if zone_type == 'demand' else float(candle['high']) > float(prior['high'].max())


def _score_imbalance(frame: pd.DataFrame, *, zone_type: str, departure_start_idx: int, entry_idx: int, atr_series: pd.Series) -> tuple[float, dict[str, float | bool]]:
    departure = _slice(frame, departure_start_idx, entry_idx)
    if len(departure) < 2:
        return 0.0, {'imbalance_score': 0.0, 'fvg_present': False, 'imbalance_size_atr': 0.0, 'displacement_score': 0.0}
    atr_ref = max(_safe_float(atr_series.iloc[min(entry_idx, len(atr_series) - 1)], 1e-6), 1e-6)
    fvg_present = False
    max_gap = 0.0
    for left_idx in range(departure.index.min(), departure.index.max()):
        left = frame.iloc[left_idx]
        right = frame.iloc[left_idx + 1]
        gap = float(right['low']) - float(left['high']) if zone_type == 'demand' else float(left['low']) - float(right['high'])
        max_gap = max(max_gap, gap)
        if gap > 0:
            fvg_present = True
    displacement_score = float((((departure['high'] - departure['low']) / atr_ref).mean() * 2.5) + (_body_pct(departure).mean() * 3.0))
    imbalance_size_atr = max(max_gap, 0.0) / atr_ref
    score = _clamp((4.0 if fvg_present else 0.0) + (imbalance_size_atr / 0.5 * 3.0) + displacement_score)
    return score, {'imbalance_score': round(score, 4), 'fvg_present': fvg_present, 'imbalance_size_atr': round(imbalance_size_atr, 4), 'displacement_score': round(displacement_score, 4)}


def calculate_rr(entry_price: float, stop_loss: float, target_price: float) -> float:
    risk = abs(float(entry_price) - float(stop_loss))
    reward = abs(float(target_price) - float(entry_price))
    return 0.0 if risk <= 1e-6 else reward / risk


def _rr_score(rr_ratio: float, minimum_rr: float) -> float:
    return 0.0 if rr_ratio <= 0 else _clamp((rr_ratio / max(minimum_rr, 1e-6)) * 7.0)


def build_fail_reasons(metrics: dict[str, Any], scores: dict[str, float], config: ZoneValidationConfig) -> list[str]:
    reasons: list[str] = []
    if metrics['move_away_atr'] < config.min_move_away_atr or metrics['impulsive_candles'] < config.min_impulsive_candles or metrics['avg_body_pct'] < config.min_avg_body_pct:
        reasons.append('weak_move_away')
    if not bool(metrics['bos_confirmed']):
        reasons.append('no_break_of_structure')
    if metrics['base_candles'] > config.max_base_candles or metrics['base_departure_ratio'] > config.max_base_departure_ratio or metrics['base_wick_ratio'] > config.max_base_wick_ratio or metrics['base_overlap_ratio'] > config.max_base_overlap_ratio:
        reasons.append('dirty_base')
    if metrics['touch_count'] >= max(config.max_touch_count + 1, 2):
        reasons.append('not_fresh')
    if scores['rejection_score'] < config.min_rejection_score or ((not bool(metrics['strong_rejection_candle'])) and (not bool(metrics['engulf_confirmed']))) or metrics['close_away_pct'] < 0.20:
        reasons.append('weak_rejection')
    if scores['structure_score'] < config.min_structure_score or metrics['chop_score'] > config.max_chop_score:
        reasons.append('bad_structure')
    if not bool(metrics['trend_aligned']) and metrics['reversal_score'] < config.reversal_score_threshold:
        reasons.append('trend_misaligned')
    if config.require_sweep and not bool(metrics['sweep_confirmed']):
        reasons.append('no_liquidity_sweep')
    if scores['imbalance_score'] < config.min_imbalance_score:
        reasons.append('no_imbalance')
    if metrics['atr_pct'] < config.min_atr_pct or metrics['volatility_percentile'] < config.min_volatility_percentile:
        reasons.append('low_volatility')
    if metrics['rr_ratio'] < config.min_rr_ratio:
        reasons.append('bad_rr')
    if metrics['candles_since_zone_created'] > config.max_retest_delay:
        reasons.append('late_retest')
    if metrics['penetration_pct'] > config.max_penetration_pct:
        reasons.append('deep_penetration')
    if metrics['zone_width_atr'] > config.max_zone_width_atr:
        reasons.append('oversized_zone')
    if not bool(metrics['session_allowed']):
        reasons.append('session_fail')
    if scores.get('validation_score', 0.0) < config.soft_score_threshold:
        reasons.append('low_validation_score')
    return reasons


def _build_explanation(fail_reasons: list[str]) -> str:
    if not fail_reasons:
        return 'Zone passed strict demand/supply validation with acceptable structure, momentum, freshness, and reward relative to risk.'
    phrases = [FAIL_REASON_MESSAGES.get(reason, reason.replace('_', ' ')) for reason in fail_reasons]
    if len(phrases) == 1:
        return f'Zone failed because {phrases[0]}.'
    if len(phrases) == 2:
        return f'Zone failed because {phrases[0]} and {phrases[1]}.'
    return f"Zone failed because {', '.join(phrases[:-1])}, and {phrases[-1]}."


def validation_result_to_log_row(result: dict[str, Any]) -> dict[str, object]:
    return {
        'timestamp': str(result.get('timestamp', '')),
        'symbol': str(result.get('symbol', '')),
        'zone_type': str(result.get('zone_type', '')),
        'status': str(result.get('status', '')),
        'validation_score': round(_safe_float(result.get('validation_score', 0.0)), 2),
        'fail_reasons': '|'.join(str(reason) for reason in result.get('fail_reasons', []) or []),
    }

def validate_zone(
    df: pd.DataFrame,
    *,
    zone_type: str,
    zone_low: float,
    zone_high: float,
    created_idx: int,
    touch_idx: int,
    entry_idx: int,
    entry_price: float,
    stop_loss: float,
    target_price: float,
    symbol: str = '',
    config: ZoneValidationConfig | None = None,
    htf_trend: str | None = None,
) -> dict[str, Any]:
    """Validate a demand/supply candidate and return a transparent pass/fail contract."""
    cfg = config or ZoneValidationConfig()
    try:
        frame = _normalize_ohlcv_frame(df)
    except (TypeError, ValueError) as exc:
        return _validation_fail(str(zone_type), symbol, ['invalid_input'], f'Zone validation failed because {exc}.')
    if created_idx < 0 or touch_idx < 0 or entry_idx < 0 or created_idx >= len(frame) or touch_idx >= len(frame) or entry_idx >= len(frame):
        return _validation_fail(str(zone_type), symbol, ['invalid_input'], 'Zone validation failed because candidate indices were out of bounds.')
    if not (created_idx <= touch_idx <= entry_idx):
        return _validation_fail(str(zone_type), symbol, ['invalid_input'], 'Zone validation failed because zone lifecycle indices were not ordered correctly.')
    if float(zone_high) <= float(zone_low):
        return _validation_fail(str(zone_type), symbol, ['invalid_input'], 'Zone validation failed because zone_high must be above zone_low.')
    zone_key = str(zone_type).strip().lower()
    if zone_key not in {'demand', 'supply'}:
        return _validation_fail(str(zone_type), symbol, ['invalid_input'], 'Zone validation failed because zone_type must be demand or supply.')

    atr_series = _atr_series(frame, cfg.atr_window)
    atr_ref = max(_safe_float(atr_series.iloc[entry_idx], 1e-6), 1e-6)
    move_away_score, move_metrics = score_move_away(frame, zone_type=zone_key, zone_low=float(zone_low), zone_high=float(zone_high), created_idx=int(created_idx), entry_idx=int(entry_idx), atr_series=atr_series, config=cfg)
    base_slice = _slice(frame, created_idx, int(move_metrics['base_end_idx']))
    base_range = float(base_slice['high'].max() - base_slice['low'].min()) if not base_slice.empty else 0.0
    base_wick_ratio = float(_wick_ratio(base_slice).mean()) if not base_slice.empty else 0.0
    overlap_values = [_range_overlap_ratio(base_slice.iloc[idx - 1], base_slice.iloc[idx]) for idx in range(1, len(base_slice))] if len(base_slice) > 1 else []
    base_overlap_ratio = float(sum(overlap_values) / len(overlap_values)) if overlap_values else 0.0
    departure_range = max(_safe_float(move_metrics['departure_range'], 0.0), 1e-6)
    bos_confirmed = detect_bos(frame, zone_type=zone_key, created_idx=created_idx, entry_idx=entry_idx, swing_window=cfg.swing_window)
    freshness_score, freshness_metrics = score_freshness(frame, zone_low=float(zone_low), zone_high=float(zone_high), created_idx=created_idx, entry_idx=entry_idx, config=cfg)
    rejection_score, rejection_metrics = score_rejection(frame, zone_type=zone_key, zone_low=float(zone_low), zone_high=float(zone_high), touch_idx=touch_idx, entry_idx=entry_idx)
    structure_score, structure_metrics = score_structure(frame, zone_type=zone_key, created_idx=created_idx, entry_idx=entry_idx, config=cfg)
    trend_score, trend_metrics = _score_trend(frame, zone_type=zone_key, entry_idx=entry_idx, move_away_score=move_away_score, rejection_score=rejection_score, config=cfg, htf_trend=htf_trend)
    sweep_confirmed = detect_liquidity_sweep(frame, zone_type=zone_key, touch_idx=touch_idx)
    imbalance_score, imbalance_metrics = _score_imbalance(frame, zone_type=zone_key, departure_start_idx=int(move_metrics['departure_start_idx']), entry_idx=entry_idx, atr_series=atr_series)
    rr_ratio = calculate_rr(entry_price, stop_loss, target_price)
    rr_score = _rr_score(rr_ratio, cfg.min_rr_ratio)
    entry_ts = pd.Timestamp(frame.iloc[entry_idx]['timestamp'])
    session_tag = _session_tag(entry_ts)
    session_allowed = session_tag in set(cfg.allowed_sessions)
    zone_width_atr = (float(zone_high) - float(zone_low)) / atr_ref
    touch_candle = frame.iloc[touch_idx]
    if zone_key == 'demand':
        penetration_pct = max(0.0, (float(zone_high) - float(touch_candle['low'])) / max(float(zone_high - zone_low), 1e-6))
    else:
        penetration_pct = max(0.0, (float(touch_candle['high']) - float(zone_low)) / max(float(zone_high - zone_low), 1e-6))
    atr_pct = atr_ref / max(float(frame.iloc[entry_idx]['close']), 1e-6) * 100.0
    atr_window = atr_series.iloc[max(0, entry_idx - cfg.volatility_lookback + 1): entry_idx + 1]
    volatility_percentile = float((atr_window <= atr_ref).mean()) if not atr_window.empty else 0.0

    scores = {
        'move_away_score': round(move_away_score, 4),
        'freshness_score': round(freshness_score, 4),
        'rejection_score': round(rejection_score, 4),
        'structure_score': round(structure_score, 4),
        'trend_score': round(trend_score, 4),
        'imbalance_score': round(imbalance_score, 4),
        'rr_score': round(rr_score, 4),
    }
    validation_score = sum(scores[name] * float(weight) for name, weight in cfg.weights.items())
    scores['validation_score'] = round(validation_score, 4)
    metrics: dict[str, Any] = {
        'move_away_atr': move_metrics['move_away_atr'],
        'impulsive_candles': move_metrics['impulsive_candles'],
        'avg_body_pct': move_metrics['avg_body_pct'],
        'large_candle_pct': move_metrics['large_candle_pct'],
        'move_away_speed': move_metrics['move_away_speed'],
        'bos_confirmed': bool(bos_confirmed),
        'base_candles': int(len(base_slice)),
        'base_departure_ratio': round(base_range / departure_range, 4),
        'base_wick_ratio': round(base_wick_ratio, 4),
        'base_overlap_ratio': round(base_overlap_ratio, 4),
        'touch_count': freshness_metrics['touch_count'],
        'rejection_score': round(rejection_score, 4),
        'rejection_wick_body_ratio': rejection_metrics['rejection_wick_body_ratio'],
        'rejection_body_strength': rejection_metrics['rejection_body_strength'],
        'engulf_confirmed': bool(rejection_metrics['engulf_confirmed']),
        'strong_rejection_candle': bool(rejection_metrics['strong_rejection_candle']),
        'close_away_pct': rejection_metrics['close_away_pct'],
        'structure_score': structure_metrics['structure_score'],
        'swing_clarity_score': structure_metrics['swing_clarity_score'],
        'overlap_ratio': structure_metrics['overlap_ratio'],
        'chop_score': structure_metrics['chop_score'],
        'htf_trend': trend_metrics['htf_trend'],
        'ema_slope': trend_metrics['ema_slope'],
        'swing_alignment': trend_metrics['swing_alignment'],
        'trend_aligned': bool(trend_metrics['trend_aligned']),
        'reversal_score': trend_metrics['reversal_score'],
        'sweep_confirmed': bool(sweep_confirmed),
        'imbalance_score': imbalance_metrics['imbalance_score'],
        'fvg_present': bool(imbalance_metrics['fvg_present']),
        'imbalance_size_atr': imbalance_metrics['imbalance_size_atr'],
        'displacement_score': imbalance_metrics['displacement_score'],
        'atr_pct': round(atr_pct, 4),
        'volatility_percentile': round(volatility_percentile, 4),
        'rr_ratio': round(rr_ratio, 4),
        'candles_since_zone_created': freshness_metrics['candles_since_zone_created'],
        'penetration_pct': round(penetration_pct, 4),
        'zone_width_atr': round(zone_width_atr, 4),
        'session_tag': session_tag,
        'session_allowed': bool(session_allowed),
        'entry_idx': int(entry_idx),
        'touch_idx': int(touch_idx),
        'created_idx': int(created_idx),
        **scores,
    }
    fail_reasons = build_fail_reasons(metrics, scores, cfg)
    result = {
        'zone_type': zone_key,
        'status': 'FAIL' if fail_reasons else 'PASS',
        'validation_score': round(validation_score, 2),
        'fail_reasons': fail_reasons,
        'metrics': metrics,
        'explanation': _build_explanation(fail_reasons),
        'timestamp': entry_ts.strftime('%Y-%m-%d %H:%M:%S'),
        'symbol': str(symbol or ''),
    }
    if result['status'] == 'FAIL':
        _DEFAULT_LOGGER.log(result)
        if cfg.logger_csv_path:
            write_rejected_zone_log(cfg.logger_csv_path)
    return result

def sample_usage() -> dict[str, Any]:
    frame = pd.DataFrame([
        {'timestamp': '2026-03-05 09:15:00', 'open': 100.8, 'high': 101.0, 'low': 100.4, 'close': 100.6, 'volume': 1000},
        {'timestamp': '2026-03-05 09:20:00', 'open': 100.6, 'high': 100.8, 'low': 98.9, 'close': 99.3, 'volume': 1500},
        {'timestamp': '2026-03-05 09:25:00', 'open': 99.3, 'high': 100.4, 'low': 99.1, 'close': 100.2, 'volume': 1400},
        {'timestamp': '2026-03-05 09:30:00', 'open': 100.2, 'high': 101.2, 'low': 100.0, 'close': 101.0, 'volume': 1400},
        {'timestamp': '2026-03-05 09:35:00', 'open': 101.0, 'high': 101.8, 'low': 100.8, 'close': 101.5, 'volume': 1450},
        {'timestamp': '2026-03-05 09:40:00', 'open': 101.5, 'high': 101.6, 'low': 99.2, 'close': 100.7, 'volume': 1700},
        {'timestamp': '2026-03-05 09:45:00', 'open': 100.7, 'high': 102.3, 'low': 100.5, 'close': 102.2, 'volume': 1800},
    ])
    return validate_zone(frame, zone_type='demand', zone_low=98.9, zone_high=100.6, created_idx=1, touch_idx=5, entry_idx=6, entry_price=102.2, stop_loss=98.7, target_price=109.1, symbol='NIFTY', config=ZoneValidationConfig(require_sweep=False))


__all__ = [
    'ZoneValidationConfig',
    'ZoneValidationLogger',
    'build_fail_reasons',
    'calculate_rr',
    'candles_to_dataframe',
    'clear_rejected_zone_log',
    'detect_bos',
    'detect_liquidity_sweep',
    'get_rejected_zone_log_frame',
    'sample_usage',
    'score_freshness',
    'score_move_away',
    'score_rejection',
    'score_structure',
    'validate_zone',
    'validation_result_to_log_row',
    'write_rejected_zone_log',
]
