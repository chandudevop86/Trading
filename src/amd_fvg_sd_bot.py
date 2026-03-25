from __future__ import annotations

from dataclasses import dataclass, replace
from math import floor
from datetime import time
from typing import Any

import pandas as pd

from src.trading_core import ScoringConfig, weighted_score

REQUIRED_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
STRATEGY_NAME = 'AMD_FVG_SUPPLY_DEMAND'


@dataclass(slots=True)
class ConfluenceConfig:
    mode: str = 'Balanced'
    swing_window: int = 3
    accumulation_lookback: int = 10
    manipulation_lookback: int = 6
    distribution_lookback: int = 8
    min_fvg_size: float = 0.35
    min_bvg_size: float = 0.25
    zone_merge_tolerance: float = 0.0015
    zone_fresh_bars: int = 24
    min_zone_reaction: float = 0.55
    min_zone_strength_score: float = 4.0
    max_nearby_zones: int = 2
    retest_tolerance_pct: float = 0.0015
    max_retest_bars: int = 6
    rr_ratio: float = 2.0
    trailing_sl_pct: float = 0.0
    duplicate_signal_cooldown_bars: int = 12
    min_score_conservative: float = 7.4
    min_score_balanced: float = 6.2
    min_score_aggressive: float = 4.8
    require_vwap_alignment: bool = True
    require_trend_alignment: bool = True
    require_retest_confirmation: bool = True
    require_liquidity_sweep: bool = True
    require_fvg_confirmation: bool = True
    allow_bvg_entries: bool = False
    require_distribution_phase: bool = True
    minimum_amd_confidence: float = 1.2
    morning_session_start: str = '09:20'
    morning_session_end: str = '11:30'
    midday_start: str = '12:00'
    midday_end: str = '13:30'
    allow_afternoon_session: bool = False
    afternoon_session_start: str = '13:45'
    afternoon_session_end: str = '15:00'
    allow_secondary_entries: bool = False
    max_trades_per_day: int = 1

    @classmethod
    def for_mode(cls, mode: str) -> 'ConfluenceConfig':
        normalized = _normalize_mode(mode)
        base = cls(mode=normalized)
        if normalized == 'Conservative':
            return replace(
                base,
                swing_window=4,
                accumulation_lookback=12,
                manipulation_lookback=7,
                distribution_lookback=10,
                min_fvg_size=0.45,
                min_bvg_size=0.35,
                zone_fresh_bars=18,
                min_zone_reaction=0.65,
                min_zone_strength_score=4.8,
                retest_tolerance_pct=0.0012,
                max_retest_bars=4,
                require_distribution_phase=True,
                minimum_amd_confidence=1.35,
                duplicate_signal_cooldown_bars=14,
                allow_secondary_entries=False,
                max_trades_per_day=1,
            )
        if normalized == 'Aggressive':
            return replace(
                base,
                swing_window=2,
                accumulation_lookback=8,
                manipulation_lookback=4,
                distribution_lookback=6,
                min_fvg_size=0.25,
                min_bvg_size=0.2,
                zone_fresh_bars=30,
                min_zone_reaction=0.5,
                min_zone_strength_score=3.8,
                retest_tolerance_pct=0.002,
                max_retest_bars=8,
                require_distribution_phase=False,
                minimum_amd_confidence=0.9,
                duplicate_signal_cooldown_bars=8,
                allow_secondary_entries=False,
                max_trades_per_day=1,
            )
        return base


def _normalize_mode(mode: str) -> str:
    raw = str(mode or '').strip().lower()
    if raw == 'conservative':
        return 'Conservative'
    if raw == 'aggressive':
        return 'Aggressive'
    return 'Balanced'


def _score_threshold(config: ConfluenceConfig, mode: str) -> float:
    normalized = _normalize_mode(mode)
    if normalized == 'Conservative':
        return float(config.min_score_conservative)
    if normalized == 'Aggressive':
        return float(config.min_score_aggressive)
    return float(config.min_score_balanced)


def _mode_max_trades(config: ConfluenceConfig, mode: str) -> int:
    return max(1, int(config.max_trades_per_day))


def _prepare_df(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        df = pd.DataFrame(data)

    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    df.columns = [str(column).strip().lower() for column in df.columns]
    rename_map = {}
    if 'datetime' in df.columns:
        rename_map['datetime'] = 'timestamp'
    if 'date' in df.columns and 'timestamp' not in df.columns:
        rename_map['date'] = 'timestamp'
    if rename_map:
        df = df.rename(columns=rename_map)

    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            df[column] = None

    df = df.loc[:, REQUIRED_COLUMNS].copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    for column in ['open', 'high', 'low', 'close', 'volume']:
        df[column] = pd.to_numeric(df[column], errors='coerce')

    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close']).drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    df['bar_range'] = (df['high'] - df['low']).clip(lower=0.0)
    df['body_size'] = (df['close'] - df['open']).abs()
    df['direction'] = df['close'] - df['open']
    df['ema_fast'] = df['close'].ewm(span=8, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=21, adjust=False).mean()
    df['session_day'] = df['timestamp'].dt.strftime('%Y-%m-%d')
    typical_price = (df['high'] + df['low'] + df['close']) / 3.0
    session_value = (typical_price * df['volume'].fillna(0.0)).groupby(df['session_day']).cumsum()
    session_volume = df['volume'].fillna(0.0).groupby(df['session_day']).cumsum().replace(0.0, pd.NA)
    df['vwap'] = (session_value / session_volume).fillna(df['close'])
    df['avg_range_5'] = df['bar_range'].rolling(5, min_periods=1).mean()
    df['avg_body_5'] = df['body_size'].rolling(5, min_periods=1).mean()
    return df


def _safe_price_gap(reference: float, config_value: float) -> float:
    return max(float(config_value), max(abs(float(reference)), 1.0) * 0.0002)


def _window_slice(df: pd.DataFrame, start: int, end: int) -> pd.DataFrame:
    left = max(0, int(start))
    right = min(len(df), int(end))
    if left >= right:
        return df.iloc[0:0]
    return df.iloc[left:right]

def _parse_hhmm(value: str, fallback: str) -> time:
    raw = str(value or fallback).strip() or fallback
    hour_text, minute_text = raw.split(':', 1)
    return time(hour=int(hour_text), minute=int(minute_text))


def _session_window(row: pd.Series, config: ConfluenceConfig) -> str:
    current_time = pd.Timestamp(row['timestamp']).time()
    morning_start = _parse_hhmm(config.morning_session_start, '09:20')
    morning_end = _parse_hhmm(config.morning_session_end, '11:30')
    midday_start = _parse_hhmm(config.midday_start, '12:00')
    midday_end = _parse_hhmm(config.midday_end, '13:30')
    afternoon_start = _parse_hhmm(config.afternoon_session_start, '13:45')
    afternoon_end = _parse_hhmm(config.afternoon_session_end, '15:00')
    if morning_start <= current_time <= morning_end:
        return 'MORNING'
    if midday_start <= current_time <= midday_end:
        return 'MIDDAY_BLOCKED'
    if config.allow_afternoon_session and afternoon_start <= current_time <= afternoon_end:
        return 'AFTERNOON'
    return ''


def _vwap_alignment(row: pd.Series, side: str) -> bool:
    vwap = float(row.get('vwap', 0.0) or 0.0)
    if vwap <= 0:
        return False
    if side == 'BUY':
        return float(row['close']) >= vwap
    return float(row['close']) <= vwap


def _zone_strength_score(zone: dict[str, Any], row: pd.Series, side: str, nearby_zones: int) -> float:
    reaction_strength = float(zone.get('reaction_strength', 0.0) or 0.0)
    score = 0.0
    if reaction_strength >= 0.55:
        score += 2.0
    elif reaction_strength >= 0.35:
        score += 1.0
    age = max(0, int(row.name) - int(zone.get('created_index', row.name)))
    if age <= 8:
        score += 1.0
    elif age <= 16:
        score += 0.5
    if side == 'BUY' and float(row['close']) >= float(row['ema_fast']):
        score += 1.0
    if side == 'SELL' and float(row['close']) <= float(row['ema_fast']):
        score += 1.0
    if nearby_zones <= 1:
        score += 1.0
    elif nearby_zones >= 3:
        score -= 1.0
    return round(score, 2)


def _nearby_zone_count(zones: list[dict[str, Any]], zone: dict[str, Any], row: pd.Series, side: str, config: ConfluenceConfig) -> int:
    tolerance_abs = max(float(row['close']) * float(config.zone_merge_tolerance), float(row['avg_range_5']) * 0.35, 0.05)
    zone_mid = (float(zone['zone_low']) + float(zone['zone_high'])) / 2.0
    zone_type = 'demand' if side == 'BUY' else 'supply'
    count = 0
    for item in zones:
        if str(item.get('type', '')) != zone_type:
            continue
        item_mid = (float(item['zone_low']) + float(item['zone_high'])) / 2.0
        if abs(item_mid - zone_mid) <= tolerance_abs:
            count += 1
    return count

def detect_amd_phase(df: pd.DataFrame, config: ConfluenceConfig) -> pd.DataFrame:
    candles = _prepare_df(df)
    if candles.empty:
        return candles

    candles['recent_high'] = candles['high'].shift(1).rolling(config.manipulation_lookback, min_periods=2).max()
    candles['recent_low'] = candles['low'].shift(1).rolling(config.manipulation_lookback, min_periods=2).min()
    candles['rolling_range'] = candles['high'].rolling(config.accumulation_lookback, min_periods=3).max() - candles['low'].rolling(config.accumulation_lookback, min_periods=3).min()
    candles['rolling_std'] = candles['close'].rolling(config.accumulation_lookback, min_periods=3).std().fillna(0.0)
    candles['rolling_mid'] = candles['close'].rolling(config.accumulation_lookback, min_periods=3).mean()
    candles['distribution_body_avg'] = candles['body_size'].rolling(config.distribution_lookback, min_periods=2).mean().fillna(candles['body_size'])

    accumulation_limit = candles['close'].abs().clip(lower=1.0) * 0.018
    accumulation = (candles['rolling_range'] <= accumulation_limit) & (candles['rolling_std'] <= candles['avg_range_5'] * 0.9)

    bullish_manipulation = (candles['low'] < candles['recent_low']) & (candles['close'] > candles['recent_low'])
    bearish_manipulation = (candles['high'] > candles['recent_high']) & (candles['close'] < candles['recent_high'])

    bullish_distribution = (
        (candles['close'] > candles['recent_high'])
        & (candles['body_size'] >= candles['distribution_body_avg'] * 1.1)
        & (candles['close'] > candles['ema_fast'])
    )
    bearish_distribution = (
        (candles['close'] < candles['recent_low'])
        & (candles['body_size'] >= candles['distribution_body_avg'] * 1.1)
        & (candles['close'] < candles['ema_fast'])
    )

    candles['amd_phase'] = 'neutral'
    candles['amd_bias'] = 'NEUTRAL'
    candles['amd_score'] = 0.0

    candles.loc[accumulation, 'amd_phase'] = 'accumulation'
    candles.loc[accumulation & (candles['close'] >= candles['rolling_mid']), 'amd_bias'] = 'BULLISH'
    candles.loc[accumulation & (candles['close'] < candles['rolling_mid']), 'amd_bias'] = 'BEARISH'
    candles.loc[accumulation, 'amd_score'] = 0.8

    candles.loc[bullish_manipulation | bearish_manipulation, 'amd_phase'] = 'manipulation'
    candles.loc[bullish_manipulation, 'amd_bias'] = 'BULLISH'
    candles.loc[bearish_manipulation, 'amd_bias'] = 'BEARISH'
    candles.loc[bullish_manipulation | bearish_manipulation, 'amd_score'] = 1.2

    candles.loc[bullish_distribution | bearish_distribution, 'amd_phase'] = 'distribution'
    candles.loc[bullish_distribution, 'amd_bias'] = 'BULLISH'
    candles.loc[bearish_distribution, 'amd_bias'] = 'BEARISH'
    candles.loc[bullish_distribution | bearish_distribution, 'amd_score'] = 1.35
    return candles


def detect_fvg(df: pd.DataFrame, config: ConfluenceConfig) -> pd.DataFrame:
    candles = _prepare_df(df)
    if candles.empty:
        return candles

    candles['bullish_fvg'] = False
    candles['bearish_fvg'] = False
    candles['bullish_fvg_low'] = pd.NA
    candles['bullish_fvg_high'] = pd.NA
    candles['bearish_fvg_low'] = pd.NA
    candles['bearish_fvg_high'] = pd.NA
    candles['fvg_size'] = 0.0

    bullish_gap = candles['low'] - candles['high'].shift(2)
    bearish_gap = candles['low'].shift(2) - candles['high']
    candles.loc[bullish_gap >= config.min_fvg_size, 'bullish_fvg'] = True
    candles.loc[bearish_gap >= config.min_fvg_size, 'bearish_fvg'] = True
    candles.loc[candles['bullish_fvg'], 'bullish_fvg_low'] = candles['high'].shift(2)
    candles.loc[candles['bullish_fvg'], 'bullish_fvg_high'] = candles['low']
    candles.loc[candles['bearish_fvg'], 'bearish_fvg_low'] = candles['high']
    candles.loc[candles['bearish_fvg'], 'bearish_fvg_high'] = candles['low'].shift(2)
    candles['fvg_size'] = bullish_gap.where(candles['bullish_fvg'], 0.0).fillna(0.0) + bearish_gap.where(candles['bearish_fvg'], 0.0).fillna(0.0)
    return candles


def detect_bvg(df: pd.DataFrame, config: ConfluenceConfig) -> pd.DataFrame:
    candles = _prepare_df(df)
    if candles.empty:
        return candles

    candles['bullish_bvg'] = False
    candles['bearish_bvg'] = False
    candles['bullish_bvg_low'] = pd.NA
    candles['bullish_bvg_high'] = pd.NA
    candles['bearish_bvg_low'] = pd.NA
    candles['bearish_bvg_high'] = pd.NA
    candles['bvg_size'] = 0.0

    bullish_gap = candles['low'] - candles['high'].shift(1)
    bearish_gap = candles['low'].shift(1) - candles['high']
    bullish_body = candles['direction'].shift(1) > 0
    bearish_body = candles['direction'].shift(1) < 0
    bullish_follow = candles['close'] >= candles['close'].shift(1)
    bearish_follow = candles['close'] <= candles['close'].shift(1)

    candles.loc[(bullish_gap >= config.min_bvg_size) & bullish_body & bullish_follow, 'bullish_bvg'] = True
    candles.loc[(bearish_gap >= config.min_bvg_size) & bearish_body & bearish_follow, 'bearish_bvg'] = True
    candles.loc[candles['bullish_bvg'], 'bullish_bvg_low'] = candles['high'].shift(1)
    candles.loc[candles['bullish_bvg'], 'bullish_bvg_high'] = candles['low']
    candles.loc[candles['bearish_bvg'], 'bearish_bvg_low'] = candles['high']
    candles.loc[candles['bearish_bvg'], 'bearish_bvg_high'] = candles['low'].shift(1)
    candles['bvg_size'] = bullish_gap.where(candles['bullish_bvg'], 0.0).fillna(0.0) + bearish_gap.where(candles['bearish_bvg'], 0.0).fillna(0.0)
    return candles


def _merge_zone_if_needed(zones: list[dict[str, Any]], zone: dict[str, Any], tolerance_abs: float) -> bool:
    candidate_mid = (float(zone['zone_low']) + float(zone['zone_high'])) / 2.0
    for existing in zones:
        if existing['type'] != zone['type']:
            continue
        existing_mid = (float(existing['zone_low']) + float(existing['zone_high'])) / 2.0
        overlaps = not (float(zone['zone_high']) < float(existing['zone_low']) or float(zone['zone_low']) > float(existing['zone_high']))
        if overlaps or abs(candidate_mid - existing_mid) <= tolerance_abs:
            existing['zone_low'] = round(min(float(existing['zone_low']), float(zone['zone_low'])), 4)
            existing['zone_high'] = round(max(float(existing['zone_high']), float(zone['zone_high'])), 4)
            existing['reaction_strength'] = max(float(existing.get('reaction_strength', 0.0)), float(zone.get('reaction_strength', 0.0)))
            existing['created_index'] = min(int(existing.get('created_index', zone['created_index'])), int(zone['created_index']))
            return True
    return False


def detect_supply_demand_zones(df: pd.DataFrame, config: ConfluenceConfig) -> list[dict[str, Any]]:
    candles = _prepare_df(df)
    if candles.empty:
        return []

    zones: list[dict[str, Any]] = []
    window = max(2, int(config.swing_window))
    for index in range(window, len(candles) - window):
        low_slice = candles['low'].iloc[index - window : index + window + 1]
        high_slice = candles['high'].iloc[index - window : index + window + 1]
        row = candles.iloc[index]
        tolerance_abs = max(float(row['close']) * float(config.zone_merge_tolerance), 0.05)
        future = _window_slice(candles, index + 1, index + 1 + max(config.zone_fresh_bars, 6))
        if future.empty:
            continue

        if float(row['low']) <= float(low_slice.min()) and float(row['close']) >= float(row['open']):
            zone = {
                'type': 'demand',
                'created_index': index,
                'timestamp': row['timestamp'],
                'zone_low': round(float(row['low']), 4),
                'zone_high': round(max(float(row['open']), float(row['close'])), 4),
                'reaction_strength': round(max(0.0, float(future['high'].max()) - max(float(row['open']), float(row['close']))) / max(float(row['bar_range']), 0.1), 4),
            }
            if float(zone['reaction_strength']) >= float(config.min_zone_reaction) and not _merge_zone_if_needed(zones, zone, tolerance_abs):
                zones.append(zone)

        if float(row['high']) >= float(high_slice.max()) and float(row['close']) <= float(row['open']):
            zone = {
                'type': 'supply',
                'created_index': index,
                'timestamp': row['timestamp'],
                'zone_low': round(min(float(row['open']), float(row['close'])), 4),
                'zone_high': round(float(row['high']), 4),
                'reaction_strength': round(max(0.0, min(float(row['open']), float(row['close'])) - float(future['low'].min())) / max(float(row['bar_range']), 0.1), 4),
            }
            if float(zone['reaction_strength']) >= float(config.min_zone_reaction) and not _merge_zone_if_needed(zones, zone, tolerance_abs):
                zones.append(zone)

    return sorted(zones, key=lambda item: (item['created_index'], item['type']))


def detect_liquidity_sweeps(df: pd.DataFrame, config: ConfluenceConfig) -> pd.DataFrame:
    candles = _prepare_df(df)
    if candles.empty:
        return candles

    lookback = max(3, int(config.swing_window) * 2)
    candles['recent_swing_high'] = candles['high'].shift(1).rolling(lookback, min_periods=2).max()
    candles['recent_swing_low'] = candles['low'].shift(1).rolling(lookback, min_periods=2).min()
    candles['bullish_sweep'] = (candles['low'] < candles['recent_swing_low']) & (candles['close'] > candles['recent_swing_low'])
    candles['bearish_sweep'] = (candles['high'] > candles['recent_swing_high']) & (candles['close'] < candles['recent_swing_high'])
    candles['sweep_level'] = candles['recent_swing_low'].where(candles['bullish_sweep'], candles['recent_swing_high'].where(candles['bearish_sweep'], pd.NA))
    return candles


def _recent_flag(series: pd.Series, index: int, lookback: int) -> bool:
    start = max(0, int(index) - int(lookback))
    window = series.iloc[start : index + 1]
    return bool(window.fillna(False).any()) if not window.empty else False


def _latest_true_index(series: pd.Series, index: int, lookback: int) -> int | None:
    start = max(0, int(index) - int(lookback))
    window = series.iloc[start : index + 1]
    true_indices = window[window.fillna(False)].index.tolist()
    return int(true_indices[-1]) if true_indices else None


def _find_active_zone(zones: list[dict[str, Any]], candle: pd.Series, index: int, side: str, config: ConfluenceConfig) -> dict[str, Any] | None:
    zone_type = 'demand' if side == 'BUY' else 'supply'
    best_zone: dict[str, Any] | None = None
    best_distance = float('inf')
    close_price = float(candle['close'])
    tolerance_abs = max(close_price * float(config.retest_tolerance_pct), float(candle['avg_range_5']) * 0.2, 0.05)
    for zone in zones:
        if str(zone.get('type')) != zone_type:
            continue
        age = int(index) - int(zone.get('created_index', index))
        if age < 0 or age > int(config.zone_fresh_bars):
            continue
        zone_low = float(zone['zone_low'])
        zone_high = float(zone['zone_high'])
        touched = float(candle['low']) <= zone_high + tolerance_abs and float(candle['high']) >= zone_low - tolerance_abs
        if not touched:
            continue
        distance = abs(close_price - ((zone_low + zone_high) / 2.0))
        if distance < best_distance:
            best_distance = distance
            best_zone = zone
    return best_zone


def _recent_imbalance_context(candles: pd.DataFrame, index: int, side: str, config: ConfluenceConfig) -> dict[str, Any]:
    tolerance_abs = max(float(candles.iloc[index]['close']) * float(config.retest_tolerance_pct), 0.05)
    result = {
        'has_fvg': False,
        'has_bvg': False,
        'imbalance_type': '',
        'retest_confirmed': False,
        'imbalance_low': None,
        'imbalance_high': None,
    }
    if side == 'BUY':
        fvg_index = _latest_true_index(candles['bullish_fvg'], index, config.max_retest_bars)
        bvg_index = _latest_true_index(candles['bullish_bvg'], index, config.max_retest_bars)
    else:
        fvg_index = _latest_true_index(candles['bearish_fvg'], index, config.max_retest_bars)
        bvg_index = _latest_true_index(candles['bearish_bvg'], index, config.max_retest_bars)

    selected = None
    selected_type = ''
    if fvg_index is not None and (bvg_index is None or fvg_index >= bvg_index):
        selected = fvg_index
        selected_type = 'FVG'
    elif bvg_index is not None:
        selected = bvg_index
        selected_type = 'BVG'

    if selected is None:
        return result

    row = candles.iloc[selected]
    current = candles.iloc[index]
    if side == 'BUY' and selected_type == 'FVG':
        low_bound = float(row['bullish_fvg_low'])
        high_bound = float(row['bullish_fvg_high'])
    elif side == 'BUY':
        low_bound = float(row['bullish_bvg_low'])
        high_bound = float(row['bullish_bvg_high'])
    elif selected_type == 'FVG':
        low_bound = float(row['bearish_fvg_low'])
        high_bound = float(row['bearish_fvg_high'])
    else:
        low_bound = float(row['bearish_bvg_low'])
        high_bound = float(row['bearish_bvg_high'])

    touched = float(current['low']) <= high_bound + tolerance_abs and float(current['high']) >= low_bound - tolerance_abs
    if side == 'BUY':
        retest_confirmed = touched and float(current['close']) > max(float(current['open']), low_bound)
        result['has_fvg'] = selected_type == 'FVG'
        result['has_bvg'] = selected_type == 'BVG'
    else:
        retest_confirmed = touched and float(current['close']) < min(float(current['open']), high_bound)
        result['has_fvg'] = selected_type == 'FVG'
        result['has_bvg'] = selected_type == 'BVG'

    result['imbalance_type'] = selected_type
    result['retest_confirmed'] = bool(retest_confirmed)
    result['imbalance_low'] = round(low_bound, 4)
    result['imbalance_high'] = round(high_bound, 4)
    return result


def score_trade_setup(context: dict[str, Any], config: ConfluenceConfig, mode: str) -> dict[str, Any]:
    normalized_mode = _normalize_mode(mode)
    scoring = ScoringConfig(mode=normalized_mode)
    scoring.thresholds.conservative = float(config.min_score_conservative)
    scoring.thresholds.balanced = float(config.min_score_balanced)
    scoring.thresholds.aggressive = float(config.min_score_aggressive)

    has_fvg = bool(context.get('has_fvg', False))
    has_bvg = bool(context.get('has_bvg', False))
    imbalance_ok = has_fvg or (bool(config.allow_bvg_entries) and has_bvg)
    trend_ok = bool(context.get('trend_alignment', False)) or float(context.get('amd_confidence', 0.0) or 0.0) >= float(config.minimum_amd_confidence)
    sweep_ok = bool(context.get('liquidity_sweep', False))
    retest_ok = bool(context.get('retest_confirmation', False))
    vwap_ok = bool(context.get('vwap_alignment', False))
    distribution_ok = (not bool(config.require_distribution_phase)) or str(context.get('amd_phase', '') or '').strip().lower() == 'distribution'

    score = weighted_score(
        {
            'trend': trend_ok,
            'vwap': vwap_ok,
            'rsi': False,
            'adx': False,
            'zone': bool(context.get('zone_proximity', False)),
            'fvg': imbalance_ok,
            'sweep': sweep_ok,
            'retest': retest_ok,
        },
        scoring,
    )

    blockers: list[str] = []
    if bool(config.require_liquidity_sweep) and not sweep_ok:
        blockers.append('missing_required_sweep')
    if bool(config.require_fvg_confirmation) and not has_fvg:
        blockers.append('missing_required_fvg')
    if bool(config.require_distribution_phase) and not distribution_ok:
        blockers.append('missing_distribution_phase')
    if float(context.get('amd_confidence', 0.0) or 0.0) < float(config.minimum_amd_confidence):
        blockers.append(f'amd_confidence_below_{float(config.minimum_amd_confidence):.2f}')

    accepted = score.accepted and not blockers
    rejection_tokens = [f'missing_{reason}' for reason in score.reasons]
    if score.total < score.threshold:
        rejection_tokens.append(f'score_below_{score.threshold:.2f}')
    rejection_tokens.extend(blockers)

    return {
        'accepted': accepted,
        'threshold': score.threshold,
        'amd_score': round(score.components.get('trend', 0.0), 2),
        'sweep_score': round(score.components.get('sweep', 0.0), 2),
        'imbalance_score': round(score.components.get('fvg', 0.0), 2),
        'zone_score': round(score.components.get('zone', 0.0), 2),
        'trend_score': round(score.components.get('trend', 0.0), 2),
        'retest_score': round(score.components.get('retest', 0.0), 2),
        'total_score': score.total,
        'rejection_reason': '' if accepted else ','.join(rejection_tokens),
    }


def _risk_fraction(risk_pct: float) -> float:
    value = float(risk_pct or 0.0)
    return value / 100.0 if value > 1 else value


def _calculate_quantity(capital: float, risk_pct: float, entry: float, stop_loss: float) -> int:
    risk_per_unit = abs(float(entry) - float(stop_loss))
    if risk_per_unit <= 0:
        return 0
    risk_amount = max(0.0, float(capital)) * _risk_fraction(risk_pct)
    if risk_amount <= 0:
        return 1
    return max(1, floor(risk_amount / risk_per_unit))


def _supportive_amd(row: pd.Series, side: str, mode: str) -> float:
    phase = str(row.get('amd_phase', 'neutral'))
    bias = str(row.get('amd_bias', 'NEUTRAL'))
    if side == 'BUY' and bias == 'BULLISH':
        return float(row.get('amd_score', 0.0) or 0.0)
    if side == 'SELL' and bias == 'BEARISH':
        return float(row.get('amd_score', 0.0) or 0.0)
    if _normalize_mode(mode) == 'Aggressive' and phase in {'accumulation', 'manipulation'}:
        return 0.45
    return 0.0


def _trend_alignment(row: pd.Series, side: str) -> bool:
    if side == 'BUY':
        return float(row['close']) >= float(row['ema_fast']) and float(row['ema_fast']) >= float(row['ema_slow'])
    return float(row['close']) <= float(row['ema_fast']) and float(row['ema_fast']) <= float(row['ema_slow'])


def _stop_buffer(row: pd.Series, config: ConfluenceConfig) -> float:
    return max(float(row['avg_range_5']) * 0.15, float(row['close']) * float(config.retest_tolerance_pct), 0.05)


def _build_reason(side: str, context: dict[str, Any], score: dict[str, Any]) -> str:
    parts = [side]
    if context.get('liquidity_sweep'):
        parts.append('sweep')
    if context.get('zone_type'):
        parts.append(str(context['zone_type']))
    if context.get('has_fvg'):
        parts.append('fvg')
    if context.get('has_bvg'):
        parts.append('bvg')
    if context.get('retest_confirmation'):
        parts.append('retest')
    amd_phase = str(context.get('amd_phase', '') or '')
    if amd_phase and amd_phase != 'neutral':
        parts.append(amd_phase)
    return ' + '.join(parts) + f" | score={float(score.get('total_score', 0.0)):.2f}"


def _timestamp_text(value: object) -> str:
    ts = pd.to_datetime(value, errors='coerce')
    if pd.isna(ts):
        return str(value or '')
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def generate_trades(
    df: pd.DataFrame,
    capital: float,
    risk_pct: float,
    rr_ratio: float | None = None,
    config: ConfluenceConfig | None = None,
) -> list[dict[str, object]]:
    config = config or ConfluenceConfig()
    mode = _normalize_mode(config.mode)
    candles = detect_amd_phase(df, config)
    if candles.empty:
        return []
    fvg = detect_fvg(candles, config)
    bvg = detect_bvg(candles, config)
    sweeps = detect_liquidity_sweeps(candles, config)
    zones = detect_supply_demand_zones(candles, config)

    for column in [
        'bullish_fvg', 'bearish_fvg', 'bullish_fvg_low', 'bullish_fvg_high', 'bearish_fvg_low', 'bearish_fvg_high', 'fvg_size',
        'bullish_bvg', 'bearish_bvg', 'bullish_bvg_low', 'bullish_bvg_high', 'bearish_bvg_low', 'bearish_bvg_high', 'bvg_size',
        'recent_swing_high', 'recent_swing_low', 'bullish_sweep', 'bearish_sweep', 'sweep_level',
    ]:
        candles[column] = fvg[column] if column in fvg.columns else bvg[column] if column in bvg.columns else sweeps[column]

    effective_rr = float(rr_ratio if rr_ratio is not None else config.rr_ratio)
    max_trades_per_day = _mode_max_trades(config, mode)
    trades: list[dict[str, object]] = []
    trade_counts: dict[str, int] = {}
    last_signal_index: dict[str, int] = {'BUY': -10_000, 'SELL': -10_000}
    last_signal_day_side: set[tuple[str, str]] = set()

    start_index = max(int(config.accumulation_lookback), int(config.manipulation_lookback), int(config.swing_window) * 2, 5)
    for index in range(start_index, len(candles)):
        row = candles.iloc[index]
        day_key = str(row['session_day'])
        if trade_counts.get(day_key, 0) >= max_trades_per_day:
            continue

        for side in ['BUY', 'SELL']:
            if index - last_signal_index[side] < int(config.duplicate_signal_cooldown_bars):
                continue
            if not config.allow_secondary_entries and (day_key, side) in last_signal_day_side:
                continue

            session_window = _session_window(row, config)
            if session_window not in {'MORNING', 'AFTERNOON'}:
                continue

            recent_sweep = _recent_flag(candles['bullish_sweep'] if side == 'BUY' else candles['bearish_sweep'], index, config.max_retest_bars)
            zone = _find_active_zone(zones, row, index, side, config)
            if zone is None:
                continue
            nearby_zones = _nearby_zone_count(zones, zone, row, side, config)
            zone_strength_score = _zone_strength_score(zone, row, side, nearby_zones)
            if nearby_zones > int(config.max_nearby_zones):
                continue
            if float(zone.get('reaction_strength', 0.0) or 0.0) < float(config.min_zone_reaction):
                continue
            if zone_strength_score < float(config.min_zone_strength_score):
                continue

            imbalance = _recent_imbalance_context(candles, index, side, config)
            retest_confirmation = bool(imbalance['retest_confirmed'])
            if not retest_confirmation:
                zone_mid = (float(zone['zone_low']) + float(zone['zone_high'])) / 2.0
                if side == 'BUY':
                    retest_confirmation = float(row['close']) > max(float(row['open']), zone_mid)
                else:
                    retest_confirmation = float(row['close']) < min(float(row['open']), zone_mid)
            trend_alignment = _trend_alignment(row, side)
            vwap_alignment = _vwap_alignment(row, side)
            amd_confidence = _supportive_amd(row, side, mode)
            if bool(config.require_retest_confirmation) and not retest_confirmation:
                continue
            if bool(config.require_trend_alignment) and not trend_alignment:
                continue
            if bool(config.require_vwap_alignment) and not vwap_alignment:
                continue

            context = {
                'side': side,
                'amd_phase': str(row['amd_phase']),
                'amd_confidence_value': round(float(amd_confidence), 2),
                'amd_confidence': amd_confidence,
                'liquidity_sweep': recent_sweep,
                'has_fvg': bool(imbalance['has_fvg']),
                'has_bvg': bool(imbalance['has_bvg']),
                'imbalance_type': str(imbalance['imbalance_type'] or ''),
                'zone_proximity': True,
                'zone_type': zone['type'],
                'zone_reaction_strength': float(zone.get('reaction_strength', 0.0) or 0.0),
                'zone_strength_score': float(zone_strength_score),
                'retest_confirmation': retest_confirmation,
                'trend_alignment': trend_alignment,
                'vwap_alignment': vwap_alignment,
            }
            score = score_trade_setup(context, config, mode)
            if not score['accepted']:
                continue

            entry_price = float(row['close'])
            stop_anchor_candidates = [float(row['low'])] if side == 'BUY' else [float(row['high'])]
            if zone is not None:
                stop_anchor_candidates.append(float(zone['zone_low'] if side == 'BUY' else zone['zone_high']))
            sweep_level = row['sweep_level']
            if pd.notna(sweep_level):
                stop_anchor_candidates.append(float(sweep_level))
            if imbalance['imbalance_low'] is not None and imbalance['imbalance_high'] is not None:
                stop_anchor_candidates.append(float(imbalance['imbalance_low'] if side == 'BUY' else imbalance['imbalance_high']))

            stop_buffer = _stop_buffer(row, config)
            if side == 'BUY':
                stop_loss = min(stop_anchor_candidates) - stop_buffer
                if stop_loss >= entry_price:
                    stop_loss = entry_price - max(stop_buffer, float(row['avg_range_5']) * 0.5, 0.1)
                target_price = entry_price + ((entry_price - stop_loss) * effective_rr)
            else:
                stop_loss = max(stop_anchor_candidates) + stop_buffer
                if stop_loss <= entry_price:
                    stop_loss = entry_price + max(stop_buffer, float(row['avg_range_5']) * 0.5, 0.1)
                target_price = entry_price - ((stop_loss - entry_price) * effective_rr)

            quantity = _calculate_quantity(capital, risk_pct, entry_price, stop_loss)
            if quantity <= 0:
                continue

            trade_no = len(trades) + 1
            timestamp_text = _timestamp_text(row['timestamp'])
            reason = _build_reason(side, context, score)
            trade = {
                'timestamp': timestamp_text,
                'entry_time': timestamp_text,
                'side': side,
                'entry': round(entry_price, 4),
                'entry_price': round(entry_price, 4),
                'stop_loss': round(stop_loss, 4),
                'trailing_stop_loss': round(stop_loss, 4),
                'trailing_sl_pct': round(float(config.trailing_sl_pct), 4),
                'target': round(target_price, 4),
                'target_price': round(target_price, 4),
                'strategy': STRATEGY_NAME,
                'trade_no': trade_no,
                'trade_label': f'Trade {trade_no}',
                'reason': reason,
                'amd_phase': str(row['amd_phase']),
                'amd_confidence_value': round(float(amd_confidence), 2),
                'imbalance_type': str(context['imbalance_type'] or ('FVG' if context['has_fvg'] else 'BVG' if context['has_bvg'] else '')),
                'zone_type': str(context['zone_type'] or ''),
                'score': float(score['total_score']),
                'amd_score': float(score['amd_score']),
                'imbalance_score': float(score['imbalance_score']),
                'zone_score': float(score['zone_score']),
                'sweep_score': float(score['sweep_score']),
                'total_score': float(score['total_score']),
                'trend_score': float(score['trend_score']),
                'retest_score': float(score['retest_score']),
                'rejection_reason': '',
                'quantity': int(quantity),
                'risk_per_unit': round(abs(entry_price - stop_loss), 4),
                'rr_ratio': round(effective_rr, 2),
                'mode': mode,
                'liquidity_sweep': 'YES' if recent_sweep else 'NO',
                'zone_reaction_strength': round(float(context['zone_reaction_strength']), 4),
                'zone_strength_score': round(float(context['zone_strength_score']), 2),
                'vwap_aligned': 'YES' if vwap_alignment else 'NO',
                'session_allowed': 'YES',
                'session_window': session_window,
            }
            if zone is not None:
                trade['zone_low'] = round(float(zone['zone_low']), 4)
                trade['zone_high'] = round(float(zone['zone_high']), 4)
            if imbalance['imbalance_low'] is not None:
                trade['imbalance_low'] = imbalance['imbalance_low']
                trade['imbalance_high'] = imbalance['imbalance_high']
            trades.append(trade)
            trade_counts[day_key] = trade_counts.get(day_key, 0) + 1
            last_signal_index[side] = index
            last_signal_day_side.add((day_key, side))
            break

    return trades


__all__ = [
    'ConfluenceConfig',
    'detect_amd_phase',
    'detect_fvg',
    'detect_bvg',
    'detect_supply_demand_zones',
    'detect_liquidity_sweeps',
    'score_trade_setup',
    'generate_trades',
]



