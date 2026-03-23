from __future__ import annotations

from collections.abc import Sequence
from math import floor
from typing import Any

import pandas as pd


def _coerce_candles(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, Sequence):
        rows: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                rows.append(
                    {
                        'timestamp': item.get('timestamp'),
                        'open': item.get('open'),
                        'high': item.get('high'),
                        'low': item.get('low'),
                        'close': item.get('close'),
                        'volume': item.get('volume'),
                    }
                )
            else:
                rows.append(
                    {
                        'timestamp': getattr(item, 'timestamp', None),
                        'open': getattr(item, 'open', None),
                        'high': getattr(item, 'high', None),
                        'low': getattr(item, 'low', None),
                        'close': getattr(item, 'close', None),
                        'volume': getattr(item, 'volume', None),
                    }
                )
        df = pd.DataFrame(rows)
    else:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    if df.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df.columns = [str(col).strip().lower() for col in df.columns]
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'timestamp' not in df.columns:
        df['timestamp'] = None

    return df.dropna(subset=['open', 'high', 'low', 'close']).reset_index(drop=True)


def _body_low(candle: pd.Series) -> float:
    return float(min(float(candle['open']), float(candle['close'])))


def _body_high(candle: pd.Series) -> float:
    return float(max(float(candle['open']), float(candle['close'])))


def _base_bounds(candles: pd.DataFrame, start_index: int, end_index: int) -> tuple[float, float]:
    start = max(0, int(start_index))
    end = min(len(candles) - 1, int(end_index))
    window = candles.iloc[start : end + 1]
    if window.empty:
        return 0.0, 0.0

    body_lows = window.apply(_body_low, axis=1)
    body_highs = window.apply(_body_high, axis=1)
    zone_low = float(body_lows.min())
    zone_high = float(body_highs.max())
    if zone_high <= zone_low:
        zone_low = float(window['low'].min())
        zone_high = float(window['high'].max())
    return zone_low, zone_high


def _append_zone(
    zones: list[dict[str, Any]],
    seen: set[tuple[str, int, float, float, str]],
    *,
    zone_type: str,
    index: int,
    price: float,
    zone_low: float,
    zone_high: float,
    source: str,
    structure_level: float | None = None,
    boundary_mode: str = 'BASE',
) -> None:
    key = (zone_type, index, round(price, 6), round(zone_low, 6), source)
    if key in seen:
        return
    seen.add(key)
    zone = {
        'type': zone_type,
        'price': round(float(price), 4),
        'index': int(index),
        'zone_low': round(float(zone_low), 4),
        'zone_high': round(float(zone_high), 4),
        'source': source,
        'boundary_mode': boundary_mode,
    }
    if structure_level is not None:
        zone['structure_level'] = round(float(structure_level), 4)
    zones.append(zone)


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    if capital <= 0 or risk_pct <= 0:
        return 1
    return max(1, floor((capital * risk_pct) / risk_per_unit))


def _touch_buffer(price: float) -> float:
    return max(float(price) * 0.0015, 0.15)


def _zone_touched(candle: pd.Series, zone_low: float, zone_high: float, buffer_size: float) -> bool:
    high = float(candle['high'])
    low = float(candle['low'])
    return low <= (zone_high + buffer_size) and high >= (zone_low - buffer_size)


def _zone_is_fresh(candles: pd.DataFrame, zone: dict[str, Any], current_index: int, buffer_size: float) -> bool:
    zone_index = int(zone['index'])
    if current_index - zone_index < 1:
        return False

    zone_low = float(zone['zone_low'])
    zone_high = float(zone['zone_high'])
    for idx in range(zone_index + 1, current_index):
        if _zone_touched(candles.iloc[idx], zone_low, zone_high, buffer_size):
            return False
    return True


def _avg_range(candles: pd.DataFrame, current_index: int, lookback: int = 5) -> float:
    start = max(0, current_index - lookback)
    window = candles.iloc[start:current_index]
    if window.empty:
        return 0.0
    ranges = (window['high'] - window['low']).astype(float)
    if ranges.empty:
        return 0.0
    return float(ranges.mean())


def _departure_strength(candles: pd.DataFrame, zone: dict[str, Any]) -> bool:
    zone_index = int(zone['index'])
    zone_type = str(zone['type']).strip().lower()
    start = zone_index + 1
    end = min(len(candles), zone_index + 4)
    if start >= end:
        return False

    departure = candles.iloc[start:end]
    before_start = max(0, zone_index - 4)
    before = candles.iloc[before_start:zone_index]
    departure_avg_range = float((departure['high'] - departure['low']).mean()) if not departure.empty else 0.0
    prior_avg_range = float((before['high'] - before['low']).mean()) if not before.empty else 0.0
    if departure_avg_range <= 0:
        return False

    if zone_type == 'demand':
        move_ok = float(departure['close'].max()) > float(zone['zone_high']) + (departure_avg_range * 0.35)
    else:
        move_ok = float(departure['close'].min()) < float(zone['zone_low']) - (departure_avg_range * 0.35)

    range_ok = prior_avg_range <= 0 or departure_avg_range >= (prior_avg_range * 0.7)
    return bool(move_ok and range_ok)


def _compact_zone(candles: pd.DataFrame, zone: dict[str, Any]) -> bool:
    zone_index = int(zone['index'])
    avg_range = _avg_range(candles, zone_index + 1)
    zone_width = abs(float(zone['zone_high']) - float(zone['zone_low']))
    if zone_width <= 0:
        return True
    if avg_range <= 0:
        return zone_width <= max(abs(float(zone['price'])) * 0.004, 0.5)
    return zone_width <= (avg_range * 1.6)


def _higher_timeframe_bias(candles: pd.DataFrame, current_index: int, group_size: int = 3) -> str:
    if current_index < (group_size * 5):
        return 'NEUTRAL'

    closes = candles['close'].iloc[: current_index + 1].astype(float).reset_index(drop=True)
    grouped: list[float] = []
    for start in range(0, len(closes), group_size):
        chunk = closes.iloc[start : start + group_size]
        if len(chunk) < group_size:
            continue
        grouped.append(float(chunk.iloc[-1]))

    if len(grouped) < 5:
        return 'NEUTRAL'

    series = pd.Series(grouped, dtype='float64')
    fast = series.ewm(span=3, adjust=False).mean().iloc[-1]
    slow = series.ewm(span=5, adjust=False).mean().iloc[-1]
    slope = series.iloc[-1] - series.iloc[-3]

    if fast > slow and slope > 0:
        return 'BULLISH'
    if fast < slow and slope < 0:
        return 'BEARISH'
    return 'NEUTRAL'


def _zone_score(candles: pd.DataFrame, zone: dict[str, Any], current_index: int, buffer_size: float) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    source = str(zone.get('source', '')).lower()

    if _zone_is_fresh(candles, zone, current_index, buffer_size):
        score += 1
        reasons.append('fresh')
    if _compact_zone(candles, zone):
        score += 1
        reasons.append('compact')
    if _departure_strength(candles, zone):
        score += 1
        reasons.append('strong_departure')
    if source in {'bos', 'fvg'}:
        score += 1
        reasons.append(source)
    if zone.get('structure_level') not in {None, ''}:
        score += 1
        reasons.append('structure_break')

    return score, reasons


def _accumulation_profile(
    candles: pd.DataFrame,
    zone: dict[str, Any],
    *,
    lookback: int = 6,
    longer_lookback: int = 12,
) -> dict[str, Any]:
    zone_index = int(zone['index'])
    end = zone_index + 1
    start = max(0, end - max(int(lookback), 3))
    window = candles.iloc[start:end]
    if len(window) < 3:
        return {
            'is_valid': False,
            'range_width': 0.0,
            'avg_range': 0.0,
            'compression_ratio': 0.0,
            'close_span': 0.0,
            'reason': 'insufficient_history',
        }

    longer_start = max(0, end - max(int(longer_lookback), len(window)))
    longer_window = candles.iloc[longer_start:end]
    range_width = float(window['high'].max() - window['low'].min())
    avg_range = float((window['high'] - window['low']).mean())
    longer_avg_range = float((longer_window['high'] - longer_window['low']).mean()) if not longer_window.empty else avg_range
    close_span = float(window['close'].max() - window['close'].min())
    reference_price = max(abs(float(zone['price'])), 1.0)
    compression_ratio = (avg_range / longer_avg_range) if longer_avg_range > 0 else 1.0
    sideways_ok = range_width <= max(avg_range * 3.0, reference_price * 0.03)
    low_vol_ok = longer_avg_range <= 0 or compression_ratio <= 1.1
    close_ok = close_span <= max(avg_range * 2.0, reference_price * 0.02)
    is_valid = bool(sideways_ok and low_vol_ok and close_ok)
    reason = 'sideways_low_vol' if is_valid else 'range_expansion'
    return {
        'is_valid': is_valid,
        'range_width': round(range_width, 4),
        'avg_range': round(avg_range, 4),
        'compression_ratio': round(compression_ratio, 4),
        'close_span': round(close_span, 4),
        'reason': reason,
    }


def _manipulation_profile(
    candle: pd.Series,
    zone: dict[str, Any],
    *,
    buffer_size: float,
) -> dict[str, Any]:
    open_price = float(candle['open'])
    high = float(candle['high'])
    low = float(candle['low'])
    close = float(candle['close'])
    zone_low = float(zone['zone_low'])
    zone_high = float(zone['zone_high'])
    zone_mid = (zone_low + zone_high) / 2.0
    candle_range = max(high - low, 0.0)
    body = abs(close - open_price)
    lower_wick = max(min(open_price, close) - low, 0.0)
    upper_wick = max(high - max(open_price, close), 0.0)
    zone_type = str(zone['type']).strip().lower()

    if zone_type == 'demand':
        sweep = low < (zone_low - (buffer_size * 0.15))
        reclaim = close >= zone_low and close >= zone_mid
        wick_ok = lower_wick >= max(body * 0.75, candle_range * 0.2)
        direction_ok = close > open_price
    elif zone_type == 'supply':
        sweep = high > (zone_high + (buffer_size * 0.15))
        reclaim = close <= zone_high and close <= zone_mid
        wick_ok = upper_wick >= max(body * 0.75, candle_range * 0.2)
        direction_ok = close < open_price
    else:
        sweep = False
        reclaim = False
        wick_ok = False
        direction_ok = False

    is_valid = bool(sweep and reclaim and wick_ok and direction_ok)
    return {
        'is_valid': is_valid,
        'sweep': sweep,
        'reclaim': reclaim,
        'wick_ok': wick_ok,
        'direction_ok': direction_ok,
        'reason': 'false_break_reversal' if is_valid else 'no_false_break',
    }


def _distribution_profile(
    candles: pd.DataFrame,
    candle_index: int,
    candle: pd.Series,
    zone: dict[str, Any],
) -> dict[str, Any]:
    start = max(0, candle_index - 3)
    prior = candles.iloc[start:candle_index]
    open_price = float(candle['open'])
    high = float(candle['high'])
    low = float(candle['low'])
    close = float(candle['close'])
    candle_range = max(high - low, 0.0)
    body = abs(close - open_price)
    body_ratio = (body / candle_range) if candle_range > 0 else 0.0
    avg_range = _avg_range(candles, candle_index, lookback=4)
    expansion_ok = avg_range <= 0 or candle_range >= (avg_range * 1.05)
    zone_type = str(zone['type']).strip().lower()

    if prior.empty:
        structure_break = True
    elif zone_type == 'demand':
        structure_break = close > float(prior['high'].max())
    else:
        structure_break = close < float(prior['low'].min())

    if zone_type == 'demand':
        direction_ok = close > open_price and close > float(zone['zone_high'])
    elif zone_type == 'supply':
        direction_ok = close < open_price and close < float(zone['zone_low'])
    else:
        direction_ok = False

    strong_body = body_ratio >= 0.45
    is_valid = bool(strong_body and structure_break and direction_ok and expansion_ok)
    return {
        'is_valid': is_valid,
        'body_ratio': round(body_ratio, 4),
        'structure_break': structure_break,
        'expansion_ok': expansion_ok,
        'direction_ok': direction_ok,
        'reason': 'displacement_break' if is_valid else 'weak_follow_through',
    }


def _next_opposing_zone_price(
    ordered_zones: list[dict[str, Any]],
    current_zone: dict[str, Any],
    candle_index: int,
) -> float | None:
    current_type = str(current_zone['type']).strip().lower()
    opposite_type = 'supply' if current_type == 'demand' else 'demand'
    candidates: list[float] = []

    for zone in ordered_zones:
        zone_index = int(zone['index'])
        if zone_index >= candle_index:
            continue
        if zone_index == int(current_zone['index']):
            continue
        if str(zone.get('type', '')).strip().lower() != opposite_type:
            continue
        if opposite_type == 'supply':
            candidates.append(float(zone['zone_low']))
        else:
            candidates.append(float(zone['zone_high']))

    if not candidates:
        return None

    entry_reference = float(current_zone['zone_high'] if current_type == 'demand' else current_zone['zone_low'])
    if current_type == 'demand':
        above = [price for price in candidates if price > entry_reference]
        return min(above) if above else None
    below = [price for price in candidates if price < entry_reference]
    return max(below) if below else None


def _build_signal_from_zone(
    *,
    candles: pd.DataFrame,
    candle_index: int,
    candle: pd.Series,
    zone: dict[str, Any],
    ordered_zones: list[dict[str, Any]],
    rr_ratio: float,
    capital: float,
    risk_pct: float,
    min_zone_score: int = 3,
    use_amd_filters: bool = False,
    accumulation_lookback: int = 6,
    accumulation_longer_lookback: int = 12,
) -> dict[str, Any] | None:
    open_price = float(candle['open'])
    high = float(candle['high'])
    low = float(candle['low'])
    close = float(candle['close'])
    zone_low = float(zone['zone_low'])
    zone_high = float(zone['zone_high'])
    zone_mid = (zone_low + zone_high) / 2.0
    zone_type = str(zone['type']).strip().lower()

    side = ''
    touched = False
    rejection_ok = False
    stop = 0.0
    candle_range = max(high - low, 0.0)
    body = abs(close - open_price)
    lower_wick = max(min(open_price, close) - low, 0.0)
    upper_wick = max(high - max(open_price, close), 0.0)
    close_location = 0.5 if candle_range <= 0 else (close - low) / candle_range
    buffer_size = _touch_buffer(close)
    average_range = _avg_range(candles, candle_index)
    zone_score, zone_reasons = _zone_score(candles, zone, candle_index, buffer_size)
    higher_tf_bias = _higher_timeframe_bias(candles, candle_index)
    accumulation = _accumulation_profile(
        candles,
        zone,
        lookback=accumulation_lookback,
        longer_lookback=accumulation_longer_lookback,
    )
    manipulation = _manipulation_profile(candle, zone, buffer_size=buffer_size)
    distribution = _distribution_profile(candles, candle_index, candle, zone)

    if zone_score < int(min_zone_score):
        return None
    if average_range > 0 and candle_range < (average_range * 0.8):
        return None

    if zone_type == 'demand':
        if higher_tf_bias == 'BEARISH':
            return None
        touched = _zone_touched(candle, zone_low, zone_high, buffer_size)
        rejection_ok = (
            close > open_price
            and close >= zone_mid
            and lower_wick >= max(body, candle_range * 0.2)
            and close_location >= 0.55
        )
        stop = min(low, zone_low) - max(close * 0.001, 0.05)
        side = 'BUY'
    elif zone_type == 'supply':
        if higher_tf_bias == 'BULLISH':
            return None
        touched = _zone_touched(candle, zone_low, zone_high, buffer_size)
        rejection_ok = (
            close < open_price
            and close <= zone_mid
            and upper_wick >= max(body, candle_range * 0.2)
            and close_location <= 0.45
        )
        stop = max(high, zone_high) + max(close * 0.001, 0.05)
        side = 'SELL'
    else:
        return None

    if not touched or not rejection_ok:
        return None

    if use_amd_filters and not (accumulation['is_valid'] and manipulation['is_valid'] and distribution['is_valid']):
        return None

    entry = close
    risk = abs(entry - stop)
    if risk <= 0:
        return None

    if side == 'BUY':
        target = entry + (risk * rr_ratio)
    else:
        target = entry - (risk * rr_ratio)

    opposing_zone_price = _next_opposing_zone_price(ordered_zones, zone, candle_index)
    if opposing_zone_price is not None:
        if side == 'BUY':
            available_room = opposing_zone_price - entry
        else:
            available_room = entry - opposing_zone_price
        if available_room < (risk * rr_ratio):
            return None

    quantity = _calc_qty(capital, risk_pct, entry, stop)
    timestamp = candle.get('timestamp', '')
    amd_state = 'CONFIRMED' if (accumulation['is_valid'] and manipulation['is_valid'] and distribution['is_valid']) else 'PARTIAL'

    return {
        'strategy': 'DEMAND_SUPPLY',
        'timestamp': timestamp,
        'entry_time': timestamp,
        'side': side,
        'entry_price': round(entry, 4),
        'stop_loss': round(stop, 4),
        'trailing_stop_loss': round(stop, 4),
        'target_price': round(target, 4),
        'quantity': int(quantity),
        'signal': f'{zone_type.upper()}_RETEST',
        'zone_type': zone_type,
        'zone_low': round(zone_low, 4),
        'zone_high': round(zone_high, 4),
        'zone_source': zone.get('source', ''),
        'zone_boundary_mode': zone.get('boundary_mode', 'BASE'),
        'structure_level': zone.get('structure_level', ''),
        'zone_fresh': 'YES',
        'zone_score': int(zone_score),
        'zone_reasons': ','.join(zone_reasons),
        'higher_tf_bias': higher_tf_bias,
        'opposing_zone_price': round(opposing_zone_price, 4) if opposing_zone_price is not None else '',
        'accumulation_ok': 'YES' if accumulation['is_valid'] else 'NO',
        'accumulation_reason': accumulation['reason'],
        'accumulation_range_width': accumulation['range_width'],
        'accumulation_compression_ratio': accumulation['compression_ratio'],
        'manipulation_ok': 'YES' if manipulation['is_valid'] else 'NO',
        'manipulation_reason': manipulation['reason'],
        'distribution_ok': 'YES' if distribution['is_valid'] else 'NO',
        'distribution_reason': distribution['reason'],
        'distribution_body_ratio': distribution['body_ratio'],
        'distribution_structure_break': 'YES' if distribution['structure_break'] else 'NO',
        'amd_state': amd_state,
        'amd_filter_enabled': 'YES' if use_amd_filters else 'NO',
    }


def generate_trades(
    df: Any,
    include_fvg: bool = True,
    include_bos: bool = True,
    capital: float = 100000.0,
    risk_pct: float = 0.01,
    rr_ratio: float = 2.0,
    use_amd_filters: bool = False,
    accumulation_lookback: int = 6,
    accumulation_longer_lookback: int = 12,
    **_: Any,
) -> list[dict[str, Any]]:
    candles = _coerce_candles(df)
    zones: list[dict[str, Any]] = []
    seen: set[tuple[str, int, float, float, str]] = set()
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []

    for i in range(2, len(candles) - 2):
        high = float(candles['high'].iloc[i])
        low = float(candles['low'].iloc[i])

        prev_high = float(candles['high'].iloc[i - 1])
        next_high = float(candles['high'].iloc[i + 1])

        prev_low = float(candles['low'].iloc[i - 1])
        next_low = float(candles['low'].iloc[i + 1])

        if high > prev_high and high > next_high:
            swing_highs.append((i, high))
            zone_low, zone_high = _base_bounds(candles, i - 1, i)
            _append_zone(
                zones,
                seen,
                zone_type='supply',
                index=i,
                price=(zone_low + zone_high) / 2.0,
                zone_low=zone_low,
                zone_high=zone_high,
                source='pivot',
                boundary_mode='BASE_BODY',
            )

        if low < prev_low and low < next_low:
            swing_lows.append((i, low))
            zone_low, zone_high = _base_bounds(candles, i - 1, i)
            _append_zone(
                zones,
                seen,
                zone_type='demand',
                index=i,
                price=(zone_low + zone_high) / 2.0,
                zone_low=zone_low,
                zone_high=zone_high,
                source='pivot',
                boundary_mode='BASE_BODY',
            )

    if include_fvg:
        for i in range(1, len(candles) - 1):
            prev_high = float(candles['high'].iloc[i - 1])
            prev_low = float(candles['low'].iloc[i - 1])
            next_high = float(candles['high'].iloc[i + 1])
            next_low = float(candles['low'].iloc[i + 1])

            if prev_high < next_low:
                zone_low = prev_high
                zone_high = next_low
                _append_zone(
                    zones,
                    seen,
                    zone_type='demand',
                    index=i,
                    price=(zone_low + zone_high) / 2.0,
                    zone_low=zone_low,
                    zone_high=zone_high,
                    source='fvg',
                    boundary_mode='GAP',
                )

            if prev_low > next_high:
                zone_low = next_high
                zone_high = prev_low
                _append_zone(
                    zones,
                    seen,
                    zone_type='supply',
                    index=i,
                    price=(zone_low + zone_high) / 2.0,
                    zone_low=zone_low,
                    zone_high=zone_high,
                    source='fvg',
                    boundary_mode='GAP',
                )

    if include_bos:
        for i in range(1, len(candles)):
            close = float(candles['close'].iloc[i])
            prev_close = float(candles['close'].iloc[i - 1])

            prior_swing_highs = [level for idx, level in swing_highs if idx < i]
            prior_swing_lows = [level for idx, level in swing_lows if idx < i]

            if prior_swing_highs:
                last_swing_high = prior_swing_highs[-1]
                if close > last_swing_high and prev_close <= last_swing_high:
                    zone_low, zone_high = _base_bounds(candles, i - 1, i)
                    _append_zone(
                        zones,
                        seen,
                        zone_type='demand',
                        index=i,
                        price=(zone_low + zone_high) / 2.0,
                        zone_low=zone_low,
                        zone_high=zone_high,
                        source='bos',
                        structure_level=last_swing_high,
                        boundary_mode='BASE_BODY',
                    )

            if prior_swing_lows:
                last_swing_low = prior_swing_lows[-1]
                if close < last_swing_low and prev_close >= last_swing_low:
                    zone_low, zone_high = _base_bounds(candles, i - 1, i)
                    _append_zone(
                        zones,
                        seen,
                        zone_type='supply',
                        index=i,
                        price=(zone_low + zone_high) / 2.0,
                        zone_low=zone_low,
                        zone_high=zone_high,
                        source='bos',
                        structure_level=last_swing_low,
                        boundary_mode='BASE_BODY',
                    )

    ordered_zones = sorted(zones, key=lambda zone: int(zone['index']))
    signals: list[dict[str, Any]] = []
    used_zone_keys: set[tuple[int, str, str]] = set()

    for i in range(1, len(candles)):
        candle = candles.iloc[i]
        prior_zones = [zone for zone in ordered_zones if int(zone['index']) < i]
        if not prior_zones:
            continue

        for zone in reversed(prior_zones[-6:]):
            zone_key = (int(zone['index']), str(zone['type']), str(zone.get('source', '')))
            if zone_key in used_zone_keys:
                continue
            signal = _build_signal_from_zone(
                candles=candles,
                candle_index=i,
                candle=candle,
                zone=zone,
                ordered_zones=ordered_zones,
                rr_ratio=float(rr_ratio),
                capital=float(capital),
                risk_pct=float(risk_pct),
                use_amd_filters=bool(use_amd_filters),
                accumulation_lookback=int(accumulation_lookback),
                accumulation_longer_lookback=int(accumulation_longer_lookback),
            )
            if signal is None:
                continue
            signal['zone_index'] = int(zone['index'])
            signal['zone_price'] = zone.get('price', '')
            signals.append(signal)
            used_zone_keys.add(zone_key)
            break

    return signals