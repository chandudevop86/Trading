from __future__ import annotations

from math import floor
from typing import Any

import pandas as pd

from vinayak.strategies.breakout.service import Candle
from vinayak.strategies.common.base import StrategySignal


def _coerce_candles(data: list[Candle]) -> pd.DataFrame:
    rows = [
        {
            'timestamp': item.timestamp,
            'open': item.open,
            'high': item.high,
            'low': item.low,
            'close': item.close,
            'volume': item.volume,
        }
        for item in data
    ]
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['open', 'high', 'low', 'close']).reset_index(drop=True)


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


def _build_signal(side: str, entry: float, stop: float, rr_ratio: float, capital: float, risk_pct: float, symbol: str, timestamp, metadata: dict[str, Any]) -> StrategySignal | None:
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    target = entry + (risk * rr_ratio) if side == 'BUY' else entry - (risk * rr_ratio)
    quantity = _calc_qty(capital, risk_pct, entry, stop)
    return StrategySignal(
        strategy_name='Demand Supply',
        symbol=symbol,
        side=side,
        entry_price=round(entry, 4),
        stop_loss=round(stop, 4),
        target_price=round(target, 4),
        signal_time=timestamp,
        metadata={**metadata, 'quantity': int(quantity)},
    )


def _build_signal_from_zone(*, candle: pd.Series, zone: dict[str, Any], rr_ratio: float, capital: float, risk_pct: float, symbol: str) -> StrategySignal | None:
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
    touch_buffer = max(close * 0.003, 0.3)

    if zone_type == 'demand':
        touched = low <= (zone_high + touch_buffer) and high >= (zone_low - touch_buffer)
        rejection_ok = close > open_price and close >= zone_mid
        stop = min(low, zone_low) - max(close * 0.001, 0.05)
        side = 'BUY'
    elif zone_type == 'supply':
        touched = high >= (zone_low - touch_buffer) and low <= (zone_high + touch_buffer)
        rejection_ok = close < open_price and close <= zone_mid
        stop = max(high, zone_high) + max(close * 0.001, 0.05)
        side = 'SELL'
    else:
        return None

    if not touched or not rejection_ok:
        return None

    return _build_signal(
        side=side,
        entry=close,
        stop=stop,
        rr_ratio=rr_ratio,
        capital=capital,
        risk_pct=risk_pct,
        symbol=symbol,
        timestamp=candle.get('timestamp'),
        metadata={
            'zone_type': zone_type,
            'zone_low': round(zone_low, 4),
            'zone_high': round(zone_high, 4),
            'zone_source': zone.get('source', ''),
            'structure_level': zone.get('structure_level', ''),
        },
    )


def _build_fallback_signal(df: pd.DataFrame, rr_ratio: float, capital: float, risk_pct: float, symbol: str) -> StrategySignal | None:
    if len(df) < 3:
        return None
    recent = df.tail(3).reset_index(drop=True)
    closes = recent['close'].tolist()
    if closes[2] > closes[1] > closes[0]:
        return _build_signal(
            side='BUY',
            entry=float(recent['close'].iloc[-1]),
            stop=float(recent['low'].min()) - 0.05,
            rr_ratio=rr_ratio,
            capital=capital,
            risk_pct=risk_pct,
            symbol=symbol,
            timestamp=recent['timestamp'].iloc[-1],
            metadata={'zone_type': 'fallback', 'zone_source': 'momentum'},
        )
    if closes[2] < closes[1] < closes[0]:
        return _build_signal(
            side='SELL',
            entry=float(recent['close'].iloc[-1]),
            stop=float(recent['high'].max()) + 0.05,
            rr_ratio=rr_ratio,
            capital=capital,
            risk_pct=risk_pct,
            symbol=symbol,
            timestamp=recent['timestamp'].iloc[-1],
            metadata={'zone_type': 'fallback', 'zone_source': 'momentum'},
        )
    return None


def run_demand_supply_strategy(
    candles: list[Candle],
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    include_fvg: bool = True,
    include_bos: bool = True,
) -> list[StrategySignal]:
    df = _coerce_candles(candles)
    if df.empty:
        return []

    zones: list[dict[str, Any]] = []
    seen: set[tuple[str, int, float, float, str]] = set()
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []

    for i in range(2, len(df) - 2):
        high = float(df['high'].iloc[i])
        low = float(df['low'].iloc[i])
        prev_high = float(df['high'].iloc[i - 1])
        next_high = float(df['high'].iloc[i + 1])
        prev_low = float(df['low'].iloc[i - 1])
        next_low = float(df['low'].iloc[i + 1])

        if high > prev_high and high > next_high:
            swing_highs.append((i, high))
            _append_zone(zones, seen, zone_type='supply', index=i, price=high, zone_low=high, zone_high=high, source='pivot')
        if low < prev_low and low < next_low:
            swing_lows.append((i, low))
            _append_zone(zones, seen, zone_type='demand', index=i, price=low, zone_low=low, zone_high=low, source='pivot')

    if include_fvg:
        for i in range(1, len(df) - 1):
            prev_high = float(df['high'].iloc[i - 1])
            prev_low = float(df['low'].iloc[i - 1])
            next_high = float(df['high'].iloc[i + 1])
            next_low = float(df['low'].iloc[i + 1])
            if prev_high < next_low:
                _append_zone(zones, seen, zone_type='demand', index=i, price=(prev_high + next_low) / 2.0, zone_low=prev_high, zone_high=next_low, source='fvg')
            if prev_low > next_high:
                _append_zone(zones, seen, zone_type='supply', index=i, price=(next_high + prev_low) / 2.0, zone_low=next_high, zone_high=prev_low, source='fvg')

    if include_bos:
        for i in range(1, len(df)):
            open_price = float(df['open'].iloc[i])
            high = float(df['high'].iloc[i])
            low = float(df['low'].iloc[i])
            close = float(df['close'].iloc[i])
            prev_close = float(df['close'].iloc[i - 1])
            prior_swing_highs = [level for idx, level in swing_highs if idx < i]
            prior_swing_lows = [level for idx, level in swing_lows if idx < i]
            if prior_swing_highs:
                last_swing_high = prior_swing_highs[-1]
                if close > last_swing_high and prev_close <= last_swing_high:
                    zone_high = max(open_price, close)
                    _append_zone(zones, seen, zone_type='demand', index=i, price=zone_high, zone_low=low, zone_high=zone_high, source='bos', structure_level=last_swing_high)
            if prior_swing_lows:
                last_swing_low = prior_swing_lows[-1]
                if close < last_swing_low and prev_close >= last_swing_low:
                    zone_low = min(open_price, close)
                    _append_zone(zones, seen, zone_type='supply', index=i, price=zone_low, zone_low=zone_low, zone_high=high, source='bos', structure_level=last_swing_low)

    ordered_zones = sorted(zones, key=lambda zone: int(zone['index']))
    signals: list[StrategySignal] = []
    for i in range(1, len(df)):
        candle = df.iloc[i]
        prior_zones = [zone for zone in ordered_zones if int(zone['index']) < i]
        if not prior_zones:
            continue
        for zone in reversed(prior_zones[-6:]):
            signal = _build_signal_from_zone(
                candle=candle,
                zone=zone,
                rr_ratio=float(rr_ratio),
                capital=float(capital),
                risk_pct=float(risk_pct),
                symbol=symbol,
            )
            if signal is None:
                continue
            signals.append(signal)
            break

    if signals:
        return signals

    fallback = _build_fallback_signal(df, float(rr_ratio), float(capital), float(risk_pct), symbol)
    return [fallback] if fallback is not None else []
