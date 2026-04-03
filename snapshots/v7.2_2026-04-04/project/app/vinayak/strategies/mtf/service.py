from __future__ import annotations

from dataclasses import dataclass
from math import floor

from vinayak.strategies.breakout.service import Candle, build_indicator_snapshot, ensure_required_indicator_candles
from vinayak.strategies.common.base import StrategySignal


@dataclass
class AggCandle:
    start_idx: int
    end_idx: int
    timestamp: object
    open: float
    high: float
    low: float
    close: float


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    grouped: dict[object, list[Candle]] = {}
    for candle in candles:
        grouped.setdefault(candle.timestamp.date(), []).append(candle)
    return grouped


def _aggregate_chunks(day_candles: list[Candle], chunk_size: int) -> list[AggCandle]:
    bars: list[AggCandle] = []
    for start in range(0, len(day_candles), chunk_size):
        chunk = day_candles[start:start + chunk_size]
        if len(chunk) < chunk_size:
            break
        bars.append(
            AggCandle(
                start_idx=start,
                end_idx=start + chunk_size - 1,
                timestamp=chunk[-1].timestamp,
                open=float(chunk[0].open),
                high=max(float(c.high) for c in chunk),
                low=min(float(c.low) for c in chunk),
                close=float(chunk[-1].close),
            )
        )
    return bars


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    out = [float(values[0])]
    for value in values[1:]:
        out.append((float(value) * alpha) + (out[-1] * (1.0 - alpha)))
    return out


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk = abs(entry - stop)
    if risk <= 0:
        return 0
    return floor((capital * risk_pct) / risk)


def _setup_confirmation(completed_15m: list[AggCandle], side: str, setup_mode: str) -> tuple[bool, str, float, float]:
    if len(completed_15m) < 2:
        return False, '', 0.0, 0.0
    latest = completed_15m[-1]
    prev = completed_15m[-2]
    prev2 = completed_15m[-3] if len(completed_15m) >= 3 else None
    mode = (setup_mode or 'either').strip().lower()

    if side == 'BUY':
        bos = latest.close > latest.open and latest.close > prev.high
        fvg = prev2 is not None and prev2.high < latest.low
        if mode == 'bos':
            fvg = False
        elif mode == 'fvg':
            bos = False
        if not bos and not fvg:
            return False, '', 0.0, 0.0
        zone_low = prev.high if bos else float(prev2.high)
        zone_high = latest.low if fvg else prev.high
        source = 'BOS+FVG' if bos and fvg else 'BOS' if bos else 'FVG'
        return True, source, min(zone_low, zone_high), max(zone_low, zone_high)

    bos = latest.close < latest.open and latest.close < prev.low
    fvg = prev2 is not None and prev2.low > latest.high
    if mode == 'bos':
        fvg = False
    elif mode == 'fvg':
        bos = False
    if not bos and not fvg:
        return False, '', 0.0, 0.0
    zone_high = prev.low if bos else float(prev2.low)
    zone_low = latest.high if fvg else prev.low
    source = 'BOS+FVG' if bos and fvg else 'BOS' if bos else 'FVG'
    return True, source, min(zone_low, zone_high), max(zone_low, zone_high)


def _strong_retest(trigger: Candle, side: str) -> bool:
    open_price = float(trigger.open)
    close_price = float(trigger.close)
    high = float(trigger.high)
    low = float(trigger.low)
    candle_range = high - low
    if candle_range <= 0:
        return False
    body = abs(close_price - open_price)
    body_ratio = body / candle_range
    if side == 'BUY':
        return close_price > open_price and body_ratio >= 0.35 and close_price >= low + (0.65 * candle_range)
    return close_price < open_price and body_ratio >= 0.35 and close_price <= high - (0.65 * candle_range)


def run_mtf_strategy(
    candles: list[Candle],
    symbol: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    ema_period: int = 3,
    setup_mode: str = 'either',
    require_retest_strength: bool = True,
) -> list[StrategySignal]:
    if not candles:
        return []

    candles = ensure_required_indicator_candles(candles)
    grouped = _group_by_day(sorted(candles, key=lambda c: c.timestamp))
    results: list[StrategySignal] = []

    for day in sorted(grouped.keys()):
        day_candles = grouped[day]
        if len(day_candles) < 18:
            continue

        candles_15m = _aggregate_chunks(day_candles, 3)
        candles_1h = _aggregate_chunks(day_candles, 12)
        if len(candles_15m) < 3 or len(candles_1h) < 1:
            continue

        for i in range(15, len(day_candles)):
            completed_1h = [bar for bar in candles_1h if bar.end_idx < i]
            completed_15m = [bar for bar in candles_15m if bar.end_idx < i]
            if not completed_1h or len(completed_15m) < 3:
                continue

            h1_closes = [bar.close for bar in completed_1h]
            h1_ema = _ema(h1_closes, period=max(2, int(ema_period or 3)))
            latest_1h = completed_1h[-1]
            ema_now = h1_ema[-1]
            ema_prev = h1_ema[-2] if len(h1_ema) > 1 else h1_ema[-1]

            bullish_bias = latest_1h.close > latest_1h.open and latest_1h.close >= ema_now and ema_now >= ema_prev
            bearish_bias = latest_1h.close < latest_1h.open and latest_1h.close <= ema_now and ema_now <= ema_prev
            side = 'BUY' if bullish_bias else 'SELL' if bearish_bias else ''
            if not side:
                continue

            confirmed, setup_source, zone_low, zone_high = _setup_confirmation(completed_15m, side, setup_mode=setup_mode)
            if not confirmed:
                continue

            latest_15m = completed_15m[-1]
            if i <= latest_15m.end_idx:
                continue

            trigger = day_candles[i]
            trigger_open = float(trigger.open)
            trigger_high = float(trigger.high)
            trigger_low = float(trigger.low)
            trigger_close = float(trigger.close)

            if require_retest_strength and not _strong_retest(trigger, side):
                continue

            if side == 'BUY':
                retest = trigger_low <= zone_high and trigger_close >= zone_high and trigger_close > trigger_open
                if not retest:
                    continue
                entry = zone_high
                stop = min(trigger_low, zone_low)
                target = entry + (entry - stop) * rr_ratio
            else:
                retest = trigger_high >= zone_low and trigger_close <= zone_low and trigger_close < trigger_open
                if not retest:
                    continue
                entry = zone_low
                stop = max(trigger_high, zone_high)
                target = entry - (stop - entry) * rr_ratio

            qty = _calc_qty(capital, risk_pct, entry, stop)
            if qty <= 0:
                continue

            results.append(
                StrategySignal(
                    strategy_name='MTF 5m',
                    symbol=symbol,
                    side=side,
                    entry_price=round(entry, 4),
                    stop_loss=round(stop, 4),
                    target_price=round(target, 4),
                    signal_time=trigger.timestamp,
                    metadata={
                        'quantity': qty,
                        'bias_tf': '1h',
                        'setup_tf': '15m',
                        'entry_tf': '5m',
                        'setup_source': setup_source,
                        'retest_zone_low': round(zone_low, 4),
                        'retest_zone_high': round(zone_high, 4),
                        'trend_ema': round(ema_now, 4),
                        **build_indicator_snapshot(trigger),
                    },
                )
            )
            break

    return results

