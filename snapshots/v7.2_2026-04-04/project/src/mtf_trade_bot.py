from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from math import floor
from typing import Any

from src.breakout_bot import Candle, add_intraday_vwap
from src.trade_safety import calculate_net_pnl, daily_limit_reached


@dataclass
class AggCandle:
    start_idx: int
    end_idx: int
    timestamp: object
    open: float
    high: float
    low: float
    close: float


@dataclass(slots=True)
class MtfTradeConfig:
    trailing_sl_pct: float = 0.0
    ema_period: int = 3
    setup_mode: str = "either"
    require_retest_strength: bool = True
    max_trades_per_day: int = 1
    cost_bps: float = 0.0
    fixed_cost_per_trade: float = 0.0
    max_daily_loss: float | None = None
    min_score_5m: int = 8
    min_score_15m: int = 4
    min_score_1h: int = 2
    min_mtf_score: int = 14

def _group_by_day(candles: list[Candle]) -> dict:
    by_day: dict = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _aggregate_chunks(day_candles: list[Candle], chunk_size: int) -> list[AggCandle]:
    bars: list[AggCandle] = []
    for start in range(0, len(day_candles), chunk_size):
        chunk = day_candles[start : start + chunk_size]
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


def _setup_confirmation(
    completed_15m: list[AggCandle],
    side: str,
    setup_mode: str,
) -> tuple[bool, str, float, float]:
    if len(completed_15m) < 2:
        return False, "", 0.0, 0.0

    latest = completed_15m[-1]
    prev = completed_15m[-2]
    prev2 = completed_15m[-3] if len(completed_15m) >= 3 else None
    mode = (setup_mode or "either").strip().lower()

    if side == "BUY":
        bos = latest.close > latest.open and latest.close > prev.high
        fvg = prev2 is not None and prev2.high < latest.low
        if mode == "bos":
            fvg = False
        elif mode == "fvg":
            bos = False
        if not bos and not fvg:
            return False, "", 0.0, 0.0
        zone_low = prev.high if bos else float(prev2.high)
        zone_high = latest.low if fvg else prev.high
        source = "BOS+FVG" if bos and fvg else "BOS" if bos else "FVG"
        return True, source, min(zone_low, zone_high), max(zone_low, zone_high)

    bos = latest.close < latest.open and latest.close < prev.low
    fvg = prev2 is not None and prev2.low > latest.high
    if mode == "bos":
        fvg = False
    elif mode == "fvg":
        bos = False
    if not bos and not fvg:
        return False, "", 0.0, 0.0
    zone_high = prev.low if bos else float(prev2.low)
    zone_low = latest.high if fvg else prev.low
    source = "BOS+FVG" if bos and fvg else "BOS" if bos else "FVG"
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
    if side == "BUY":
        return close_price > open_price and body_ratio >= 0.35 and close_price >= low + (0.65 * candle_range)
    return close_price < open_price and body_ratio >= 0.35 and close_price <= high - (0.65 * candle_range)


def _parse_hhmm(value: str, fallback: str) -> time:
    raw = str(value or fallback).strip() or fallback
    try:
        hh, mm = raw.split(':', 1)
        return time(hour=max(0, min(23, int(hh))), minute=max(0, min(59, int(mm))))
    except Exception:
        fh, fm = fallback.split(':', 1)
        return time(hour=int(fh), minute=int(fm))


def _session_allowed(candle: Candle, *, start: str = '09:20', end: str = '11:30') -> bool:
    current = candle.timestamp.time().replace(second=0, microsecond=0)
    return _parse_hhmm(start, '09:20') <= current <= _parse_hhmm(end, '11:30')


def _midday_restricted(candle: Candle, *, start: str = '12:00', end: str = '13:30') -> bool:
    current = candle.timestamp.time().replace(second=0, microsecond=0)
    return _parse_hhmm(start, '12:00') <= current <= _parse_hhmm(end, '13:30')


def _setup_strength(latest_15m: AggCandle, zone_low: float, zone_high: float, side: str) -> float:
    candle_range = max(latest_15m.high - latest_15m.low, 0.0001)
    body_ratio = abs(latest_15m.close - latest_15m.open) / candle_range
    displacement = ((latest_15m.close - latest_15m.open) / candle_range) if side == 'BUY' else ((latest_15m.open - latest_15m.close) / candle_range)
    zone_width = max(zone_high - zone_low, 0.0)
    width_ratio = zone_width / candle_range
    score = 0.0
    if body_ratio >= 0.45:
        score += 1.0
    if displacement >= 0.45:
        score += 1.0
    if width_ratio <= 1.1:
        score += 1.0
    return score


def _bar_range(candle: Candle | AggCandle) -> float:
    return max(float(candle.high) - float(candle.low), 0.0001)


def _avg_volume(day_candles: list[Candle], idx: int, lookback: int = 5) -> float:
    left = max(0, idx - max(1, int(lookback)))
    sample = [max(float(getattr(candle, 'volume', 0.0) or 0.0), 0.0) for candle in day_candles[left:idx]]
    if not sample:
        return max(float(getattr(day_candles[idx], 'volume', 0.0) or 0.0), 0.0)
    return sum(sample) / len(sample)


def _zone_alignment_15m(side: str) -> str:
    return 'DEMAND' if side == 'BUY' else 'SUPPLY'


def _score_5m(day_candles: list[Candle], trigger_idx: int, *, zone_low: float, zone_high: float, side: str, rr_ratio: float, retest_ok: bool) -> tuple[int, dict[str, object]]:
    trigger = day_candles[trigger_idx]
    zone_width = max(zone_high - zone_low, 0.0001)
    entry_reference = zone_high if side == 'BUY' else zone_low
    entry_to_zone_distance = abs(float(trigger.close) - entry_reference)
    zone_quality = 3 if entry_to_zone_distance <= zone_width * 0.25 else 2 if entry_to_zone_distance <= zone_width * 0.75 else 1 if entry_to_zone_distance <= zone_width * 1.25 else 0
    retest_score = 2 if retest_ok else 0
    candle_range = _bar_range(trigger)
    body = abs(float(trigger.close) - float(trigger.open))
    impulse_ratio = body / candle_range
    impulse_score = 2 if impulse_ratio >= 0.6 else 1 if impulse_ratio >= 0.35 else 0
    breakout_volume = max(float(getattr(trigger, 'volume', 0.0) or 0.0), 0.0)
    avg_volume = _avg_volume(day_candles, trigger_idx, lookback=5)
    volume_spike = avg_volume > 0 and breakout_volume > avg_volume * 1.5
    volume_score = 1 if volume_spike else 0
    vwap_ok = (float(trigger.close) >= float(trigger.vwap)) if side == 'BUY' else (float(trigger.close) <= float(trigger.vwap))
    vwap_score = 1 if vwap_ok else 0
    rr_ok = float(rr_ratio) >= 1.8
    rr_score = 1 if rr_ok else 0
    score = zone_quality + retest_score + impulse_score + volume_score + vwap_score + rr_score
    return score, {
        'zone_score_5m': zone_quality,
        'retest_ok_5m': bool(retest_ok),
        'impulse_score_5m': impulse_score,
        'volume_spike_5m': bool(volume_spike),
        'volume_score_5m': volume_score,
        'breakout_volume_5m': round(breakout_volume, 2),
        'avg_volume_5m': round(avg_volume, 2),
        'vwap_alignment_5m': bool(vwap_ok),
        'vwap_ok_5m': bool(vwap_ok),
        'entry_to_zone_distance': round(entry_to_zone_distance, 4),
        'rr_planned': round(float(rr_ratio), 2),
        'sl_points': round(zone_width, 4),
    }


def _score_15m(latest_15m: AggCandle, prev_15m: AggCandle, *, side: str, setup_source: str, zone_low: float, zone_high: float) -> tuple[int, dict[str, object]]:
    trend_ok = (float(latest_15m.close) > float(latest_15m.open)) if side == 'BUY' else (float(latest_15m.close) < float(latest_15m.open))
    trend_score = 2 if trend_ok else 0
    zone_ok = bool(setup_source)
    zone_score = 2 if zone_ok else 0
    structure_break = (float(latest_15m.close) > float(prev_15m.high)) if side == 'BUY' else (float(latest_15m.close) < float(prev_15m.low))
    structure_score = 2 if structure_break else 0
    candle_range = _bar_range(latest_15m)
    impulse_strength = abs(float(latest_15m.close) - float(latest_15m.open)) / candle_range
    impulse_score = 2 if impulse_strength >= 0.55 else 1 if impulse_strength >= 0.35 else 0
    vwap_bias = 'BULLISH' if side == 'BUY' and float(latest_15m.close) >= max(zone_high, zone_low) else 'BEARISH' if side == 'SELL' and float(latest_15m.close) <= min(zone_high, zone_low) else 'NEUTRAL'
    score = trend_score + zone_score + structure_score
    return score, {
        'trend_15m': 'UP' if side == 'BUY' and trend_ok else 'DOWN' if side == 'SELL' and trend_ok else 'MIXED',
        'trend_ok_15m': bool(trend_ok),
        'zone_alignment_15m': _zone_alignment_15m(side),
        'zone_ok_15m': bool(zone_ok),
        'vwap_bias_15m': vwap_bias,
        'impulse_score_15m': int(impulse_score),
        'structure_break_15m': bool(structure_break),
    }


def _nearest_levels_1h(completed_1h: list[AggCandle]) -> tuple[float | None, float | None]:
    if not completed_1h:
        return None, None
    nearest_supply = max(float(bar.high) for bar in completed_1h[-3:])
    nearest_demand = min(float(bar.low) for bar in completed_1h[-3:])
    return nearest_supply, nearest_demand


def _score_1h(latest_1h: AggCandle, *, ema_now: float, ema_prev: float, side: str, entry: float, zone_width: float, completed_1h: list[AggCandle]) -> tuple[int, dict[str, object]]:
    bullish_bias = latest_1h.close > latest_1h.open and latest_1h.close >= ema_now and ema_now >= ema_prev
    bearish_bias = latest_1h.close < latest_1h.open and latest_1h.close <= ema_now and ema_now <= ema_prev
    bias_ok = bullish_bias if side == 'BUY' else bearish_bias
    trend_score = 2 if bias_ok else 0
    nearest_supply, nearest_demand = _nearest_levels_1h(completed_1h)
    distance_to_supply = round(max((nearest_supply - entry), 0.0), 4) if nearest_supply is not None else None
    distance_to_demand = round(max((entry - nearest_demand), 0.0), 4) if nearest_demand is not None else None
    conflict = False
    if side == 'BUY' and distance_to_supply is not None:
        conflict = distance_to_supply <= max(zone_width * 2.0, 5.0)
    if side == 'SELL' and distance_to_demand is not None:
        conflict = distance_to_demand <= max(zone_width * 2.0, 5.0)
    zone_score = 0 if conflict else 2
    score = trend_score + zone_score
    return score, {
        'trend_1h': 'UP' if bullish_bias else 'DOWN' if bearish_bias else 'MIXED',
        'bias_ok_1h': bool(bias_ok),
        'nearest_supply_1h': round(float(nearest_supply), 4) if nearest_supply is not None else None,
        'nearest_demand_1h': round(float(nearest_demand), 4) if nearest_demand is not None else None,
        'distance_to_supply_1h': distance_to_supply,
        'distance_to_demand_1h': distance_to_demand,
        'distance_to_htf_zone': distance_to_supply if side == 'BUY' else distance_to_demand,
        'htf_conflict': bool(conflict),
    }


def _mtf_bucket(score: int) -> str:
    if score >= 16:
        return '16-20'
    if score >= 12:
        return '12-15'
    if score >= 9:
        return '9-11'
    return '0-8'


def _trade_result(exit_reason: str, pnl: float) -> str:
    if str(exit_reason).upper() == 'TARGET' or pnl > 0:
        return 'WIN'
    if pnl < 0:
        return 'LOSS'
    return 'FLAT'


def _mae_mfe(day_candles: list[Candle], start_idx: int, end_idx: int, side: str, entry: float) -> tuple[float, float]:
    sample = day_candles[start_idx:end_idx + 1]
    if not sample:
        return 0.0, 0.0
    if side == 'BUY':
        mae = min(float(c.low) - entry for c in sample)
        mfe = max(float(c.high) - entry for c in sample)
    else:
        mae = min(entry - float(c.high) for c in sample)
        mfe = max(entry - float(c.low) for c in sample)
    return round(mae, 2), round(mfe, 2)


def _minutes_between(start: Any, end: Any) -> int | None:
    try:
        return max(int((end - start).total_seconds() // 60), 0)
    except Exception:
        return None


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: MtfTradeConfig | None = None,
    *,
    trailing_sl_pct: float = 0.0,
    ema_period: int = 3,
    setup_mode: str = "either",
    require_retest_strength: bool = True,
    max_trades_per_day: int = 1,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
) -> list[dict[str, object]]:
    cfg = config or MtfTradeConfig(
        trailing_sl_pct=float(trailing_sl_pct),
        ema_period=int(ema_period),
        setup_mode=str(setup_mode),
        require_retest_strength=bool(require_retest_strength),
        max_trades_per_day=int(max_trades_per_day),
        cost_bps=float(cost_bps),
        fixed_cost_per_trade=float(fixed_cost_per_trade),
        max_daily_loss=max_daily_loss,
    )
    trades: list[dict[str, object]] = []
    add_intraday_vwap(candles)
    by_day = _group_by_day(candles)
    ema_period = max(2, int(cfg.ema_period or 3))
    max_trades_per_day = max(1, int(cfg.max_trades_per_day or 1))

    for day in sorted(by_day.keys()):
        day_candles = by_day[day]
        if len(day_candles) < 18:
            continue

        candles_15m = _aggregate_chunks(day_candles, 3)
        candles_1h = _aggregate_chunks(day_candles, 12)
        if len(candles_15m) < 3 or len(candles_1h) < 1:
            continue

        search_start = 15
        trades_taken = 0
        realized_pnl = 0.0

        while search_start < len(day_candles) and not daily_limit_reached(
            trades_taken,
            realized_pnl,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=cfg.max_daily_loss,
        ):
            trade = None
            entry_idx = -1

            for i in range(max(15, search_start), len(day_candles)):
                completed_1h = [bar for bar in candles_1h if bar.end_idx < i]
                completed_15m = [bar for bar in candles_15m if bar.end_idx < i]
                if not completed_1h or len(completed_15m) < 3:
                    continue

                h1_closes = [bar.close for bar in completed_1h]
                h1_ema = _ema(h1_closes, period=ema_period)
                latest_1h = completed_1h[-1]
                ema_now = h1_ema[-1]
                ema_prev = h1_ema[-2] if len(h1_ema) > 1 else h1_ema[-1]

                bullish_bias = latest_1h.close > latest_1h.open and latest_1h.close >= ema_now and ema_now >= ema_prev
                bearish_bias = latest_1h.close < latest_1h.open and latest_1h.close <= ema_now and ema_now <= ema_prev

                side = "BUY" if bullish_bias else "SELL" if bearish_bias else ""
                if not side:
                    continue

                confirmed, setup_source, zone_low, zone_high = _setup_confirmation(completed_15m, side, setup_mode=cfg.setup_mode)
                if not confirmed:
                    continue

                latest_15m = completed_15m[-1]
                prev_15m = completed_15m[-2]
                if i <= latest_15m.end_idx:
                    continue

                trigger = day_candles[i]
                if not _session_allowed(trigger) or _midday_restricted(trigger):
                    continue
                trigger_open = float(trigger.open)
                trigger_high = float(trigger.high)
                trigger_low = float(trigger.low)
                trigger_close = float(trigger.close)

                if cfg.require_retest_strength and not _strong_retest(trigger, side):
                    continue
                if side == 'BUY' and trigger_close < float(trigger.vwap):
                    continue
                if side == 'SELL' and trigger_close > float(trigger.vwap):
                    continue
                setup_strength = _setup_strength(latest_15m, zone_low, zone_high, side)
                if setup_strength < 2.5:
                    continue

                if side == "BUY":
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

                zone_width = max(zone_high - zone_low, 0.0001)
                score_5m, metrics_5m = _score_5m(day_candles, i, zone_low=zone_low, zone_high=zone_high, side=side, rr_ratio=rr_ratio, retest_ok=retest)
                score_15m, metrics_15m = _score_15m(latest_15m, prev_15m, side=side, setup_source=setup_source, zone_low=zone_low, zone_high=zone_high)
                score_1h, metrics_1h = _score_1h(latest_1h, ema_now=ema_now, ema_prev=ema_prev, side=side, entry=entry, zone_width=zone_width, completed_1h=completed_1h)
                mtf_score = int(score_5m + score_15m + score_1h)
                htf_conflict = bool(metrics_1h.get('htf_conflict', False))
                if score_5m < cfg.min_score_5m or score_15m < cfg.min_score_15m or score_1h < cfg.min_score_1h or mtf_score < cfg.min_mtf_score or htf_conflict:
                    continue

                qty = _calc_qty(capital, risk_pct, entry, stop)
                if qty <= 0:
                    continue

                trade = {
                    "strategy": "MTF_5M",
                    "day": day.isoformat(),
                    "trade_no": trades_taken + 1,
                    "trade_label": f"Trade {trades_taken + 1}",
                    "entry_time": trigger.timestamp.isoformat(sep=" "),
                    "timestamp": trigger.timestamp.isoformat(sep=" "),
                    "side": side,
                    "pattern": setup_source,
                    "entry_price": round(entry, 4),
                    "entry": round(entry, 4),
                    "stop_loss": round(stop, 4),
                    "trailing_stop_loss": round(stop, 4),
                    "target_price": round(target, 4),
                    "target": round(target, 4),
                    "target_1": round(entry + abs(entry - stop), 4) if side == "BUY" else round(entry - abs(entry - stop), 4),
                    "target_2": round(entry + (2.0 * abs(entry - stop)), 4) if side == "BUY" else round(entry - (2.0 * abs(entry - stop)), 4),
                    "target_3": round(entry + (3.0 * abs(entry - stop)), 4) if side == "BUY" else round(entry - (3.0 * abs(entry - stop)), 4),
                    "quantity": qty,
                    "bias_tf": "1h",
                    "setup_tf": "15m",
                    "entry_tf": "5m",
                    "bias_candle_time": latest_1h.timestamp.isoformat(sep=" "),
                    "setup_candle_time": latest_15m.timestamp.isoformat(sep=" "),
                    "setup_source": setup_source,
                    "retest_zone_low": round(zone_low, 4),
                    "retest_zone_high": round(zone_high, 4),
                    "zone_low": round(zone_low, 4),
                    "zone_high": round(zone_high, 4),
                    "trend_ema": round(ema_now, 4),
                    "ema_period": ema_period,
                    "setup_mode": (cfg.setup_mode or "either").strip().lower(),
                    "require_retest_strength": bool(cfg.require_retest_strength),
                    "max_trades_per_day": max_trades_per_day,
                    "vwap_aligned": 'YES',
                    "session_allowed": 'YES',
                    "session_window": 'MORNING',
                    "setup_strength_score": round(setup_strength, 2),
                    "score_5m": int(score_5m),
                    "score_15m": int(score_15m),
                    "score_1h": int(score_1h),
                    "mtf_score": int(mtf_score),
                    "mtf_score_bucket": _mtf_bucket(mtf_score),
                    "allow_trade": True,
                }
                trade.update(metrics_5m)
                trade.update(metrics_15m)
                trade.update(metrics_1h)
                trade['sl_points'] = round(abs(entry - stop), 4)
                entry_idx = i
                break
                entry_idx = i
                break

            if trade is None or entry_idx < 0:
                break

            side = str(trade["side"])
            entry = float(trade["entry_price"])
            stop = float(trade["stop_loss"])
            trail_stop = float(trade["trailing_stop_loss"])
            target = float(trade["target_price"])
            qty = int(trade["quantity"])
            exit_price = float(day_candles[-1].close)
            exit_time = day_candles[-1].timestamp
            exit_reason = "EOD"
            exit_idx = len(day_candles) - 1

            for idx in range(entry_idx + 1, len(day_candles)):
                candle = day_candles[idx]
                if cfg.trailing_sl_pct > 0:
                    if side == "BUY":
                        trail_stop = max(trail_stop, float(candle.high) * (1.0 - cfg.trailing_sl_pct))
                    else:
                        trail_stop = min(trail_stop, float(candle.low) * (1.0 + cfg.trailing_sl_pct))
                    trade["trailing_stop_loss"] = round(trail_stop, 4)

                if side == "BUY":
                    if float(candle.low) <= trail_stop:
                        exit_price = trail_stop
                        exit_time = candle.timestamp
                        exit_reason = "TRAILING_STOP" if trail_stop > stop else "STOP_LOSS"
                        exit_idx = idx
                        break
                    if float(candle.high) >= target:
                        exit_price = target
                        exit_time = candle.timestamp
                        exit_reason = "TARGET"
                        exit_idx = idx
                        break
                else:
                    if float(candle.high) >= trail_stop:
                        exit_price = trail_stop
                        exit_time = candle.timestamp
                        exit_reason = "TRAILING_STOP" if trail_stop < stop else "STOP_LOSS"
                        exit_idx = idx
                        break
                    if float(candle.low) <= target:
                        exit_price = target
                        exit_time = candle.timestamp
                        exit_reason = "TARGET"
                        exit_idx = idx
                        break

            gross_pnl, trading_cost, pnl = calculate_net_pnl(
                side,
                entry,
                exit_price,
                qty,
                cost_bps=cfg.cost_bps,
                fixed_cost_per_trade=cfg.fixed_cost_per_trade,
            )
            trade["exit_time"] = exit_time.isoformat(sep=" ")
            trade["exit_price"] = round(exit_price, 4)
            trade["exit_reason"] = exit_reason
            trade["gross_pnl"] = round(gross_pnl, 2)
            trade["trading_cost"] = round(trading_cost, 2)
            trade["pnl"] = round(pnl, 2)
            trade["result"] = _trade_result(exit_reason, float(pnl))
            mae, mfe = _mae_mfe(day_candles, entry_idx, exit_idx, side, entry)
            trade["mae"] = mae
            trade["mfe"] = mfe
            trade["time_to_target_min"] = _minutes_between(trigger.timestamp, exit_time) if exit_reason == 'TARGET' else None
            trade["time_to_stop_min"] = _minutes_between(trigger.timestamp, exit_time) if exit_reason in {'STOP_LOSS', 'TRAILING_STOP'} else None
            trades.append(trade)

            trades_taken += 1
            realized_pnl += float(pnl)
            search_start = exit_idx + 1

    return trades


