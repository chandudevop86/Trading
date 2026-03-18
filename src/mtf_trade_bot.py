from __future__ import annotations

from dataclasses import dataclass
from math import floor

from src.breakout_bot import Candle
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


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
    ema_period: int = 3,
    setup_mode: str = "either",
    require_retest_strength: bool = True,
    max_trades_per_day: int = 3,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
) -> list[dict[str, object]]:
    trades: list[dict[str, object]] = []
    by_day = _group_by_day(candles)
    ema_period = max(2, int(ema_period or 3))
    max_trades_per_day = max(1, int(max_trades_per_day or 1))

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

        while search_start < len(day_candles) and not daily_limit_reached(trades_taken, realized_pnl, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss):
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

                qty = _calc_qty(capital, risk_pct, entry, stop)
                if qty <= 0:
                    continue

                trade = {
                    "strategy": "MTF_5M",
                    "day": day.isoformat(),
                    "trade_no": trades_taken + 1,
                    "trade_label": f"Trade {trades_taken + 1}",
                    "entry_time": trigger.timestamp.isoformat(sep=" "),
                    "side": side,
                    "entry_price": round(entry, 4),
                    "stop_loss": round(stop, 4),
                    "trailing_stop_loss": round(stop, 4),
                    "target_price": round(target, 4),
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
                    "trend_ema": round(ema_now, 4),
                    "ema_period": ema_period,
                    "setup_mode": (setup_mode or "either").strip().lower(),
                    "require_retest_strength": bool(require_retest_strength),
                    "max_trades_per_day": max_trades_per_day,
                }
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
                if trailing_sl_pct > 0:
                    if side == "BUY":
                        trail_stop = max(trail_stop, float(candle.high) * (1.0 - trailing_sl_pct))
                    else:
                        trail_stop = min(trail_stop, float(candle.low) * (1.0 + trailing_sl_pct))
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
                cost_bps=cost_bps,
                fixed_cost_per_trade=fixed_cost_per_trade,
            )
            trade["exit_time"] = exit_time.isoformat(sep=" ")
            trade["exit_price"] = round(exit_price, 4)
            trade["exit_reason"] = exit_reason
            trade["gross_pnl"] = round(gross_pnl, 2)
            trade["trading_cost"] = round(trading_cost, 2)
            trade["pnl"] = round(pnl, 2)
            trades.append(trade)

            trades_taken += 1
            realized_pnl += float(pnl)
            search_start = exit_idx + 1

    return trades
