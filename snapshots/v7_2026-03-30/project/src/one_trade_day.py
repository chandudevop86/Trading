from __future__ import annotations

from datetime import time
from math import floor

from src.breakout_bot import Candle
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.trade_safety import calculate_net_pnl, daily_limit_reached


def _group_day_indices(candles: list[Candle]) -> dict:
    by_day: dict = {}
    for idx, candle in enumerate(candles):
        by_day.setdefault(candle.timestamp.date(), []).append(idx)
    return by_day


def _calc_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk = abs(entry - stop)
    if risk <= 0:
        return 0
    return floor((capital * risk_pct) / risk)


def _parse_cutoff(hhmm: str) -> time | None:
    text = (hhmm or '').strip()
    if not text:
        return None
    try:
        h, m = text.split(':', 1)
        return time(hour=int(h), minute=int(m))
    except Exception as exc:
        raise ValueError('entry cutoff must be HH:MM') from exc


def _side_from_row(row: dict[str, object], adx_min: float) -> str:
    signal = str(row.get('market_signal', ''))
    close = float(row.get('close', 0.0) or 0.0)
    vwap = float(row.get('vwap', 0.0) or 0.0)
    macd = float(row.get('macd', 0.0) or 0.0)
    macd_signal = float(row.get('macd_signal', 0.0) or 0.0)
    adx = float(row.get('adx', 0.0) or 0.0)

    if adx < adx_min:
        return ''

    if signal in {'BULLISH_TREND', 'OVERSOLD'} and close > vwap and macd >= macd_signal:
        return 'BUY'
    if signal in {'BEARISH_TREND', 'OVERBOUGHT'} and close < vwap and macd <= macd_signal:
        return 'SELL'
    return ''


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: IndicatorConfig | None = None,
    *,
    entry_cutoff_hhmm: str = '',
    trailing_sl_pct: float = 0.0,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
) -> list[dict[str, object]]:
    cfg = config or IndicatorConfig()
    indicator_rows = generate_indicator_rows(candles, config=cfg)
    by_day = _group_day_indices(candles)
    trades: list[dict[str, object]] = []
    cutoff = _parse_cutoff(entry_cutoff_hhmm)

    for day in sorted(by_day.keys()):
        trades_taken = 0
        realized_pnl = 0.0
        indices = by_day[day]
        if len(indices) < 3:
            continue

        if daily_limit_reached(
            trades_taken,
            realized_pnl,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
        ):
            continue

        entry_idx = -1
        side = ''
        for i in indices:
            if cutoff is not None and candles[i].timestamp.time() > cutoff:
                continue
            s = _side_from_row(indicator_rows[i], cfg.adx_trend_min)
            if s:
                entry_idx = i
                side = s
                break

        if entry_idx < 0:
            continue

        entry_candle = candles[entry_idx]
        entry_row = indicator_rows[entry_idx]
        entry = float(entry_candle.close)

        if side == 'BUY':
            stop = float(entry_candle.low)
            target = entry + (entry - stop) * rr_ratio
        else:
            stop = float(entry_candle.high)
            target = entry - (stop - entry) * rr_ratio

        qty = _calc_qty(capital, risk_pct, entry, stop)
        if qty <= 0:
            continue

        trail_stop = stop
        exit_price = float(candles[indices[-1]].close)
        exit_time = candles[indices[-1]].timestamp
        exit_reason = 'EOD'

        for i in indices:
            if i <= entry_idx:
                continue
            c = candles[i]

            if trailing_sl_pct > 0:
                if side == 'BUY':
                    trail_stop = max(trail_stop, c.high * (1.0 - trailing_sl_pct))
                else:
                    trail_stop = min(trail_stop, c.low * (1.0 + trailing_sl_pct))

            if side == 'BUY':
                if c.low <= trail_stop:
                    exit_price = trail_stop
                    exit_time = c.timestamp
                    exit_reason = 'TRAILING_STOP' if trail_stop > stop else 'STOP_LOSS'
                    break
                if c.high >= target:
                    exit_price = target
                    exit_time = c.timestamp
                    exit_reason = 'TARGET'
                    break
            else:
                if c.high >= trail_stop:
                    exit_price = trail_stop
                    exit_time = c.timestamp
                    exit_reason = 'TRAILING_STOP' if trail_stop < stop else 'STOP_LOSS'
                    break
                if c.low <= target:
                    exit_price = target
                    exit_time = c.timestamp
                    exit_reason = 'TARGET'
                    break

        gross_pnl, trading_cost, pnl = calculate_net_pnl(
            side,
            entry,
            exit_price,
            qty,
            cost_bps=cost_bps,
            fixed_cost_per_trade=fixed_cost_per_trade,
        )
        risk = abs(entry - stop)
        rr = 0.0 if risk == 0 else abs(exit_price - entry) / risk
        if exit_reason in {'STOP_LOSS', 'TRAILING_STOP'} and ((side == 'BUY' and exit_price <= entry) or (side == 'SELL' and exit_price >= entry)):
            rr *= -1.0

        trade = {
            'strategy': 'ONE_TRADE_DAY',
            'day': day.isoformat(),
            'entry_time': entry_candle.timestamp.isoformat(sep=' '),
            'side': side,
            'entry_price': round(entry, 4),
            'stop_loss': round(stop, 4),
            'trailing_stop_loss': round(trail_stop, 4),
            'target_price': round(target, 4),
            'quantity': qty,
            'rsi': entry_row.get('rsi', ''),
            'adx': entry_row.get('adx', ''),
            'macd': entry_row.get('macd', ''),
            'macd_signal': entry_row.get('macd_signal', ''),
            'vwap': entry_row.get('vwap', ''),
            'signal': entry_row.get('market_signal', ''),
            'exit_time': exit_time.isoformat(sep=' '),
            'exit_price': round(exit_price, 4),
            'exit_reason': exit_reason,
            'gross_pnl': round(gross_pnl, 2),
            'trading_cost': round(trading_cost, 2),
            'pnl': round(pnl, 2),
            'rr_achieved': round(rr, 2),
        }
        trades_taken += 1
        realized_pnl += pnl
        trades.append(trade)

    return trades

