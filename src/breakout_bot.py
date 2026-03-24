from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from math import floor
from pathlib import Path

from dateutil import parser

from src.csv_io import read_csv_rows, write_csv_rows
from src.telegram_notifier import send_telegram_message
from src.trade_safety import calculate_net_pnl, daily_limit_reached


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float = 0.0


def parse_timestamp_robust(text: str) -> datetime:
    if not text:
        raise ValueError("Empty timestamp")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return parser.parse(text)
    except (ValueError, parser.ParserError):
        for fmt in (
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%d-%m-%Y %H:%M:%S',
            '%d-%m-%Y %H:%M',
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    raise ValueError(f"Unsupported timestamp format: {text}")


parse_timestamp = parse_timestamp_robust


def load_candles(rows: list[dict[str, str]]) -> list[Candle]:
    candles: list[Candle] = []
    for row in rows:
        try:
            normalized = {str(k).strip().lower(): str(v).strip() for k, v in row.items()}
            ts = normalized.get('datetime') or normalized.get('timestamp') or normalized.get('date') or normalized.get('time')
            if not ts:
                continue
            candles.append(
                Candle(
                    timestamp=parse_timestamp_robust(ts),
                    open=float(normalized.get('open', 0) or 0),
                    high=float(normalized.get('high', 0) or 0),
                    low=float(normalized.get('low', 0) or 0),
                    close=float(normalized.get('close', 0) or 0),
                    volume=float(normalized.get('volume', 0) or 0),
                )
            )
        except Exception as exc:
            print(f"Skipping bad row: {row} | Error: {exc}")
    candles.sort(key=lambda candle: candle.timestamp)
    return candles


def add_intraday_vwap(candles: list[Candle]) -> None:
    current_day = None
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for candle in candles:
        day = candle.timestamp.date()
        if day != current_day:
            current_day = day
            cumulative_pv = 0.0
            cumulative_volume = 0.0
        cumulative_pv += candle.close * candle.volume
        cumulative_volume += candle.volume
        candle.vwap = candle.close if cumulative_volume == 0 else cumulative_pv / cumulative_volume


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    grouped: dict[object, list[Candle]] = {}
    for candle in candles:
        grouped.setdefault(candle.timestamp.date(), []).append(candle)
    return grouped


def _calculate_qty(capital: float, risk_pct: float, entry: float, stop: float) -> int:
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return 0
    risk_amount = capital * risk_pct
    return floor(risk_amount / risk_per_unit)


def _true_range(current: Candle, previous: Candle | None) -> float:
    if previous is None:
        return max(0.0, current.high - current.low)
    return max(
        current.high - current.low,
        abs(current.high - previous.close),
        abs(current.low - previous.close),
    )


def _atr(candles: list[Candle], end_index: int, window: int = 4) -> float:
    start_index = max(0, end_index - window + 1)
    values: list[float] = []
    for index in range(start_index, end_index + 1):
        previous = candles[index - 1] if index > 0 else None
        values.append(_true_range(candles[index], previous))
    return sum(values) / len(values) if values else 0.0


def _classify_market_regime(day_candles: list[Candle]) -> str:
    if len(day_candles) < 4:
        return 'UNKNOWN'
    first_hour = day_candles[:4]
    hour_high = max(c.high for c in first_hour)
    hour_low = min(c.low for c in first_hour)
    hour_range = max(0.0, hour_high - hour_low)
    directional_move = abs(first_hour[-1].close - first_hour[0].open)
    above_vwap = sum(1 for candle in first_hour if candle.close >= candle.vwap)
    below_vwap = sum(1 for candle in first_hour if candle.close <= candle.vwap)
    if hour_range > 0 and directional_move / hour_range >= 0.35 and max(above_vwap, below_vwap) >= 3:
        return 'TREND'
    return 'CHOPPY'


def _breakout_strength(candle: Candle) -> float:
    range_value = max(candle.high - candle.low, 0.0)
    if range_value <= 0:
        return 0.0
    return abs(candle.close - candle.open) / range_value


def _volume_ratio(day_candles: list[Candle], index: int, lookback: int = 3) -> float:
    start = max(0, index - lookback)
    history = [c.volume for c in day_candles[start:index] if c.volume > 0]
    if not history:
        return 1.0
    average = sum(history) / len(history)
    if average <= 0:
        return 1.0
    return day_candles[index].volume / average


def _confirmation_holds(side: str, trigger: float, breakout: Candle, confirmation: Candle) -> bool:
    if side == 'BUY':
        return confirmation.close > trigger and confirmation.high >= breakout.high
    return confirmation.close < trigger and confirmation.low <= breakout.low


def _retest_holds(side: str, trigger: float, confirmation: Candle) -> bool:
    if side == 'BUY':
        return confirmation.low <= trigger and confirmation.close > trigger
    return confirmation.high >= trigger and confirmation.close < trigger


def _entry_with_slippage(side: str, trigger: float, atr_value: float) -> float:
    slippage = max(abs(trigger) * 0.0005, atr_value * 0.05, 0.05)
    if side == 'BUY':
        return trigger + slippage
    return trigger - slippage


def _bounded_stop(side: str, entry: float, structure_stop: float, atr_value: float) -> tuple[float, float] | None:
    raw_risk = abs(entry - structure_stop)
    min_risk = max(abs(entry) * 0.002, atr_value * 0.5, 0.5)
    max_risk = max(abs(entry) * 0.008, atr_value * 1.75, min_risk)
    if raw_risk <= 0 or raw_risk > max_risk:
        return None
    risk_distance = max(raw_risk, min_risk)
    stop = entry - risk_distance if side == 'BUY' else entry + risk_distance
    return stop, risk_distance


def _first_hour_bias(day_candles: list[Candle]) -> str:
    if len(day_candles) < 4:
        return 'NONE'
    first_hour = day_candles[:4]
    hour_open = first_hour[0].open
    hour_close = first_hour[-1].close
    if hour_close > hour_open:
        return 'BUY'
    if hour_close < hour_open:
        return 'SELL'
    return 'NONE'


def _candidate_sides(bias: str, *, use_first_hour_bias: bool) -> list[str]:
    if use_first_hour_bias and bias in {'BUY', 'SELL'}:
        return [bias]
    return ['BUY', 'SELL']


def generate_trades(
    candles: list[Candle],
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
    use_first_hour_bias: bool = True,
    filter_choppy_days: bool = True,
) -> list[dict[str, object]]:
    add_intraday_vwap(candles)
    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        trades_taken = 0
        realized_pnl = 0.0
        day_candles = by_day[day]
        if len(day_candles) < 6:
            continue
        if daily_limit_reached(trades_taken, realized_pnl, max_trades_per_day=max_trades_per_day, max_daily_loss=max_daily_loss):
            continue

        opening_range = day_candles[0]
        regime = _classify_market_regime(day_candles)
        if filter_choppy_days and regime == 'CHOPPY':
            continue

        bias = _first_hour_bias(day_candles)
        if use_first_hour_bias and bias == 'NONE':
            continue

        trade_open: dict[str, object] | None = None
        entry_idx = -1

        for idx in range(4, len(day_candles) - 1):
            breakout_candle = day_candles[idx]
            confirmation_candle = day_candles[idx + 1]
            atr_value = _atr(day_candles, idx)
            volume_ratio = _volume_ratio(day_candles, idx)
            strength = _breakout_strength(breakout_candle)
            vwap_slope = breakout_candle.vwap - day_candles[idx - 1].vwap if idx > 0 else 0.0

            for side in _candidate_sides(bias, use_first_hour_bias=use_first_hour_bias):
                trigger = opening_range.high if side == 'BUY' else opening_range.low
                broke_level = breakout_candle.close > trigger if side == 'BUY' else breakout_candle.close < trigger
                vwap_ok = breakout_candle.close > breakout_candle.vwap and vwap_slope > 0 if side == 'BUY' else breakout_candle.close < breakout_candle.vwap and vwap_slope < 0
                if not broke_level or not vwap_ok:
                    continue
                if strength < 0.2:
                    continue
                if volume_ratio < 1.15:
                    continue
                if not (_confirmation_holds(side, trigger, breakout_candle, confirmation_candle) or _retest_holds(side, trigger, confirmation_candle)):
                    continue

                entry = _entry_with_slippage(side, trigger, atr_value)
                structure_stop = min(breakout_candle.low, confirmation_candle.low) if side == 'BUY' else max(breakout_candle.high, confirmation_candle.high)
                stop_result = _bounded_stop(side, entry, structure_stop, atr_value)
                if stop_result is None:
                    continue
                stop, risk_distance = stop_result
                target = entry + (risk_distance * rr_ratio) if side == 'BUY' else entry - (risk_distance * rr_ratio)
                qty = _calculate_qty(capital, risk_pct, entry, stop)
                if qty <= 0:
                    continue

                trade_open = {
                    'day': day.isoformat(),
                    'entry_time': confirmation_candle.timestamp,
                    'side': side,
                    'entry_price': round(entry, 4),
                    'entry_trigger_price': round(trigger, 4),
                    'fill_model': 'TRIGGER_PLUS_SLIPPAGE',
                    'stop_loss': round(stop, 4),
                    'trailing_stop_loss': round(stop, 4),
                    'target_price': round(target, 4),
                    'quantity': qty,
                    'risk_per_unit': round(risk_distance, 4),
                    'market_regime': regime,
                    'breakout_strength': round(strength, 4),
                    'volume_ratio': round(volume_ratio, 4),
                    'first_hour_bias': bias,
                    'bias_mode': 'REQUIRED' if use_first_hour_bias else 'OBSERVE_ONLY',
                    'bias_aligned': 'YES' if side == bias else 'NO',
                    'regime_filter': 'ON' if filter_choppy_days else 'OFF',
                }
                entry_idx = idx + 1
                break
            if trade_open is not None:
                break

        if trade_open is None:
            continue

        exit_price = day_candles[-1].close
        exit_time = day_candles[-1].timestamp
        exit_reason = 'EOD'
        side = str(trade_open['side'])
        stop = float(trade_open['stop_loss'])
        trail_stop = float(trade_open.get('trailing_stop_loss', stop))
        target = float(trade_open['target_price'])
        entry = float(trade_open['entry_price'])

        for idx in range(entry_idx + 1, len(day_candles)):
            candle = day_candles[idx]
            if trailing_sl_pct > 0:
                if side == 'BUY':
                    trail_stop = max(trail_stop, candle.high * (1 - trailing_sl_pct))
                else:
                    trail_stop = min(trail_stop, candle.low * (1 + trailing_sl_pct))
                trade_open['trailing_stop_loss'] = round(trail_stop, 4)

            if side == 'BUY':
                if candle.low <= trail_stop:
                    exit_price = trail_stop
                    exit_time = candle.timestamp
                    exit_reason = 'STOP_LOSS'
                    break
                if candle.high >= target:
                    exit_price = target
                    exit_time = candle.timestamp
                    exit_reason = 'TARGET'
                    break
            else:
                if candle.high >= trail_stop:
                    exit_price = trail_stop
                    exit_time = candle.timestamp
                    exit_reason = 'STOP_LOSS'
                    break
                if candle.low <= target:
                    exit_price = target
                    exit_time = candle.timestamp
                    exit_reason = 'TARGET'
                    break

        qty = int(trade_open['quantity'])
        gross_pnl, trading_cost, pnl = calculate_net_pnl(
            side,
            entry,
            exit_price,
            qty,
            cost_bps=cost_bps,
            fixed_cost_per_trade=fixed_cost_per_trade,
        )
        risk_per_unit = abs(entry - stop)
        rr_achieved = 0.0 if risk_per_unit == 0 else abs(exit_price - entry) / risk_per_unit
        trade_open.update(
            {
                'exit_time': exit_time,
                'exit_price': round(exit_price, 4),
                'exit_reason': exit_reason,
                'gross_pnl': round(gross_pnl, 2),
                'trading_cost': round(trading_cost, 2),
                'pnl': round(pnl, 2),
                'rr_achieved': round(rr_achieved, 2),
            }
        )
        trades_taken += 1
        realized_pnl += float(trade_open.get('pnl', 0.0) or 0.0)
        trades.append(trade_open)

    return trades


def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return 'Intratrade: no trades generated for this run.'
    closed_trades = [trade for trade in trades if 'pnl' in trade and 'exit_time' in trade and 'exit_reason' in trade]
    if not closed_trades:
        return (
            'Intratrade alert\n'
            f'Trades opened: {len(trades)}\n'
            'Trades closed: 0\n'
            'Win rate: N/A\n'
            'Total PnL: 0.00\n'
            'Last exit: N/A\n'
            'Last reason: N/A'
        )
    total_pnl = sum(float(trade.get('pnl', 0)) for trade in closed_trades)
    wins = sum(1 for trade in closed_trades if float(trade.get('pnl', 0)) > 0)
    win_rate = (wins / len(closed_trades)) * 100.0
    last_trade = closed_trades[-1]
    return (
        'Intratrade alert\n'
        f'Trades opened: {len(trades)}\n'
        f'Trades closed: {len(closed_trades)}\n'
        f'Win rate: {win_rate:.2f}%\n'
        f'Total PnL: {total_pnl:.2f}\n'
        f'Last exit: {last_trade.get("exit_time", "N/A")}\n'
        f'Last reason: {last_trade.get("exit_reason", "N/A")}'
    )


def run(
    input_path: Path,
    output_path: Path,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    trailing_sl_pct: float = 0.0,
    telegram_token: str = '',
    telegram_chat_id: str = '',
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
):
    rows = read_csv_rows(input_path)
    candles = load_candles(rows)
    trades = generate_trades(
        candles,
        capital=capital,
        risk_pct=risk_pct,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    write_csv_rows(output_path, trades)
    if telegram_token and telegram_chat_id:
        send_telegram_message(telegram_token, telegram_chat_id, build_trade_summary(trades))
    return trades


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--capital', type=float, default=100000)
    parser.add_argument('--risk-pct', type=float, default=0.01)
    parser.add_argument('--rr-ratio', type=float, default=2.0)
    parser.add_argument('--trailing-sl-pct', type=float, default=0.0)
    parser.add_argument('--telegram-token', default='')
    parser.add_argument('--telegram-chat-id', default='')
    parser.add_argument('--cost-bps', type=float, default=0.0)
    parser.add_argument('--fixed-cost-per-trade', type=float, default=0.0)
    parser.add_argument('--max-daily-loss', type=float, default=0.0)
    parser.add_argument('--max-trades-per-day', type=int, default=1)
    args = parser.parse_args()
    run(
        args.input,
        args.output,
        args.capital,
        args.risk_pct,
        args.rr_ratio,
        args.trailing_sl_pct,
        args.telegram_token,
        args.telegram_chat_id,
        args.cost_bps,
        args.fixed_cost_per_trade,
        (args.max_daily_loss if args.max_daily_loss > 0 else None),
        args.max_trades_per_day,
    )
