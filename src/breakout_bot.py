from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from dateutil import parser

from src.csv_io import read_csv_rows, write_csv_rows
from src.strategy_common import session_allowed, session_window
from src.telegram_notifier import send_telegram_message
from src.trade_safety import calculate_net_pnl, daily_limit_reached
from src.trading_core import ScoringConfig, ScoreThresholds, StandardTrade, append_log, prepare_trading_data, safe_quantity, weighted_score


@dataclass(slots=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float = 0.0


@dataclass(slots=True)
class BreakoutConfig:
    mode: str = 'Balanced'
    trailing_sl_pct: float = 0.0
    cost_bps: float = 0.0
    fixed_cost_per_trade: float = 0.0
    max_daily_loss: float | None = None
    max_trades_per_day: int | None = 1
    use_first_hour_bias: bool = True
    filter_choppy_days: bool = True
    min_breakout_strength: float = 0.18
    min_volume_ratio: float = 1.15
    duplicate_signal_cooldown_bars: int = 8
    require_vwap_alignment: bool = True
    allow_secondary_entries: bool = False
    morning_session_start: str = '09:25'
    morning_session_end: str = '11:15'
    midday_start: str = '11:16'
    midday_end: str = '13:45'
    allow_afternoon_session: bool = False
    afternoon_session_start: str = '13:46'
    afternoon_session_end: str = '14:45'
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    def __post_init__(self) -> None:
        self.scoring.mode = self.mode
        self.scoring.thresholds = ScoreThresholds(conservative=7.0, balanced=5.5, aggressive=4.2)


parse_timestamp = None


def parse_timestamp_robust(text: str) -> datetime:
    if not text:
        raise ValueError('Empty timestamp')
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    try:
        return parser.parse(text)
    except (ValueError, parser.ParserError):
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M'):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    raise ValueError(f'Unsupported timestamp format: {text}')


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
            append_log(f'breakout_bot.load_candles skipped row: {exc}')
    candles.sort(key=lambda candle: candle.timestamp)
    return candles


def _coerce_candles(df: Any) -> list[Candle]:
    if isinstance(df, list):
        if not df:
            return []
        if isinstance(df[0], Candle):
            candles = [candle for candle in df]
            candles.sort(key=lambda candle: candle.timestamp)
            return candles
        return load_candles(df)
    prepared = prepare_trading_data(df)
    return load_candles(prepared.to_dict(orient='records'))


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


def _true_range(current: Candle, previous: Candle | None) -> float:
    if previous is None:
        return max(0.0, current.high - current.low)
    return max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close))


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
    if hour_range > 0 and directional_move / hour_range >= 0.30 and max(above_vwap, below_vwap) >= 3:
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
    return 1.0 if average <= 0 else day_candles[index].volume / average


def _confirmation_holds(side: str, trigger: float, breakout: Candle, confirmation: Candle) -> bool:
    if side == 'BUY':
        return confirmation.close > trigger and confirmation.high >= breakout.high
    return confirmation.close < trigger and confirmation.low <= breakout.low


def _retest_holds(side: str, trigger: float, confirmation: Candle) -> bool:
    if side == 'BUY':
        return confirmation.low <= trigger and confirmation.close > trigger
    return confirmation.high >= trigger and confirmation.close < trigger


def _secondary_breakout_holds(side: str, trigger: float, breakout: Candle, confirmation: Candle, atr_value: float) -> bool:
    cushion = max(atr_value * 0.1, abs(trigger) * 0.0007, 0.05)
    if side == 'BUY':
        return breakout.close > trigger and confirmation.close >= breakout.close - cushion
    return breakout.close < trigger and confirmation.close <= breakout.close + cushion


def _entry_with_slippage(side: str, trigger: float, atr_value: float) -> float:
    slippage = max(abs(trigger) * 0.0005, atr_value * 0.05, 0.05)
    return trigger + slippage if side == 'BUY' else trigger - slippage


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
    if first_hour[-1].close > first_hour[0].open:
        return 'BUY'
    if first_hour[-1].close < first_hour[0].open:
        return 'SELL'
    return 'NONE'


def _candidate_sides(bias: str, *, use_first_hour_bias: bool) -> list[str]:
    if use_first_hour_bias and bias in {'BUY', 'SELL'}:
        return [bias]
    return ['BUY', 'SELL']


def _session_allowed(candle: Candle, config: BreakoutConfig) -> bool:
    return session_allowed(
        candle.timestamp,
        morning_start=config.morning_session_start,
        morning_end=config.morning_session_end,
        midday_start=config.midday_start,
        midday_end=config.midday_end,
        allow_afternoon_session=bool(config.allow_afternoon_session),
        afternoon_start=config.afternoon_session_start,
        afternoon_end=config.afternoon_session_end,
    )

def _midday_restricted(candle: Candle, config: BreakoutConfig) -> bool:
    return session_window(
        candle.timestamp,
        morning_start=config.morning_session_start,
        morning_end=config.morning_session_end,
        midday_start=config.midday_start,
        midday_end=config.midday_end,
        allow_afternoon_session=bool(config.allow_afternoon_session),
        afternoon_start=config.afternoon_session_start,
        afternoon_end=config.afternoon_session_end,
    ) == 'MIDDAY_BLOCKED'

def _score_candidate(side: str, breakout_candle: Candle, confirmation_candle: Candle, *, regime: str, bias: str, trigger: float, volume_ratio: float, strength: float, vwap_slope: float, atr_value: float, config: BreakoutConfig) -> tuple[float, str, dict[str, float], str, bool] | None:
    broke_level = breakout_candle.close > trigger if side == 'BUY' else breakout_candle.close < trigger
    vwap_ok = breakout_candle.close > breakout_candle.vwap and vwap_slope > 0 if side == 'BUY' else breakout_candle.close < breakout_candle.vwap and vwap_slope < 0
    retest_ok = _confirmation_holds(side, trigger, breakout_candle, confirmation_candle) or _retest_holds(side, trigger, confirmation_candle)
    reaction_ok = confirmation_candle.close > breakout_candle.open if side == 'BUY' else confirmation_candle.close < breakout_candle.open
    bias_support = bias == side or not config.use_first_hour_bias or regime != 'TREND'
    score = weighted_score(
        {
            'trend': regime == 'TREND' and bias_support,
            'vwap': bool(vwap_ok),
            'rsi': strength >= config.min_breakout_strength,
            'adx': volume_ratio >= config.min_volume_ratio,
            'macd': confirmation_candle.close >= breakout_candle.close if side == 'BUY' else confirmation_candle.close <= breakout_candle.close,
            'zone': broke_level,
            'sweep': broke_level and strength >= config.min_breakout_strength,
            'retest': retest_ok,
            'reaction': reaction_ok,
            'breakout_quality': strength >= config.min_breakout_strength * 0.85,
        },
        config.scoring,
    )
    if config.require_vwap_alignment and not vwap_ok:
        return None
    if not broke_level or not score.accepted:
        return None
    if not retest_ok:
        return None
    if strength < float(config.min_breakout_strength) or volume_ratio < float(config.min_volume_ratio):
        return None
    setup_type = 'retest'
    rejection_reason = '' if score.accepted else ','.join(f'missing_{reason}' for reason in score.reasons)
    reason = f'breakout {setup_type} score={score.total:.2f} mode={config.scoring.normalized_mode()} regime={regime}'
    return score.total, reason, score.components, rejection_reason, False


def generate_trades(
    df: Any,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: BreakoutConfig | None = None,
    *,
    trailing_sl_pct: float = 0.0,
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = 1,
    use_first_hour_bias: bool = True,
    filter_choppy_days: bool = True,
) -> list[dict[str, object]]:
    cfg = config or BreakoutConfig()
    if config is None:
        cfg.trailing_sl_pct = float(trailing_sl_pct)
        cfg.cost_bps = float(cost_bps)
        cfg.fixed_cost_per_trade = float(fixed_cost_per_trade)
        cfg.max_daily_loss = max_daily_loss
        cfg.max_trades_per_day = max_trades_per_day
        cfg.use_first_hour_bias = bool(use_first_hour_bias)
        cfg.filter_choppy_days = bool(filter_choppy_days)
        cfg.scoring.mode = cfg.mode

    candles = _coerce_candles(df)
    add_intraday_vwap(candles)
    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        trades_taken = 0
        realized_pnl = 0.0
        last_signal_index: dict[str, int] = {'BUY': -10_000, 'SELL': -10_000}
        day_candles = by_day[day]
        if len(day_candles) < 6:
            continue
        if daily_limit_reached(trades_taken, realized_pnl, max_trades_per_day=cfg.max_trades_per_day, max_daily_loss=cfg.max_daily_loss):
            continue

        opening_range = day_candles[0]
        regime = _classify_market_regime(day_candles)
        if cfg.filter_choppy_days and regime == 'CHOPPY':
            continue

        bias = _first_hour_bias(day_candles)
        if cfg.use_first_hour_bias and bias == 'NONE':
            continue

        for idx in range(4, len(day_candles) - 1):
            if daily_limit_reached(trades_taken, realized_pnl, max_trades_per_day=cfg.max_trades_per_day, max_daily_loss=cfg.max_daily_loss):
                break
            breakout_candle = day_candles[idx]
            confirmation_candle = day_candles[idx + 1]
            if not _session_allowed(breakout_candle, cfg) or _midday_restricted(breakout_candle, cfg):
                continue
            if not _session_allowed(confirmation_candle, cfg) or _midday_restricted(confirmation_candle, cfg):
                continue
            atr_value = _atr(day_candles, idx)
            volume_ratio = _volume_ratio(day_candles, idx)
            strength = _breakout_strength(breakout_candle)
            vwap_slope = breakout_candle.vwap - day_candles[idx - 1].vwap if idx > 0 else 0.0

            for side in _candidate_sides(bias, use_first_hour_bias=cfg.use_first_hour_bias):
                if idx - last_signal_index[side] < int(cfg.duplicate_signal_cooldown_bars):
                    continue
                trigger = opening_range.high if side == 'BUY' else opening_range.low
                score_result = _score_candidate(
                    side,
                    breakout_candle,
                    confirmation_candle,
                    regime=regime,
                    bias=bias,
                    trigger=trigger,
                    volume_ratio=volume_ratio,
                    strength=strength,
                    vwap_slope=vwap_slope,
                    atr_value=atr_value,
                    config=cfg,
                )
                if score_result is None:
                    continue
                score_value, reason, components, rejection_reason, secondary_entry = score_result

                entry = _entry_with_slippage(side, trigger, atr_value)
                structure_stop = min(breakout_candle.low, confirmation_candle.low) if side == 'BUY' else max(breakout_candle.high, confirmation_candle.high)
                stop_result = _bounded_stop(side, entry, structure_stop, atr_value)
                if stop_result is None:
                    continue
                stop, risk_distance = stop_result
                target = entry + (risk_distance * rr_ratio) if side == 'BUY' else entry - (risk_distance * rr_ratio)
                qty = safe_quantity(capital, risk_pct, entry, stop)
                if qty <= 0:
                    continue

                exit_price = day_candles[-1].close
                exit_time = day_candles[-1].timestamp
                exit_reason = 'EOD'
                trail_stop = stop
                for exit_idx in range(idx + 2, len(day_candles)):
                    candle = day_candles[exit_idx]
                    if cfg.trailing_sl_pct > 0:
                        if side == 'BUY':
                            trail_stop = max(trail_stop, candle.high * (1 - cfg.trailing_sl_pct))
                        else:
                            trail_stop = min(trail_stop, candle.low * (1 + cfg.trailing_sl_pct))
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

                gross_pnl, trading_cost, pnl = calculate_net_pnl(side, entry, exit_price, int(qty), cost_bps=cfg.cost_bps, fixed_cost_per_trade=cfg.fixed_cost_per_trade)
                rr_achieved = 0.0 if risk_distance == 0 else abs(exit_price - entry) / risk_distance
                trade = StandardTrade(
                    timestamp=confirmation_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    side=side,
                    entry=entry,
                    stop_loss=stop,
                    target=target,
                    strategy='BREAKOUT',
                    reason=reason,
                    score=score_value,
                    entry_price=entry,
                    target_price=target,
                    risk_per_unit=risk_distance,
                    quantity=int(qty),
                    extra={
                        'setup_type': 'secondary' if secondary_entry else 'retest',
                        'trend_score': round(components.get('trend', 0.0), 2),
                        'indicator_score': round(sum(components.get(key, 0.0) for key in ['vwap', 'rsi', 'adx', 'macd']), 2),
                        'zone_score': round(sum(components.get(key, 0.0) for key in ['zone', 'reaction', 'retest', 'breakout_quality']), 2),
                        'total_score': round(score_value, 2),
                        'rejection_reason': rejection_reason,
                        'day': day.isoformat(),
                        'entry_trigger_price': round(trigger, 4),
                        'fill_model': 'TRIGGER_PLUS_SLIPPAGE',
                        'trailing_stop_loss': round(trail_stop, 4),
                        'market_regime': regime,
                        'breakout_strength': round(strength, 4),
                        'volume_ratio': round(volume_ratio, 4),
                        'first_hour_bias': bias,
                        'bias_mode': 'REQUIRED' if cfg.use_first_hour_bias else 'OBSERVE_ONLY',
                        'bias_aligned': 'YES' if side == bias else 'NO',
                        'regime_filter': 'ON' if cfg.filter_choppy_days else 'OFF',
                        'session_allowed': 'YES',
                        'session_window': 'MORNING',
                        'vwap_aligned': 'YES' if cfg.require_vwap_alignment else 'OPTIONAL',
                        'secondary_entries': 'ON' if cfg.allow_secondary_entries else 'OFF',
                        'exit_time': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'exit_price': round(float(exit_price), 4),
                        'exit_reason': exit_reason,
                        'gross_pnl': round(float(gross_pnl), 2),
                        'trading_cost': round(float(trading_cost), 2),
                        'pnl': round(float(pnl), 2),
                        'rr_achieved': round(float(rr_achieved), 2),
                    },
                ).to_dict()
                trades.append(trade)
                trades_taken += 1
                realized_pnl += float(pnl)
                last_signal_index[side] = idx
                break

    return trades


def build_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return 'Intratrade: no trades generated for this run.'
    closed_trades = [trade for trade in trades if 'pnl' in trade and 'exit_time' in trade and 'exit_reason' in trade]
    if not closed_trades:
        return 'Intratrade alert\nTrades opened: 0\nTrades closed: 0\nWin rate: N/A\nTotal PnL: 0.00\nLast exit: N/A\nLast reason: N/A'
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
    trades = generate_trades(
        rows,
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




