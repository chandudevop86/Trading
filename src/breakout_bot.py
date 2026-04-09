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
from src.volatility_filter import evaluate_volatility_snapshot
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
    atr_pct: float = 0.0
    opening_volatility_pct: float = 0.0
    vwap_deviation_pct: float = 0.0
    expansion_ratio: float = 0.0
    volatility_score: float = 0.0
    volatility_decision: str = ''
    market_state: str = ''


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
    min_breakout_strength: float = 0.22
    min_volume_ratio: float = 1.30
    duplicate_signal_cooldown_bars: int = 12
    require_vwap_alignment: bool = True
    require_market_structure: bool = True
    structure_swing_window: int = 2
    allow_secondary_entries: bool = False
    morning_session_start: str = '09:35'
    morning_session_end: str = '10:45'
    midday_start: str = '11:16'
    midday_end: str = '13:45'
    allow_afternoon_session: bool = False
    afternoon_session_start: str = '13:46'
    afternoon_session_end: str = '14:45'
    min_breakout_take_score: float = 12.0
    use_volatility_filter: bool = True
    min_volatility_score: int = 6
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    def __post_init__(self) -> None:
        self.scoring.mode = self.mode
        self.scoring.thresholds = ScoreThresholds(conservative=7.6, balanced=6.2, aggressive=5.0)


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
                    vwap=float(normalized.get('vwap', 0) or 0),
                    atr_pct=float(normalized.get('atr_pct', 0) or 0),
                    opening_volatility_pct=float(normalized.get('opening_volatility_pct', 0) or 0),
                    vwap_deviation_pct=float(normalized.get('vwap_deviation_pct', 0) or 0),
                    expansion_ratio=float(normalized.get('expansion_ratio', 0) or 0),
                    volatility_score=float(normalized.get('volatility_score', 0) or 0),
                    volatility_decision=str(normalized.get('volatility_decision', '') or ''),
                    market_state=str(normalized.get('market_state', '') or ''),
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
        return breakout.close > trigger and confirmation.close > trigger and confirmation.low >= trigger - cushion
    return breakout.close < trigger and confirmation.close < trigger and confirmation.high <= trigger + cushion


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


<<<<<<< HEAD
def _recent_swings(day_candles: list[Candle], idx: int, swing_window: int) -> tuple[list[float], list[float]]:
    highs: list[float] = []
    lows: list[float] = []
    window = max(1, int(swing_window or 1))
    upper_bound = min(len(day_candles) - window - 1, idx)
    for center in range(window, upper_bound + 1):
        candle = day_candles[center]
        left = day_candles[center - window:center]
        right = day_candles[center + 1:center + window + 1]
        if len(right) < window:
            continue
        if all(float(candle.high) >= float(item.high) for item in left + right):
            highs.append(float(candle.high))
        if all(float(candle.low) <= float(item.low) for item in left + right):
            lows.append(float(candle.low))
    return highs[-2:], lows[-2:]


def _market_structure_ok(day_candles: list[Candle], idx: int, side: str, config: BreakoutConfig) -> tuple[bool, str]:
    highs, lows = _recent_swings(day_candles, idx, config.structure_swing_window)
    if len(highs) < 2 or len(lows) < 2:
        return False, 'INSUFFICIENT'
    if side == 'BUY':
        if highs[-1] > highs[-2] and lows[-1] > lows[-2]:
            return True, 'HH_HL'
        return False, 'STRUCTURE_WEAK'
    if highs[-1] < highs[-2] and lows[-1] < lows[-2]:
        return True, 'LH_LL'
    return False, 'STRUCTURE_WEAK'


=======
>>>>>>> fed8576 ( modifyed with ltp verson2)
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
<<<<<<< HEAD

def _avg_candle_range(day_candles: list[Candle], idx: int, lookback: int = 5) -> float:
    start = max(0, idx - max(1, int(lookback)) + 1)
    sample = [max(float(c.high) - float(c.low), 0.0) for c in day_candles[start:idx + 1]]
    return (sum(sample) / len(sample)) if sample else 0.0

=======
>>>>>>> fed8576 ( modifyed with ltp verson2)

def _body_ratio(candle: Candle) -> float:
    range_value = max(float(candle.high) - float(candle.low), 0.0)
    if range_value <= 0:
        return 0.0
    return abs(float(candle.close) - float(candle.open)) / range_value


def _consolidation_candles(day_candles: list[Candle], idx: int, trigger: float, atr_value: float, lookback: int = 6) -> int:
    count = 0
    band = max(float(atr_value) * 1.2, abs(float(trigger)) * 0.0015, 2.0)
    start = max(0, idx - max(1, int(lookback)))
    for candle in reversed(day_candles[start:idx]):
        if abs(float(candle.close) - float(trigger)) <= band:
            count += 1
        else:
            break
    return count


def _opposing_level_distance(day_candles: list[Candle], idx: int, side: str, entry: float) -> float | None:
    history = day_candles[max(0, idx - 20):idx]
    if side == 'BUY':
        candidates = [float(c.high) - float(entry) for c in history if float(c.high) > float(entry)]
    else:
        candidates = [float(entry) - float(c.low) for c in history if float(c.low) < float(entry)]
    if not candidates:
        return None
    return min(candidates)


def _breakout_score_bucket(score: float) -> str:
    if score >= 15:
        return '15+'
    if score >= 12:
        return '12-14'
    if score >= 9:
        return '9-11'
    return '0-8'


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


def _minutes_between(start: datetime, end: datetime) -> int | None:
    try:
        return max(int((end - start).total_seconds() // 60), 0)
    except Exception:
        return None


def _volatility_snapshot(day_candles: list[Candle], idx: int, breakout_candle: Candle, opening_range: Candle, atr_value: float) -> dict[str, object]:
    atr_pct = breakout_candle.atr_pct if breakout_candle.atr_pct > 0 else ((atr_value / breakout_candle.close) * 100.0 if breakout_candle.close > 0 else 0.0)
    opening_volatility_pct = breakout_candle.opening_volatility_pct
    if opening_volatility_pct <= 0:
        opening_width = max(float(opening_range.high) - float(opening_range.low), 0.0)
        opening_reference = max(abs(float(opening_range.open)), 0.0001)
        opening_volatility_pct = (opening_width / opening_reference) * 100.0
    vwap_deviation_pct = breakout_candle.vwap_deviation_pct
    if vwap_deviation_pct <= 0 and breakout_candle.vwap > 0:
        vwap_deviation_pct = abs(float(breakout_candle.close) - float(breakout_candle.vwap)) / float(breakout_candle.vwap) * 100.0
    expansion_ratio = breakout_candle.expansion_ratio
    if expansion_ratio <= 0:
        avg_range_10 = max(_avg_candle_range(day_candles, idx, lookback=10), 0.0001)
        expansion_ratio = max(float(breakout_candle.high) - float(breakout_candle.low), 0.0) / avg_range_10
    return evaluate_volatility_snapshot(
        {
            'atr_pct': atr_pct,
            'opening_volatility_pct': opening_volatility_pct,
            'vwap_deviation_pct': vwap_deviation_pct,
            'expansion_ratio': expansion_ratio,
        }
    )


def _score_candidate(side: str, breakout_candle: Candle, confirmation_candle: Candle, *, day_candles: list[Candle], idx: int, regime: str, bias: str, trigger: float, volume_ratio: float, vwap_slope: float, atr_value: float, structure_ok: bool, structure_label: str, config: BreakoutConfig) -> tuple[float, str, dict[str, float], str, bool] | None:
    previous_candle = day_candles[idx - 1] if idx > 0 else breakout_candle
    broke_level = breakout_candle.close > trigger if side == 'BUY' else breakout_candle.close < trigger
    clean_break = breakout_candle.close > previous_candle.high if side == 'BUY' else breakout_candle.close < previous_candle.low
    vwap_ok = breakout_candle.close > breakout_candle.vwap and confirmation_candle.close > confirmation_candle.vwap and vwap_slope > 0 if side == 'BUY' else breakout_candle.close < breakout_candle.vwap and confirmation_candle.close < confirmation_candle.vwap and vwap_slope < 0
    retest_happened = _retest_holds(side, trigger, confirmation_candle)
    continuation_ok = _secondary_breakout_holds(side, trigger, breakout_candle, confirmation_candle, atr_value)
    held_zone = _confirmation_holds(side, trigger, breakout_candle, confirmation_candle) or retest_happened
    body_ratio = _body_ratio(breakout_candle)
    avg_range = max(_avg_candle_range(day_candles, idx, lookback=5), 0.0001)
    breakout_strength = abs(float(breakout_candle.close) - float(trigger))
    strength_ratio = breakout_strength / avg_range
    consolidation_bars = _consolidation_candles(day_candles, idx, trigger, atr_value, lookback=6)
    entry_reference = max(float(trigger), float(breakout_candle.close)) if side == 'BUY' else min(float(trigger), float(breakout_candle.close))
    next_level_distance = _opposing_level_distance(day_candles, idx, side, entry_reference)
    min_rr_distance = max(float(atr_value) * 1.5, abs(float(trigger)) * 0.002, 3.0)
    bias_support = bias == side or not config.use_first_hour_bias or regime != 'TREND'

    strength_score = 3 if strength_ratio > 2.0 else 2 if strength_ratio > 1.2 else 0
    body_score = 2 if body_ratio > 0.7 else 1 if body_ratio > 0.5 else 0
    volume_score = 3 if volume_ratio > 1.8 else 2 if volume_ratio > 1.3 else 0
    structure_score = 2 if clean_break else 0
    retest_score = 3 if retest_happened and held_zone else 1 if (not retest_happened and continuation_ok) else 0
    time_score = 2 if consolidation_bars >= 5 else 1
    distance_score = 2 if (next_level_distance is None or next_level_distance > min_rr_distance) else 0
    trend_score = 2 if (vwap_ok and bias_support) else 0

    breakout_score = strength_score + body_score + volume_score + structure_score + retest_score + time_score + distance_score + trend_score
    if config.require_vwap_alignment and not vwap_ok:
        return None
    if config.require_market_structure and not structure_ok and structure_label != 'INSUFFICIENT':
        return None
    if not broke_level or not clean_break:
        return None
    if retest_score <= 0:
        return None
    if breakout_score < float(config.min_breakout_take_score):
        return None
    if abs(float(breakout_candle.close) - float(breakout_candle.vwap)) < max(atr_value * 0.12, abs(trigger) * 0.0008, 0.08):
        return None

    setup_type = 'retest' if retest_happened else 'continuation'
    reason = f'breakout {setup_type} score={breakout_score:.2f} strength={strength_ratio:.2f} structure={structure_label.lower()}'
    components = {
        'strength_score': float(strength_score),
        'body_score': float(body_score),
        'volume_score': float(volume_score),
        'structure_score': float(structure_score),
        'retest_score': float(retest_score),
        'time_score': float(time_score),
        'distance_score': float(distance_score),
        'trend_score': float(trend_score),
        'strength_ratio': round(float(strength_ratio), 4),
        'body_ratio': round(float(body_ratio), 4),
        'breakout_strength': round(float(breakout_strength), 4),
        'avg_candle_range': round(float(avg_range), 4),
        'volume_ratio': round(float(volume_ratio), 4),
        'clean_break': 1.0 if clean_break else 0.0,
        'retest_happened': 1.0 if retest_happened else 0.0,
        'continuation_ok': 1.0 if continuation_ok else 0.0,
        'consolidation_candles': float(consolidation_bars),
        'next_level_distance': round(float(next_level_distance), 4) if next_level_distance is not None else -1.0,
    }
    return breakout_score, reason, components, '', setup_type == 'continuation'

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
                structure_ok, structure_label = _market_structure_ok(day_candles, idx, side, cfg)
                trigger = opening_range.high if side == 'BUY' else opening_range.low
                score_result = _score_candidate(
                    side,
                    breakout_candle,
                    confirmation_candle,
                    day_candles=day_candles,
                    idx=idx,
                    regime=regime,
                    bias=bias,
                    trigger=trigger,
                    volume_ratio=volume_ratio,
                    vwap_slope=vwap_slope,
                    atr_value=atr_value,
                    structure_ok=structure_ok,
                    structure_label=structure_label,
                    config=cfg,
                )
                if score_result is None:
                    continue
                score_value, reason, components, rejection_reason, secondary_entry = score_result
                volatility_snapshot = _volatility_snapshot(day_candles, idx, breakout_candle, opening_range, atr_value)
                if cfg.use_volatility_filter and (
                    not bool(volatility_snapshot.get('trade_allowed'))
                    or int(volatility_snapshot.get('volatility_score', 0)) < int(cfg.min_volatility_score)
                ):
                    continue

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
                mae, mfe = _mae_mfe(day_candles, idx, exit_idx if 'exit_idx' in locals() else idx + 1, side, entry)
                time_to_target_min = _minutes_between(breakout_candle.timestamp, exit_time) if exit_reason == 'TARGET' else None
                time_to_stop_min = _minutes_between(breakout_candle.timestamp, exit_time) if exit_reason == 'STOP_LOSS' else None
                fake_breakout = 'YES' if exit_reason == 'STOP_LOSS' and time_to_stop_min is not None and time_to_stop_min <= 15 else 'NO'
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
                        'breakout_score_bucket': _breakout_score_bucket(score_value),
                        'retest_only_entry': 'YES',
                        'entry_policy': 'RETEST_ONLY',
                        'trend_score': round(components.get('trend_score', 0.0), 2),
                        'indicator_score': round(sum(components.get(key, 0.0) for key in ['volume_score', 'trend_score']), 2),
                        'zone_score': round(sum(components.get(key, 0.0) for key in ['structure_score', 'retest_score', 'distance_score']), 2),
                        'zone_gate_score': round(sum(components.get(key, 0.0) for key in ['structure_score', 'retest_score', 'distance_score']), 2),
                        'zone_gate_threshold': round(float(cfg.min_breakout_take_score), 2),
                        'total_score': round(score_value, 2),
                        'strength_score': round(components.get('strength_score', 0.0), 2),
                        'body_score': round(components.get('body_score', 0.0), 2),
                        'volume_score': round(components.get('volume_score', 0.0), 2),
                        'structure_score': round(components.get('structure_score', 0.0), 2),
                        'retest_score': round(components.get('retest_score', 0.0), 2),
                        'time_score': round(components.get('time_score', 0.0), 2),
                        'distance_score': round(components.get('distance_score', 0.0), 2),
                        'strength_ratio': round(components.get('strength_ratio', 0.0), 4),
                        'body_ratio': round(components.get('body_ratio', 0.0), 4),
                        'consolidation_candles': int(components.get('consolidation_candles', 0.0)),
                        'next_level_distance': round(components.get('next_level_distance', 0.0), 4) if components.get('next_level_distance', -1.0) >= 0 else '',
                        'rejection_reason': rejection_reason,
                        'day': day.isoformat(),
                        'entry_trigger_price': round(trigger, 4),
                        'fill_model': 'TRIGGER_PLUS_SLIPPAGE',
                        'trailing_stop_loss': round(trail_stop, 4),
                        'market_regime': regime,
                        'breakout_strength': round(components.get('breakout_strength', 0.0), 4),
                        'volume_ratio': round(components.get('volume_ratio', 0.0), 4),
                        'avg_candle_range': round(components.get('avg_candle_range', 0.0), 4),
                        'clean_break': 'YES' if components.get('clean_break', 0.0) >= 1 else 'NO',
                        'mae': mae,
                        'mfe': mfe,
                        'time_to_target_min': time_to_target_min,
                        'time_to_stop_min': time_to_stop_min,
                        'fake_breakout': fake_breakout,
                        'first_hour_bias': bias,
                        'bias_mode': 'REQUIRED' if cfg.use_first_hour_bias else 'OBSERVE_ONLY',
                        'bias_aligned': 'YES' if side == bias else 'NO',
                        'market_structure_ok': 'YES' if structure_ok else 'NO',
                        'market_structure_label': structure_label,
                        'regime_filter': 'ON' if cfg.filter_choppy_days else 'OFF',
                        'session_allowed': 'YES',
                        'session_gate': 'PASS',
                        'session_filter_mode': 'MORNING_ONLY',
                        'session_window': 'MORNING',
                        'vwap_aligned': 'YES' if cfg.require_vwap_alignment else 'OPTIONAL',
                        'vwap_gate': 'PASS' if cfg.require_vwap_alignment else 'OPTIONAL',
                        'secondary_entries': 'ON' if cfg.allow_secondary_entries else 'OFF',
                        'volatility_score': int(volatility_snapshot.get('volatility_score', 0)),
                        'volatility_decision': str(volatility_snapshot.get('volatility_decision', 'NO_TRADE_LOW_VOL')),
                        'volatility_market_state': str(volatility_snapshot.get('market_state', 'QUIET')),
                        'atr_pct': round(float(volatility_snapshot.get('atr_pct', 0.0)), 2),
                        'opening_volatility_pct': round(float(volatility_snapshot.get('opening_volatility_pct', 0.0)), 2),
                        'vwap_deviation_pct': round(float(volatility_snapshot.get('vwap_deviation_pct', 0.0)), 2),
                        'expansion_ratio': round(float(volatility_snapshot.get('expansion_ratio', 0.0)), 2),
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

<<<<<<< HEAD
















=======
>>>>>>> fed8576 ( modifyed with ltp verson2)
