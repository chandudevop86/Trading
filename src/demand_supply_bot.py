from __future__ import annotations

from dataclasses import dataclass, field
from datetime import time
from typing import Any

from src.breakout_bot import Candle, _coerce_candles, add_intraday_vwap
from src.trading_core import ScoringConfig, ScoreThresholds, StandardTrade, safe_quantity, weighted_score


@dataclass(frozen=True, slots=True)
class Zone:
    kind: str
    low: float
    high: float
    idx: int
    reaction_strength: float


@dataclass(slots=True)
class DemandSupplyConfig:
    mode: str = 'Balanced'
    trailing_sl_pct: float = 0.0
    pivot_window: int = 2
    touch_tolerance_pct: float = 0.004
    max_trades_per_day: int = 1
    duplicate_signal_cooldown_bars: int = 12
    opening_range_minutes: int = 15
    atr_window: int = 6
    min_volatility_ratio: float = 0.85
    zone_freshness_bars: int = 18
    reaction_threshold: float = 0.45
    min_reaction_strength: float = 0.55
    min_zone_strength_score: float = 4.0
    require_vwap_alignment: bool = True
    require_trend_bias: bool = True
    morning_session_start: str = '09:20'
    morning_session_end: str = '11:30'
    afternoon_session_start: str = '13:45'
    afternoon_session_end: str = '15:00'
    allow_afternoon_session: bool = False
    midday_start: str = '12:00'
    midday_end: str = '13:30'
    retest_confirmation_bars: int = 2
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    def __post_init__(self) -> None:
        self.scoring.mode = self.mode
        self.scoring.thresholds = ScoreThresholds(conservative=7.0, balanced=5.4, aggressive=4.0)


def _group_by_day(candles: list[Candle]) -> dict[object, list[Candle]]:
    by_day: dict[object, list[Candle]] = {}
    for candle in candles:
        by_day.setdefault(candle.timestamp.date(), []).append(candle)
    return by_day


def _parse_hhmm(value: str, fallback: str) -> time:
    raw = str(value or fallback).strip() or fallback
    try:
        hh, mm = raw.split(':', 1)
        return time(hour=max(0, min(23, int(hh))), minute=max(0, min(59, int(mm))))
    except Exception:
        fh, fm = fallback.split(':', 1)
        return time(hour=int(fh), minute=int(fm))


def _intraday_range(candle: Candle) -> float:
    return max(float(candle.high) - float(candle.low), 0.0001)


def _body_ratio(candle: Candle) -> float:
    return abs(float(candle.close) - float(candle.open)) / _intraday_range(candle)

def calculate_vwap(candles: list[Candle]) -> None:
    """Compute and attach intraday VWAP once for the supplied candle sequence."""
    add_intraday_vwap(candles)


def session_filter(candle: Candle, config: DemandSupplyConfig) -> bool:
    """Return True when a candle is inside the allowed trading session and outside midday block."""
    return _session_allowed(candle, config) and not _midday_restricted(candle, config)


def detect_retest(day_candles: list[Candle], zone: Zone, side: str, start_idx: int, config: DemandSupplyConfig) -> tuple[int, int] | None:
    """Detect a valid touch plus rejection retest cycle for a zone and return touch/confirmation indexes."""
    tol = float(config.touch_tolerance_pct or 0.0)
    for touch_idx in range(max(zone.idx + 1, start_idx), len(day_candles)):
        candle = day_candles[touch_idx]
        if not session_filter(candle, config):
            continue
        touch = float(candle.low) <= float(zone.high) * (1.0 + tol) if side == 'BUY' else float(candle.high) >= float(zone.low) * (1.0 - tol)
        if not touch:
            continue
        confirmation_limit = min(len(day_candles), touch_idx + 1 + max(1, int(config.retest_confirmation_bars)))
        for confirmation_idx in range(touch_idx, confirmation_limit):
            confirm_candle = day_candles[confirmation_idx]
            if not session_filter(confirm_candle, config):
                continue
            if _retest_confirmation_candle(confirm_candle, zone, side):
                return touch_idx, confirmation_idx
    return None


def score_zone(day_candles: list[Candle], idx: int, zone: Zone, side: str, config: DemandSupplyConfig, *, touch: bool, retest_confirmed: bool) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    """Score a supply/demand zone and return diagnostics for execution-quality filtering."""
    return _quality_score(day_candles, idx, zone, side, config, touch=touch, retest_confirmed=retest_confirmed)

def _reaction_strength(day_candles: list[Candle], idx: int, side: str) -> float:
    candle = day_candles[idx]
    move = abs(float(candle.close) - float(candle.open))
    full_range = _intraday_range(candle)
    wick = (float(candle.close) - float(candle.low)) if side == 'BUY' else (float(candle.high) - float(candle.close))
    follow_through = 0.0
    if idx + 1 < len(day_candles):
        next_candle = day_candles[idx + 1]
        if side == 'BUY':
            follow_through = max(float(next_candle.close) - float(candle.close), 0.0)
        else:
            follow_through = max(float(candle.close) - float(next_candle.close), 0.0)
    return max(move / full_range, wick / full_range, follow_through / full_range)


def _find_zones(day_candles: list[Candle], pivot_window: int) -> list[Zone]:
    n = len(day_candles)
    w = max(1, int(pivot_window or 1))
    zones: list[Zone] = []
    for i in range(w, n - w):
        candle = day_candles[i]
        lows = [day_candles[j].low for j in range(i - w, i + w + 1) if j != i]
        highs = [day_candles[j].high for j in range(i - w, i + w + 1) if j != i]
        if lows and candle.low < min(lows):
            zones.append(
                Zone(
                    kind='demand',
                    low=float(candle.low),
                    high=float(max(candle.open, candle.close)),
                    idx=i,
                    reaction_strength=_reaction_strength(day_candles, i, 'BUY'),
                )
            )
        if highs and candle.high > max(highs):
            zones.append(
                Zone(
                    kind='supply',
                    low=float(min(candle.open, candle.close)),
                    high=float(candle.high),
                    idx=i,
                    reaction_strength=_reaction_strength(day_candles, i, 'SELL'),
                )
            )
    return zones


def _trend_ok(day_candles: list[Candle], idx: int, side: str) -> bool:
    if idx < 2:
        return False
    fast = sum(c.close for c in day_candles[max(0, idx - 2): idx + 1]) / min(3, idx + 1)
    slow = sum(c.close for c in day_candles[max(0, idx - 7): idx + 1]) / min(8, idx + 1)
    close = float(day_candles[idx].close)
    if side == 'BUY':
        return close >= fast >= slow
    return close <= fast <= slow


def _zone_mid(zone: Zone) -> float:
    return (float(zone.low) + float(zone.high)) / 2.0


def _opening_range(day_candles: list[Candle], minutes: int) -> tuple[float, float, float, float]:
    if not day_candles:
        return 0.0, 0.0, 0.0, 0.0
    start = day_candles[0].timestamp
    selected = [c for c in day_candles if (c.timestamp - start).total_seconds() / 60.0 < float(minutes)]
    if not selected:
        selected = day_candles[: max(1, min(3, len(day_candles)))]
    return (
        float(max(c.high for c in selected)),
        float(min(c.low for c in selected)),
        float(selected[0].open),
        float(selected[-1].close),
    )


def _opening_range_alignment(day_candles: list[Candle], idx: int, side: str, minutes: int) -> bool:
    orb_high, orb_low, orb_open, orb_close = _opening_range(day_candles, minutes)
    close = float(day_candles[idx].close)
    if side == 'BUY':
        return close >= ((orb_high + orb_open) / 2.0) and orb_close >= orb_open
    return close <= ((orb_low + orb_open) / 2.0) and orb_close <= orb_open


def _avg_range(day_candles: list[Candle], idx: int, window: int) -> float:
    start = max(0, idx - max(1, window) + 1)
    sample = day_candles[start: idx + 1]
    if not sample:
        return 0.0
    return sum(_intraday_range(c) for c in sample) / len(sample)


def _volatility_ok(day_candles: list[Candle], idx: int, config: DemandSupplyConfig) -> bool:
    current_range = _intraday_range(day_candles[idx])
    avg_range = _avg_range(day_candles, idx, config.atr_window)
    if avg_range <= 0:
        return False
    return current_range >= avg_range * float(config.min_volatility_ratio)


def _zone_freshness_score(idx: int, zone: Zone, config: DemandSupplyConfig) -> float:
    age = max(idx - zone.idx, 0)
    freshness = max(float(config.zone_freshness_bars) - float(age), 0.0)
    if config.zone_freshness_bars <= 0:
        return 0.0
    return freshness / float(config.zone_freshness_bars)


def _midday_restricted(candle: Candle, config: DemandSupplyConfig) -> bool:
    start = _parse_hhmm(config.midday_start, '12:00')
    end = _parse_hhmm(config.midday_end, '13:30')
    current = candle.timestamp.time().replace(second=0, microsecond=0)
    return start <= current <= end


def _session_allowed(candle: Candle, config: DemandSupplyConfig) -> bool:
    current = candle.timestamp.time().replace(second=0, microsecond=0)
    morning_start = _parse_hhmm(config.morning_session_start, '09:20')
    morning_end = _parse_hhmm(config.morning_session_end, '11:30')
    if morning_start <= current <= morning_end:
        return True
    if bool(config.allow_afternoon_session):
        afternoon_start = _parse_hhmm(config.afternoon_session_start, '13:45')
        afternoon_end = _parse_hhmm(config.afternoon_session_end, '15:00')
        if afternoon_start <= current <= afternoon_end:
            return True
    return False


def _higher_tf_bias(day_candles: list[Candle], idx: int) -> str:
    if idx < 4:
        return 'NEUTRAL'
    closes = [float(c.close) for c in day_candles[: idx + 1]]
    if len(closes) < 5:
        return 'NEUTRAL'
    fast_window = closes[-3:]
    slow_window = closes[-5:]
    fast = sum(fast_window) / len(fast_window)
    slow = sum(slow_window) / len(slow_window)
    slope = closes[-1] - closes[max(0, len(closes) - 4)]
    bias_tolerance = max(0.02, closes[-1] * 0.0005)
    if fast + bias_tolerance >= slow and slope > 0:
        return 'BULLISH'
    if fast - bias_tolerance <= slow and slope < 0:
        return 'BEARISH'
    return 'NEUTRAL'


def _zone_strength_score(
    *,
    freshness_score: float,
    reaction_score: float,
    touch: bool,
    retest_confirmed: bool,
    body_ok: bool,
    config: DemandSupplyConfig,
) -> float:
    score = 0.0
    if freshness_score >= 0.55:
        score += 1.0
    if reaction_score >= float(config.reaction_threshold):
        score += 1.0
    if reaction_score >= float(config.min_reaction_strength):
        score += 1.0
    if touch and retest_confirmed:
        score += 1.0
    if body_ok:
        score += 0.5
    return score


def _retest_confirmation_candle(candle: Candle, zone: Zone, side: str) -> bool:
    close = float(candle.close)
    open_ = float(candle.open)
    body_ok = _body_ratio(candle) >= 0.45
    zone_mid = _zone_mid(zone)
    if side == 'BUY':
        return close > float(zone.high) and close > open_ and close >= zone_mid and body_ok
    return close < float(zone.low) and close < open_ and close <= zone_mid and body_ok


def _quality_score(
    day_candles: list[Candle],
    idx: int,
    zone: Zone,
    side: str,
    config: DemandSupplyConfig,
    *,
    touch: bool,
    retest_confirmed: bool,
) -> tuple[float, dict[str, float], str, str, dict[str, object]] | None:
    candle = day_candles[idx]
    close = float(candle.close)
    freshness_score = _zone_freshness_score(idx, zone, config)
    reaction_score = max(zone.reaction_strength, _reaction_strength(day_candles, idx, side))
    vwap_ok = close >= float(candle.vwap) if side == 'BUY' else close <= float(candle.vwap)
    trend_ok = _trend_ok(day_candles, idx, side)
    trend_bias = _higher_tf_bias(day_candles, idx)
    bias_aligned = trend_bias == ('BULLISH' if side == 'BUY' else 'BEARISH')
    opening_ok = _opening_range_alignment(day_candles, idx, side, config.opening_range_minutes)
    volatility_ok = _volatility_ok(day_candles, idx, config)
    body_ok = _body_ratio(candle) >= 0.38
    zone_strength = _zone_strength_score(
        freshness_score=freshness_score,
        reaction_score=reaction_score,
        touch=touch,
        retest_confirmed=retest_confirmed,
        body_ok=body_ok,
        config=config,
    )

    if config.require_vwap_alignment and not vwap_ok:
        return None
    if config.require_trend_bias and not bias_aligned:
        return None
    if zone_strength < float(config.min_zone_strength_score):
        return None

    score = weighted_score(
        {
            'trend': trend_ok and bias_aligned,
            'vwap': vwap_ok,
            'rsi': freshness_score >= 0.35,
            'adx': volatility_ok,
            'zone': touch and reaction_score >= float(config.reaction_threshold),
            'retest': retest_confirmed,
            'reaction': reaction_score >= float(config.reaction_threshold),
            'breakout_quality': opening_ok and body_ok,
        },
        config.scoring,
    )
    if _midday_restricted(candle, config) and config.mode.lower() != 'aggressive' and score.total < float(config.scoring.threshold()) + 1.0:
        return None
    if not score.accepted:
        return None
    reason = f'{side.lower()} zone retest score={score.total:.2f} zone_strength={zone_strength:.2f} bias={trend_bias}'
    rejection_reason = '' if score.accepted else ','.join(f'missing_{name}' for name in score.reasons)
    diagnostics = {
        'trend_bias': trend_bias,
        'bias_aligned': bias_aligned,
        'vwap_aligned': vwap_ok,
        'zone_strength_score': round(zone_strength, 2),
        'freshness_score': round(freshness_score, 4),
        'reaction_score': round(reaction_score, 4),
        'opening_ok': opening_ok,
        'body_ok': body_ok,
        'trend_ok': trend_ok,
        'volatility_ok': volatility_ok,
    }
    return score.total, score.components, reason, rejection_reason, diagnostics

def _entry_levels(candle: Candle, zone: Zone, side: str, rr_ratio: float, avg_range: float) -> tuple[float, float, float]:
    entry = float(candle.close)
    buffer = max(avg_range * 0.18, entry * 0.0009)
    if side == 'BUY':
        stop = min(float(zone.low), float(candle.low)) - buffer
        if stop >= entry:
            stop = entry - max(avg_range * 0.25, entry * 0.001)
        target = entry + (entry - stop) * float(rr_ratio or 2.0)
    else:
        stop = max(float(zone.high), float(candle.high)) + buffer
        if stop <= entry:
            stop = entry + max(avg_range * 0.25, entry * 0.001)
        target = entry - (stop - entry) * float(rr_ratio or 2.0)
    return entry, stop, target


def generate_trades(
    df: Any,
    capital: float,
    risk_pct: float,
    rr_ratio: float = 2.0,
    config: DemandSupplyConfig | None = None,
    *,
    trailing_sl_pct: float = 0.0,
    pivot_window: int = 2,
    touch_tolerance_pct: float = 0.006,
    max_trades_per_day: int = 1,
) -> list[dict[str, object]]:
    """Generate scored demand/supply trades with Nifty 5m quality filters."""
    cfg = config or DemandSupplyConfig()
    if config is None:
        cfg.trailing_sl_pct = float(trailing_sl_pct)
        cfg.pivot_window = int(pivot_window)
        cfg.touch_tolerance_pct = float(touch_tolerance_pct)
        cfg.max_trades_per_day = int(max_trades_per_day)

    candles = _coerce_candles(df)
    if not candles:
        return []
    calculate_vwap(candles)
    by_day = _group_by_day(candles)
    trades: list[dict[str, object]] = []

    for day in sorted(by_day.keys()):
        day_candles = sorted(by_day[day], key=lambda c: c.timestamp)
        if len(day_candles) < (cfg.pivot_window * 2 + 5):
            continue
        zones = _find_zones(day_candles, pivot_window=cfg.pivot_window)
        if not zones:
            continue

        trades_taken = 0
        last_signal_index: dict[str, int] = {'BUY': -10_000, 'SELL': -10_000}
        used_signal_keys: set[tuple[str, str, str, str]] = set()

        for zone in sorted(zones, key=lambda z: (z.reaction_strength, z.idx), reverse=True):
            if trades_taken >= int(cfg.max_trades_per_day or 1):
                break
            side = 'BUY' if zone.kind == 'demand' else 'SELL'
            tol = float(cfg.touch_tolerance_pct or 0.0)

            for i in range(zone.idx + 1, len(day_candles)):
                if trades_taken >= int(cfg.max_trades_per_day or 1):
                    break
                if i - last_signal_index[side] < int(cfg.duplicate_signal_cooldown_bars):
                    continue
                retest = detect_retest(day_candles, zone, side, i, cfg)
                if retest is None:
                    continue

                touch_idx, confirmation_idx = retest
                touch = True
                candle = day_candles[touch_idx]
                entry_candle = day_candles[confirmation_idx]
                score_result = score_zone(day_candles, confirmation_idx, zone, side, cfg, touch=touch, retest_confirmed=True)
                if score_result is None:
                    continue

                signal_key = ('DEMAND_SUPPLY', str(day), entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'), side)
                if signal_key in used_signal_keys:
                    continue

                avg_range = _avg_range(day_candles, confirmation_idx, cfg.atr_window)
                entry, stop, target = _entry_levels(entry_candle, zone, side, rr_ratio, avg_range)
                qty = safe_quantity(capital=capital, risk_pct=risk_pct, entry=entry, stop_loss=stop)
                if qty <= 0:
                    continue

                score_value, components, reason, rejection_reason, diagnostics = score_result
                trail_stop = stop
                exit_price = float(day_candles[-1].close)
                exit_time = day_candles[-1].timestamp
                exit_reason = 'EOD'

                for follow_idx in range(confirmation_idx + 1, len(day_candles)):
                    follow = day_candles[follow_idx]
                    if cfg.trailing_sl_pct and float(cfg.trailing_sl_pct) > 0:
                        if side == 'BUY':
                            trail_stop = max(trail_stop, float(follow.high) * (1.0 - float(cfg.trailing_sl_pct)))
                        else:
                            trail_stop = min(trail_stop, float(follow.low) * (1.0 + float(cfg.trailing_sl_pct)))
                    if side == 'BUY':
                        if follow.low <= trail_stop:
                            exit_price = float(trail_stop)
                            exit_time = follow.timestamp
                            exit_reason = 'TRAILING_STOP' if trail_stop > stop else 'STOP_LOSS'
                            break
                        if follow.high >= target:
                            exit_price = float(target)
                            exit_time = follow.timestamp
                            exit_reason = 'TARGET'
                            break
                    else:
                        if follow.high >= trail_stop:
                            exit_price = float(trail_stop)
                            exit_time = follow.timestamp
                            exit_reason = 'TRAILING_STOP' if trail_stop < stop else 'STOP_LOSS'
                            break
                        if follow.low <= target:
                            exit_price = float(target)
                            exit_time = follow.timestamp
                            exit_reason = 'TARGET'
                            break

                pnl = (exit_price - entry) * qty if side == 'BUY' else (entry - exit_price) * qty
                trade = StandardTrade(
                    timestamp=entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    side=side,
                    entry=entry,
                    stop_loss=stop,
                    target=target,
                    strategy='DEMAND_SUPPLY',
                    reason=reason,
                    score=score_value,
                    entry_price=entry,
                    target_price=target,
                    risk_per_unit=abs(entry - stop),
                    quantity=int(qty),
                    zone_type=zone.kind,
                    extra={
                        'setup_type': 'retest',
                        'trend_score': round(components.get('trend', 0.0), 2),
                        'indicator_score': round(sum(components.get(key, 0.0) for key in ['vwap', 'rsi', 'adx', 'breakout_quality']), 2),
                        'zone_score': round(sum(components.get(key, 0.0) for key in ['zone', 'retest', 'reaction']), 2),
                        'total_score': round(score_value, 2),
                        'rejection_reason': rejection_reason,
                        'day': day.isoformat(),
                        'symbol': '^NSEI',
                        'timeframe': '5m',
                        'zone_kind': zone.kind,
                        'zone_low': round(float(zone.low), 4),
                        'zone_high': round(float(zone.high), 4),
                        'zone_reaction_strength': round(float(zone.reaction_strength), 4),
                        'retest_touch_time': candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'retest_confirmation_time': entry_candle.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'session_window': 'AFTERNOON' if bool(cfg.allow_afternoon_session) and entry_candle.timestamp.time() >= _parse_hhmm(cfg.afternoon_session_start, '13:45') else 'MORNING',
                        'zone_freshness_score': round(float(_zone_freshness_score(confirmation_idx, zone, cfg)), 4),
                        'zone_strength_score': float(diagnostics['zone_strength_score']),
                        'trend_bias': str(diagnostics['trend_bias']),
                        'bias_aligned': 'YES' if bool(diagnostics['bias_aligned']) else 'NO',
                        'vwap_aligned': 'YES' if bool(diagnostics['vwap_aligned']) else 'NO',
                        'reaction_score': float(diagnostics['reaction_score']),
                        'opening_range_alignment': 'YES' if bool(diagnostics['opening_ok']) else 'NO',
                        'trend_alignment': 'YES' if bool(diagnostics['trend_ok']) else 'NO',
                        'volatility_ok': 'YES' if bool(diagnostics['volatility_ok']) else 'NO',
                        'session_allowed': 'YES',
                        'duplicate_signal_cooldown_bars': int(cfg.duplicate_signal_cooldown_bars),
                        'trailing_stop_loss': round(float(trail_stop), 4),
                        'exit_time': exit_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'exit_price': round(float(exit_price), 4),
                        'exit_reason': exit_reason,
                        'pnl': round(float(pnl), 2),
                    },
                ).to_dict()
                trades.append(trade)
                trades_taken += 1
                last_signal_index[side] = confirmation_idx
                used_signal_keys.add(signal_key)
                break
    return trades
