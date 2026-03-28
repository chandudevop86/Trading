from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any

from src.execution_engine import normalize_order_quantity
from src.strategy_common import NSE_MARKET_CLOSE, NSE_MARKET_OPEN, parse_hhmm, session_window


@dataclass(frozen=True, slots=True)
class CrisisRiskConfig:
    market_label: str = 'NSE_NIFTY_INTRADAY'
    market_open: str = NSE_MARKET_OPEN
    market_close: str = NSE_MARKET_CLOSE
    core_session_start: str = '09:25'
    core_session_end: str = '11:15'
    midday_start: str = '11:16'
    midday_end: str = '13:45'
    afternoon_start: str = '13:46'
    afternoon_end: str = '14:45'
    caution_opening_gap_pct: float = 0.75
    crisis_opening_gap_pct: float = 1.25
    caution_first_15m_range_pct: float = 0.90
    crisis_first_15m_range_pct: float = 1.50
    caution_first_hour_range_pct: float = 1.80
    crisis_first_hour_range_pct: float = 2.80
    caution_first_hour_vol_ratio: float = 1.60
    crisis_first_hour_vol_ratio: float = 2.10
    max_missing_bars_before_halt: int = 0
    max_execution_errors_before_halt: int = 0
    caution_quantity_scale: float = 0.50
    crisis_quantity_scale: float = 0.50
    caution_max_trades_per_day: int = 1
    caution_max_open_trades: int = 1
    caution_daily_loss_scale: float = 0.50
    crisis_daily_loss_scale: float = 0.35


STATE_NORMAL = 'NORMAL'
STATE_CAUTION = 'CAUTION'
STATE_CRISIS = 'CRISIS'
STATE_HALT = 'HALT'


def default_nifty_crisis_config() -> CrisisRiskConfig:
    """Return a strict crisis-risk configuration for Nifty intraday trading."""
    return CrisisRiskConfig()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _infer_timeframe_minutes(timeframe: str) -> int:
    raw = str(timeframe or '').strip().lower()
    mapping = {
        '1m': 1,
        '2m': 2,
        '3m': 3,
        '5m': 5,
        '10m': 10,
        '15m': 15,
        '30m': 30,
        '45m': 45,
        '60m': 60,
        '1h': 60,
    }
    return int(mapping.get(raw, 5))


def _normalized_market_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _parse_dt(row.get('timestamp') or row.get('datetime') or row.get('date'))
        if timestamp is None:
            continue
        open_price = _safe_float(row.get('open'))
        high_price = _safe_float(row.get('high'))
        low_price = _safe_float(row.get('low'))
        close_price = _safe_float(row.get('close'))
        if min(open_price, high_price, low_price, close_price) <= 0:
            continue
        normalized.append(
            {
                'timestamp': timestamp,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
            }
        )
    normalized.sort(key=lambda item: item['timestamp'])
    return normalized


def _latest_day_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    latest_day = rows[-1]['timestamp'].date()
    return [row for row in rows if row['timestamp'].date() == latest_day]


def _previous_day_close(rows: list[dict[str, Any]]) -> float:
    latest_day_rows = _latest_day_rows(rows)
    if not latest_day_rows:
        return 0.0
    latest_day = latest_day_rows[0]['timestamp'].date()
    previous_rows = [row for row in rows if row['timestamp'].date() < latest_day]
    if not previous_rows:
        return 0.0
    return _safe_float(previous_rows[-1]['close'])


def _range_pct(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    first_open = _safe_float(rows[0]['open'])
    if first_open <= 0:
        return 0.0
    highest = max(_safe_float(row['high']) for row in rows)
    lowest = min(_safe_float(row['low']) for row in rows)
    return round(((highest - lowest) / first_open) * 100.0, 4)


def _median(values: list[float]) -> float:
    ordered = sorted(v for v in values if v > 0)
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _first_hour_volatility_ratio(rows: list[dict[str, Any]], timeframe_minutes: int) -> float:
    if not rows:
        return 0.0
    first_hour_bars = max(1, int(60 / max(timeframe_minutes, 1)))
    first_hour_rows = rows[:first_hour_bars]
    first_hour_ranges = [_safe_float(row['high']) - _safe_float(row['low']) for row in first_hour_rows]
    baseline_ranges = [_safe_float(row['high']) - _safe_float(row['low']) for row in rows[-max(20, first_hour_bars * 3):]]
    baseline = _median(baseline_ranges)
    if baseline <= 0:
        return 0.0
    first_hour_avg = sum(first_hour_ranges) / max(len(first_hour_ranges), 1)
    return round(first_hour_avg / baseline, 4)


def _missing_bar_count(rows: list[dict[str, Any]], timeframe_minutes: int, config: CrisisRiskConfig) -> int:
    if len(rows) < 2:
        return 0
    expected_delta = max(1, int(timeframe_minutes))
    market_open = parse_hhmm(config.market_open, NSE_MARKET_OPEN)
    market_close = parse_hhmm(config.market_close, NSE_MARKET_CLOSE)
    missing = 0
    for prev, curr in zip(rows, rows[1:]):
        prev_ts = prev['timestamp']
        curr_ts = curr['timestamp']
        if prev_ts.date() != curr_ts.date():
            continue
        if not (market_open <= prev_ts.time().replace(second=0, microsecond=0) <= market_close):
            continue
        delta_minutes = int((curr_ts - prev_ts).total_seconds() // 60)
        if delta_minutes > expected_delta:
            missing += max(0, int(delta_minutes / expected_delta) - 1)
    return missing


def detect_market_stress(
    market_rows: list[dict[str, Any]],
    execution_snapshot: dict[str, Any] | None,
    config: CrisisRiskConfig | None = None,
    *,
    timeframe: str = '5m',
) -> dict[str, Any]:
    """Detect whether current Nifty intraday conditions warrant crisis controls."""
    cfg = config or default_nifty_crisis_config()
    normalized_rows = _normalized_market_rows(market_rows)
    latest_day_rows = _latest_day_rows(normalized_rows)
    timeframe_minutes = _infer_timeframe_minutes(timeframe)
    previous_close = _previous_day_close(normalized_rows)
    opening_gap_pct = 0.0
    if latest_day_rows and previous_close > 0:
        opening_gap_pct = round(abs(_safe_float(latest_day_rows[0]['open']) - previous_close) / previous_close * 100.0, 4)
    first_15m_bars = max(1, int(15 / max(timeframe_minutes, 1)))
    first_hour_bars = max(1, int(60 / max(timeframe_minutes, 1)))
    first_15m_range_pct = _range_pct(latest_day_rows[:first_15m_bars])
    first_hour_range_pct = _range_pct(latest_day_rows[:first_hour_bars])
    first_hour_vol_ratio = _first_hour_volatility_ratio(latest_day_rows, timeframe_minutes)
    missing_bar_count = _missing_bar_count(latest_day_rows, timeframe_minutes, cfg)
    execution_errors = _safe_int((execution_snapshot or {}).get('execution_error_count', 0))
    latest_session = ''
    if latest_day_rows:
        latest_session = session_window(
            latest_day_rows[-1]['timestamp'],
            morning_start=cfg.core_session_start,
            morning_end=cfg.core_session_end,
            midday_start=cfg.midday_start,
            midday_end=cfg.midday_end,
            allow_afternoon_session=False,
            afternoon_start=cfg.afternoon_start,
            afternoon_end=cfg.afternoon_end,
        )

    blocking_reasons: list[str] = []
    warnings: list[str] = []
    state = STATE_NORMAL

    if execution_errors > cfg.max_execution_errors_before_halt:
        state = STATE_HALT
        blocking_reasons.append('execution_errors_present')
    if missing_bar_count > cfg.max_missing_bars_before_halt:
        state = STATE_HALT
        blocking_reasons.append('market_data_gaps_detected')

    crisis_conditions = {
        'opening_gap_crisis': opening_gap_pct >= cfg.crisis_opening_gap_pct,
        'first_15m_range_crisis': first_15m_range_pct >= cfg.crisis_first_15m_range_pct,
        'first_hour_range_crisis': first_hour_range_pct >= cfg.crisis_first_hour_range_pct,
        'first_hour_volatility_crisis': first_hour_vol_ratio >= cfg.crisis_first_hour_vol_ratio,
    }
    caution_conditions = {
        'opening_gap_caution': opening_gap_pct >= cfg.caution_opening_gap_pct,
        'first_15m_range_caution': first_15m_range_pct >= cfg.caution_first_15m_range_pct,
        'first_hour_range_caution': first_hour_range_pct >= cfg.caution_first_hour_range_pct,
        'first_hour_volatility_caution': first_hour_vol_ratio >= cfg.caution_first_hour_vol_ratio,
    }

    if state != STATE_HALT and any(crisis_conditions.values()):
        state = STATE_CRISIS
        blocking_reasons.extend(key for key, hit in crisis_conditions.items() if hit)
    elif state == STATE_NORMAL and any(caution_conditions.values()):
        state = STATE_CAUTION
        warnings.extend(key for key, hit in caution_conditions.items() if hit)

    recommended_action = {
        STATE_NORMAL: 'Normal risk controls can remain active.',
        STATE_CAUTION: 'Keep the system in reduced-size live mode or paper until volatility normalizes.',
        STATE_CRISIS: 'Force paper-only trading and disable new live deployment for this session.',
        STATE_HALT: 'Stop new entries immediately and only manage exits or remain flat.',
    }[state]

    return {
        'stress_state': state,
        'blocking_reasons': sorted(set(blocking_reasons)),
        'warnings': sorted(set(warnings)),
        'recommended_action': recommended_action,
        'market_metrics': {
            'market_label': cfg.market_label,
            'timeframe': timeframe,
            'opening_gap_pct': opening_gap_pct,
            'first_15m_range_pct': first_15m_range_pct,
            'first_hour_range_pct': first_hour_range_pct,
            'first_hour_volatility_ratio': first_hour_vol_ratio,
            'missing_bar_count': missing_bar_count,
            'execution_error_count': execution_errors,
            'latest_session_window': latest_session,
            'latest_day_rows': len(latest_day_rows),
        },
        'config': asdict(cfg),
    }


def _scaled_daily_loss(max_daily_loss: float | None, scale: float) -> float | None:
    if max_daily_loss is None or float(max_daily_loss) <= 0:
        return max_daily_loss
    return round(float(max_daily_loss) * float(scale), 2)


def _scale_candidates(candidates: list[dict[str, Any]], quantity_scale: float, reason: str) -> list[dict[str, Any]]:
    scaled: list[dict[str, Any]] = []
    for candidate in candidates:
        item = dict(candidate)
        raw_quantity = _safe_int(item.get('quantity', 0))
        scaled_quantity = raw_quantity
        if raw_quantity > 0 and quantity_scale > 0 and quantity_scale < 1.0:
            scaled_quantity = normalize_order_quantity(str(item.get('symbol', '')), max(1, int(round(raw_quantity * quantity_scale))))
        item['quantity'] = scaled_quantity if scaled_quantity > 0 else raw_quantity
        item['crisis_quantity_scale'] = round(float(quantity_scale), 4)
        item['crisis_risk_reason'] = reason
        scaled.append(item)
    return scaled


def apply_crisis_overrides(
    candidates: list[dict[str, Any]],
    stress_evaluation: dict[str, Any],
    config: CrisisRiskConfig | None = None,
    *,
    requested_execution_type: str,
    max_trades_per_day: int | None,
    max_daily_loss: float | None,
) -> dict[str, Any]:
    """Apply strict crisis-mode routing and risk overrides before execution."""
    cfg = config or default_nifty_crisis_config()
    state = str(stress_evaluation.get('stress_state', STATE_NORMAL) or STATE_NORMAL).upper()
    execution_type = str(requested_execution_type or 'NONE').strip().upper()
    override_reason = '|'.join(stress_evaluation.get('blocking_reasons', []) or stress_evaluation.get('warnings', []) or [state.lower()])
    adjusted_candidates = list(candidates)
    adjusted_max_trades = max_trades_per_day
    adjusted_max_daily_loss = max_daily_loss
    adjusted_max_open_trades: int | None = None
    notes: list[str] = []

    if state == STATE_HALT:
        return {
            'requested_execution_type': execution_type,
            'execution_type': 'NONE',
            'candidates': [],
            'max_trades_per_day': 0,
            'max_daily_loss': adjusted_max_daily_loss,
            'max_open_trades': 0,
            'override_notes': ['halt_state_blocks_new_entries'],
        }

    if state == STATE_CRISIS:
        execution_type = 'PAPER' if execution_type in {'LIVE', 'PAPER'} else execution_type
        adjusted_candidates = _scale_candidates(adjusted_candidates, cfg.crisis_quantity_scale, override_reason or 'crisis_mode')
        adjusted_max_trades = min(int(max_trades_per_day or 1), 1)
        adjusted_max_open_trades = 1
        adjusted_max_daily_loss = _scaled_daily_loss(max_daily_loss, cfg.crisis_daily_loss_scale)
        notes.append('crisis_mode_forces_paper')
    elif state == STATE_CAUTION:
        adjusted_candidates = _scale_candidates(adjusted_candidates, cfg.caution_quantity_scale, override_reason or 'caution_mode')
        adjusted_max_trades = min(int(max_trades_per_day or cfg.caution_max_trades_per_day), cfg.caution_max_trades_per_day)
        adjusted_max_open_trades = cfg.caution_max_open_trades
        adjusted_max_daily_loss = _scaled_daily_loss(max_daily_loss, cfg.caution_daily_loss_scale)
        notes.append('caution_mode_reduces_size')
    else:
        notes.append('normal_mode_no_crisis_override')

    for candidate in adjusted_candidates:
        candidate['crisis_state'] = state
        candidate['crisis_policy_execution_type'] = execution_type

    return {
        'requested_execution_type': str(requested_execution_type or 'NONE').strip().upper(),
        'execution_type': execution_type,
        'candidates': adjusted_candidates,
        'max_trades_per_day': adjusted_max_trades,
        'max_daily_loss': adjusted_max_daily_loss,
        'max_open_trades': adjusted_max_open_trades,
        'override_notes': notes,
    }


def evaluate_live_permission(
    go_live_evaluation: dict[str, Any],
    stress_evaluation: dict[str, Any],
    config: CrisisRiskConfig | None = None,
    *,
    requested_execution_type: str,
    allow_live_on_pass: bool,
) -> dict[str, Any]:
    """Combine strategy validation and crisis state into a strict live-deployment decision."""
    _ = config or default_nifty_crisis_config()
    stress_state = str(stress_evaluation.get('stress_state', STATE_NORMAL) or STATE_NORMAL).upper()
    go_live_status = str(go_live_evaluation.get('decision_status', 'FAIL_NOT_READY') or 'FAIL_NOT_READY').upper()
    requested = str(requested_execution_type or 'NONE').strip().upper()
    blocking_reasons = list(go_live_evaluation.get('blocking_reasons', [])) + list(stress_evaluation.get('blocking_reasons', []))
    warnings = list(go_live_evaluation.get('warnings', [])) + list(stress_evaluation.get('warnings', []))

    if requested != 'LIVE':
        decision_status = 'PAPER_ONLY' if requested == 'PAPER' else 'NO_NEW_ENTRIES'
        recommended_execution_type = requested
    elif stress_state == STATE_HALT:
        decision_status = 'NO_NEW_ENTRIES'
        recommended_execution_type = 'NONE'
        blocking_reasons.append('halt_state_blocks_live')
    elif stress_state == STATE_CRISIS:
        decision_status = 'PAPER_ONLY'
        recommended_execution_type = 'PAPER'
        blocking_reasons.append('crisis_state_blocks_live')
    elif go_live_status != 'PASS_FOR_SMALL_CAPITAL':
        decision_status = 'PAPER_ONLY'
        recommended_execution_type = 'PAPER'
        blocking_reasons.append('go_live_validation_not_passed')
    elif not allow_live_on_pass:
        decision_status = 'PAPER_ONLY'
        recommended_execution_type = 'PAPER'
        blocking_reasons.append('allow_live_on_pass_flag_required')
    elif stress_state == STATE_CAUTION:
        decision_status = 'LIVE_REDUCED_CAUTION'
        recommended_execution_type = 'LIVE'
        warnings.append('caution_state_requires_reduced_live_risk')
    else:
        decision_status = 'LIVE_ALLOWED_SMALL'
        recommended_execution_type = 'LIVE'

    next_action = {
        'LIVE_ALLOWED_SMALL': 'Live trading is allowed with the current small-capital controls.',
        'LIVE_REDUCED_CAUTION': 'Live trading may proceed only with reduced size, one open trade, and tighter daily loss limits.',
        'PAPER_ONLY': 'Stay on paper until both crisis conditions and go-live blockers clear.',
        'NO_NEW_ENTRIES': 'Do not place new trades; remain flat or manage exits only.',
    }[decision_status]

    return {
        'decision_status': decision_status,
        'recommended_execution_type': recommended_execution_type,
        'blocking_reasons': sorted(set(blocking_reasons)),
        'warnings': sorted(set(warnings)),
        'recommended_next_action': next_action,
    }


def write_crisis_summary_json(path: str | Path, summary: dict[str, Any]) -> Path:
    """Write crisis-risk evaluation details to JSON."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    return output_path
