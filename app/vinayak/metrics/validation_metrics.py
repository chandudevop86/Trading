from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from vinayak.metrics.utils import coerce_trade_records, normalize_score, safe_divide


DEFAULT_SETUP_QUALITY_WEIGHTS = {
    'zone_score': 0.18,
    'freshness_score': 0.12,
    'move_away_score': 0.12,
    'rejection_strength': 0.12,
    'structure_clarity': 0.12,
    'retest_confirmed': 0.10,
    'vwap_alignment': 0.08,
    'trend_ok': 0.06,
    'volatility_ok': 0.05,
    'chop_ok': 0.05,
}


def compute_setup_quality_score(row: dict[str, Any] | pd.Series, weights: dict[str, float] | None = None) -> float:
    cfg = {**DEFAULT_SETUP_QUALITY_WEIGHTS, **dict(weights or {})}
    total = 0.0
    for key, weight in cfg.items():
        value = row.get(key) if isinstance(row, dict) else row.get(key)
        if key in {'retest_confirmed', 'vwap_alignment', 'trend_ok', 'volatility_ok', 'chop_ok'}:
            total += (100.0 if bool(value) else 0.0) * float(weight)
        else:
            score = normalize_score(value)
            total += float(score or 0.0) * float(weight)
    return round(total, 4)


def calculate_validation_metrics(trades: Any, weights: dict[str, float] | None = None) -> dict[str, Any]:
    frame = coerce_trade_records(trades)
    if frame.empty:
        return {
            'validation_pass_rate': 0.0,
            'validation_fail_rate': 0.0,
            'validation_pass_count': 0,
            'validation_fail_count': 0,
            'rejection_reason_breakdown': {},
            'retest_confirmation_rate': 0.0,
            'vwap_alignment_rate': 0.0,
            'trend_alignment_rate': 0.0,
            'volatility_pass_rate': 0.0,
            'chop_pass_rate': 0.0,
            'average_zone_score': 0.0,
            'average_freshness_score': 0.0,
            'average_move_away_score': 0.0,
            'average_rejection_strength': 0.0,
            'average_structure_clarity': 0.0,
            'high_quality_setup_rate': 0.0,
            'setup_quality_scores': [],
        }

    validation_passed = frame.get('validation_passed', pd.Series([None] * len(frame), index=frame.index))
    if validation_passed.isna().all():
        status = frame.get('validation_status', pd.Series([''] * len(frame), index=frame.index)).astype(str).str.upper()
        validation_passed = status.eq('PASS')

    reason_counter: Counter[str] = Counter()
    for column in ['rejection_reason', 'validation_reasons']:
        if column not in frame.columns:
            continue
        for value in frame[column].tolist():
            if isinstance(value, list):
                reason_counter.update(str(item).strip() for item in value if str(item).strip())
            elif isinstance(value, str) and value.strip():
                reason_counter.update(part.strip() for part in value.split(',') if part.strip())

    scores = frame.apply(lambda row: compute_setup_quality_score(row, weights=weights), axis=1)
    high_quality = (scores >= 70.0).sum()

    def _bool_rate(column: str) -> float:
        series = frame.get(column, pd.Series([None] * len(frame), index=frame.index))
        valid = series.dropna()
        return round(float(valid.astype(bool).mean()) if not valid.empty else 0.0, 4)

    def _avg(column: str) -> float:
        series = pd.to_numeric(frame.get(column, pd.Series(dtype=float)), errors='coerce').dropna()
        return round(float(series.mean()) if not series.empty else 0.0, 4)

    pass_count = int(validation_passed.fillna(False).astype(bool).sum())
    fail_count = max(0, int(len(frame) - pass_count))
    return {
        'validation_pass_rate': round(safe_divide(pass_count, len(frame)), 4),
        'validation_fail_rate': round(safe_divide(fail_count, len(frame)), 4),
        'validation_pass_count': pass_count,
        'validation_fail_count': fail_count,
        'rejection_reason_breakdown': dict(reason_counter),
        'retest_confirmation_rate': _bool_rate('retest_confirmed'),
        'vwap_alignment_rate': _bool_rate('vwap_alignment'),
        'trend_alignment_rate': _bool_rate('trend_ok'),
        'volatility_pass_rate': _bool_rate('volatility_ok'),
        'chop_pass_rate': _bool_rate('chop_ok'),
        'average_zone_score': _avg('zone_score'),
        'average_freshness_score': _avg('freshness_score'),
        'average_move_away_score': _avg('move_away_score'),
        'average_rejection_strength': _avg('rejection_strength'),
        'average_structure_clarity': _avg('structure_clarity'),
        'high_quality_setup_rate': round(safe_divide(int(high_quality), len(frame)), 4),
        'setup_quality_scores': [round(float(value), 4) for value in scores.tolist()],
    }
