from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from vinayak.analytics.metrics import compute_trade_metrics
from vinayak.metrics import run_full_metrics_engine


DEFAULT_READINESS_THRESHOLDS = {
    'min_trades': 100,
    'min_expectancy': 0.0,
    'min_profit_factor': 1.3,
    'max_drawdown': 10.0,
    'min_validation_pass_rate': 55.0,
    'ready_profit_factor': 1.6,
    'ready_max_drawdown': 6.0,
}


def _coerce_frame(rows: Any) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows.copy()
    return pd.DataFrame(rows or [])


def summarize_validation_failures(rejects_df: Any) -> dict[str, int]:
    frame = _coerce_frame(rejects_df)
    counter: Counter[str] = Counter()
    if frame.empty:
        return {}
    for column in ('reasons', 'validation_reasons', 'reason_codes', 'blocked_reason', 'rejection_reason'):
        if column not in frame.columns:
            continue
        for value in frame[column].tolist():
            if isinstance(value, list):
                counter.update(str(item) for item in value if str(item).strip())
            elif isinstance(value, str) and value.strip():
                counter.update(part.strip() for part in value.split(',') if part.strip())
    return dict(counter)


def evaluate_readiness(trades_df: Any, rejects_df: Any, config: dict[str, Any] | None = None) -> dict[str, Any]:
    trades = _coerce_frame(trades_df)
    rejects = _coerce_frame(rejects_df)
    engine = run_full_metrics_engine(trades)
    metrics = compute_trade_metrics(trades)
    readiness = engine['readiness']
    top_rejection_reasons = summarize_validation_failures(rejects)

    total_candidates = int(len(trades) + len(rejects))
    total_rejected = int(len(rejects))
    total_passed = int(engine['validation'].get('validation_pass_count', len(trades)))
    total_executed = int(engine['execution'].get('signals_executed', len(trades)))
    validation_pass_rate = round((total_passed / total_candidates * 100.0), 2) if total_candidates > 0 else 0.0

    verdict_map = {'PASS': 'READY', 'WARNING': 'PAPER_ONLY', 'FAIL': 'NOT_READY'}
    verdict = verdict_map.get(readiness.get('overall_status', 'FAIL'), 'NOT_READY')
    reasons = list(readiness.get('failed_checks', [])) or list(readiness.get('warnings', []))
    threshold_status = {
        'min_trades': int(metrics.get('total_trades', 0)) >= int(DEFAULT_READINESS_THRESHOLDS['min_trades']),
        'min_expectancy': float(metrics.get('expectancy', 0.0)) > float(DEFAULT_READINESS_THRESHOLDS['min_expectancy']),
        'min_profit_factor': float(metrics.get('profit_factor', 0.0)) >= float(DEFAULT_READINESS_THRESHOLDS['min_profit_factor']),
        'max_drawdown': float(metrics.get('max_drawdown', 0.0)) <= float(DEFAULT_READINESS_THRESHOLDS['max_drawdown']),
        'min_validation_pass_rate': validation_pass_rate >= float(DEFAULT_READINESS_THRESHOLDS['min_validation_pass_rate']),
        'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
    }

    return {
        'verdict': verdict,
        'readiness_decision': verdict,
        'thresholds': {**DEFAULT_READINESS_THRESHOLDS, **dict(config or {})},
        'threshold_status': threshold_status,
        'reasons': reasons,
        'not_ready_reasons': reasons,
        'failure_counts': {name: 1 for name in reasons},
        'totals': {
            'total_candidates': total_candidates,
            'total_passed': total_passed,
            'total_rejected': total_rejected,
            'total_executed': total_executed,
        },
        'metrics': {
            'validation_pass_rate': validation_pass_rate,
            'win_rate': metrics.get('win_rate', 0.0),
            'expectancy': metrics.get('expectancy', 0.0),
            'profit_factor': metrics.get('profit_factor', 0.0),
            'max_drawdown': metrics.get('max_drawdown', 0.0),
            'avg_r_multiple': metrics.get('avg_r_multiple', 0.0),
            'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
        },
        'top_rejection_reasons': top_rejection_reasons,
        'validation_failure_summary': top_rejection_reasons,
        'total_candidates': total_candidates,
        'total_passed': total_passed,
        'total_rejected': total_rejected,
        'total_executed': total_executed,
        'validation_pass_rate': validation_pass_rate,
        'win_rate': metrics.get('win_rate', 0.0),
        'expectancy': metrics.get('expectancy', 0.0),
        'profit_factor': metrics.get('profit_factor', 0.0),
        'max_drawdown': metrics.get('max_drawdown', 0.0),
        'avg_r_multiple': metrics.get('avg_r_multiple', 0.0),
        'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
    }


__all__ = ['DEFAULT_READINESS_THRESHOLDS', 'evaluate_readiness', 'summarize_validation_failures']
