from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from vinayak.analytics.metrics import compute_trade_metrics
from vinayak.metrics import run_full_metrics_engine
from vinayak.metrics.utils import closed_trades_only, coerce_trade_records
from vinayak.validation.trade_evaluation import build_trade_evaluation_summary


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in {'1', 'true', 'yes', 'y', 'pass', 'passed', 'ok', 'on'}:
        return True
    if lowered in {'0', 'false', 'no', 'n', 'fail', 'failed', 'off'}:
        return False
    return default


def _reason_count(value: Any) -> int:
    if isinstance(value, list):
        return sum(1 for item in value if str(item).strip())
    if isinstance(value, str):
        return sum(1 for item in value.split(',') if item.strip())
    return 0


def _clean_trade_frame(rows: Any) -> pd.DataFrame:
    frame = coerce_trade_records(rows)
    if frame.empty:
        return frame.copy()

    closed = closed_trades_only(frame)
    validation_status = closed.get('validation_status', pd.Series([''] * len(closed), index=closed.index)).astype(str).str.upper()
    validation_passed = closed.get('validation_passed', pd.Series([None] * len(closed), index=closed.index))
    validation_passed_bool = validation_passed.apply(lambda value: _safe_bool(value, False))
    validation_ok = validation_status.eq('PASS')
    validation_ok = validation_ok | validation_passed_bool
    validation_ok = validation_ok | ((validation_status.eq('')) & validation_passed.isna())

    rejection_reason = closed.get('rejection_reason', pd.Series([''] * len(closed), index=closed.index)).fillna('').astype(str).str.strip()
    rejection_count = closed.get('validation_reasons', pd.Series([[]] * len(closed), index=closed.index)).apply(_reason_count)
    execution_status = closed.get('status', pd.Series([''] * len(closed), index=closed.index)).fillna('').astype(str).str.upper()
    duplicate_blocked = closed.get('duplicate_blocked', pd.Series([False] * len(closed), index=closed.index)).fillna(False).astype(bool)
    strict_validation_score = pd.to_numeric(closed.get('strict_validation_score', pd.Series([None] * len(closed), index=closed.index)), errors='coerce')

    execution_ok = ~execution_status.isin({'REJECTED', 'BLOCKED', 'ERROR', 'CANCELLED'})
    rejection_ok = rejection_reason.eq('') & rejection_count.eq(0)
    strict_ok = strict_validation_score.isna() | strict_validation_score.ge(7)

    return closed.loc[validation_ok & execution_ok & rejection_ok & ~duplicate_blocked & strict_ok].copy().reset_index(drop=True)


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


def evaluate_readiness(trades_df: Any, rejects_df: Any, config: dict[str, Any] | None = None, *, trade_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = {**DEFAULT_READINESS_THRESHOLDS, **dict(config or {})}
    trades = _coerce_frame(trades_df)
    rejects = _coerce_frame(rejects_df)
    clean_trades = _clean_trade_frame(trades)

    engine = run_full_metrics_engine(clean_trades)
    metrics = compute_trade_metrics(clean_trades)
    readiness = engine['readiness']
    top_rejection_reasons = summarize_validation_failures(rejects)
    resolved_trade_summary = dict(trade_summary or {}) if isinstance(trade_summary, dict) else {}
    if not resolved_trade_summary and not clean_trades.empty:
        resolved_trade_summary = build_trade_evaluation_summary(clean_trades.to_dict(orient='records'), strategy_name='PHASE3_VALIDATION')

    total_candidates = int(len(trades) + len(rejects))
    total_rejected = int(len(rejects))
    if not trades.empty and 'validation_status' in trades.columns:
        total_passed = int(len(trades[trades['validation_status'].astype(str).str.upper() == 'PASS']))
    else:
        total_passed = int(len(clean_trades))
    total_executed = int(len(clean_trades))
    validation_pass_rate = round((total_passed / total_candidates * 100.0), 2) if total_candidates > 0 else 0.0

    go_live_status = str(resolved_trade_summary.get('go_live_status', '') or '').upper()
    pass_fail_status = str(resolved_trade_summary.get('pass_fail_status', readiness.get('overall_status', 'FAIL')) or 'FAIL').upper()
    oos_status = str(resolved_trade_summary.get('oos_status', 'OOS_NEED_MORE_DATA') or 'OOS_NEED_MORE_DATA').upper()
    overfit_risk_score = float(resolved_trade_summary.get('overfit_risk_score', 10.0) or 10.0)
    regime_consistency_score = float(resolved_trade_summary.get('regime_consistency_score', 0.0) or 0.0)

    hard_gate_reasons: list[str] = []
    soft_gate_reasons: list[str] = []
    if oos_status == 'OOS_FAIL':
        hard_gate_reasons.append('OOS_FAIL')
    elif oos_status in {'OOS_BORDERLINE', 'OOS_NEED_MORE_DATA'}:
        soft_gate_reasons.append(oos_status)
    if overfit_risk_score >= 7.5:
        hard_gate_reasons.append('OVERFIT_RISK_HIGH')
    elif overfit_risk_score >= 5.0:
        soft_gate_reasons.append('OVERFIT_RISK_MODERATE')
    if regime_consistency_score < 3.5:
        hard_gate_reasons.append('REGIME_CONSISTENCY_TOO_LOW')
    elif regime_consistency_score < 5.5:
        soft_gate_reasons.append('REGIME_CONSISTENCY_BORDERLINE')

    if hard_gate_reasons:
        verdict = 'NOT_READY'
    elif go_live_status == 'LIVE_READY' and not soft_gate_reasons:
        verdict = 'READY'
    elif pass_fail_status in {'PASS', 'NEED_MORE_DATA', 'PAPER_ONLY'} or readiness.get('overall_status') == 'WARNING' or soft_gate_reasons:
        verdict = 'PAPER_ONLY'
    else:
        verdict = 'NOT_READY'

    reasons = list(resolved_trade_summary.get('pass_fail_reasons', [])) or list(readiness.get('failed_checks', [])) or list(readiness.get('warnings', []))
    reasons = list(dict.fromkeys(reasons + hard_gate_reasons + soft_gate_reasons))
    threshold_status = {
        'min_trades': int(metrics.get('total_trades', 0)) >= int(cfg['min_trades']),
        'min_expectancy': float(metrics.get('expectancy', 0.0)) > float(cfg['min_expectancy']),
        'min_profit_factor': float(metrics.get('profit_factor', 0.0)) >= float(cfg['min_profit_factor']),
        'max_drawdown': float(metrics.get('max_drawdown', 0.0)) <= float(cfg['max_drawdown']),
        'min_validation_pass_rate': validation_pass_rate >= float(cfg['min_validation_pass_rate']),
        'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
        'oos_pass': oos_status == 'OOS_PASS',
        'overfit_risk_ok': overfit_risk_score < 5.0,
        'regime_consistency_ok': regime_consistency_score >= 5.5,
    }

    edge_report = {
        'clean_trade_count': int(resolved_trade_summary.get('clean_trades', metrics.get('total_trades', 0))),
        'expectancy_per_trade': resolved_trade_summary.get('expectancy_per_trade', metrics.get('expectancy', 0.0)),
        'expectancy_r': resolved_trade_summary.get('expectancy_r', 0.0),
        'profit_factor': resolved_trade_summary.get('profit_factor', metrics.get('profit_factor', 0.0)),
        'max_drawdown_pct': resolved_trade_summary.get('max_drawdown_pct', metrics.get('max_drawdown', 0.0)),
        'win_rate': resolved_trade_summary.get('win_rate', metrics.get('win_rate', 0.0)),
        'expectancy_stability_score': resolved_trade_summary.get('expectancy_stability_score', 0.0),
        'profit_factor_stability_score': resolved_trade_summary.get('profit_factor_stability_score', 0.0),
        'trade_data_quality_score': resolved_trade_summary.get('trade_data_quality_score', 0.0),
        'go_live_status': resolved_trade_summary.get('go_live_status', 'PAPER_ONLY'),
        'promotion_status': resolved_trade_summary.get('promotion_status', 'RESEARCH_ONLY'),
        'regime_consistency_score': resolved_trade_summary.get('regime_consistency_score', 0.0),
        'regime_consistency_label': resolved_trade_summary.get('regime_consistency_label', 'DEPENDENT'),
        'dominant_regime': resolved_trade_summary.get('dominant_regime', 'none'),
        'weakest_regime': resolved_trade_summary.get('weakest_regime', 'none'),
        'regime_profit_concentration': resolved_trade_summary.get('regime_profit_concentration', 0.0),
        'walkforward_windows': resolved_trade_summary.get('walkforward_windows', 0),
        'walkforward_consistency_score': resolved_trade_summary.get('walkforward_consistency_score', 0.0),
        'oos_status': resolved_trade_summary.get('oos_status', 'OOS_NEED_MORE_DATA'),
        'oos_pass_rate': resolved_trade_summary.get('oos_pass_rate', 0.0),
        'overfit_risk_score': resolved_trade_summary.get('overfit_risk_score', 10.0),
        'overfit_risk_label': resolved_trade_summary.get('overfit_risk_label', 'HIGH'),
        'edge_decay_score': resolved_trade_summary.get('edge_decay_score', 10.0),
    }

    regime_report = {
        'regime_consistency_score': resolved_trade_summary.get('regime_consistency_score', 0.0),
        'regime_consistency_label': resolved_trade_summary.get('regime_consistency_label', 'DEPENDENT'),
        'dominant_regime': resolved_trade_summary.get('dominant_regime', 'none'),
        'weakest_regime': resolved_trade_summary.get('weakest_regime', 'none'),
        'regime_profit_concentration': resolved_trade_summary.get('regime_profit_concentration', 0.0),
        'regime_metrics': resolved_trade_summary.get('regime_metrics', {}),
    }

    walkforward_report = {
        'walkforward_windows': resolved_trade_summary.get('walkforward_windows', 0),
        'walkforward_consistency_score': resolved_trade_summary.get('walkforward_consistency_score', 0.0),
        'oos_status': resolved_trade_summary.get('oos_status', 'OOS_NEED_MORE_DATA'),
        'oos_pass_rate': resolved_trade_summary.get('oos_pass_rate', 0.0),
        'oos_reasons': resolved_trade_summary.get('oos_reasons', []),
        'oos_warnings': resolved_trade_summary.get('oos_warnings', []),
        'overfit_risk_score': resolved_trade_summary.get('overfit_risk_score', 10.0),
        'overfit_risk_label': resolved_trade_summary.get('overfit_risk_label', 'HIGH'),
        'edge_decay_score': resolved_trade_summary.get('edge_decay_score', 10.0),
    }

    return {
        'verdict': verdict,
        'readiness_decision': verdict,
        'thresholds': dict(cfg),
        'threshold_status': threshold_status,
        'reasons': reasons,
        'not_ready_reasons': reasons,
        'failure_counts': {name: 1 for name in reasons},
        'totals': {
            'total_candidates': total_candidates,
            'total_passed': total_passed,
            'total_rejected': total_rejected,
            'total_executed': total_executed,
            'clean_trade_count': int(len(clean_trades)),
        },
        'metrics': {
            'validation_pass_rate': validation_pass_rate,
            'win_rate': metrics.get('win_rate', 0.0),
            'expectancy': metrics.get('expectancy', 0.0),
            'profit_factor': metrics.get('profit_factor', 0.0),
            'max_drawdown': metrics.get('max_drawdown', 0.0),
            'avg_r_multiple': metrics.get('avg_r_multiple', 0.0),
            'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
            'clean_trade_count': int(len(clean_trades)),
        },
        'top_rejection_reasons': top_rejection_reasons,
        'validation_failure_summary': top_rejection_reasons,
        'total_candidates': total_candidates,
        'total_passed': total_passed,
        'total_rejected': total_rejected,
        'total_executed': total_executed,
        'clean_trade_count': int(len(clean_trades)),
        'validation_pass_rate': validation_pass_rate,
        'win_rate': metrics.get('win_rate', 0.0),
        'expectancy': metrics.get('expectancy', 0.0),
        'profit_factor': metrics.get('profit_factor', 0.0),
        'max_drawdown': metrics.get('max_drawdown', 0.0),
        'avg_r_multiple': metrics.get('avg_r_multiple', 0.0),
        'duplicate_prevention_proven': bool(metrics.get('duplicate_prevention_proven', False)),
        'clean_trade_metrics_only': True,
        'edge_proof_status': edge_report['go_live_status'],
        'readiness_summary': resolved_trade_summary.get('paper_readiness_summary', readiness.get('summary', '')),
        'paper_readiness_summary': resolved_trade_summary.get('paper_readiness_summary', ''),
        'go_live_status': resolved_trade_summary.get('go_live_status', 'PAPER_ONLY'),
        'promotion_status': resolved_trade_summary.get('promotion_status', 'RESEARCH_ONLY'),
        'edge_report': edge_report,
        'regime_report': regime_report,
        'walkforward_report': walkforward_report,
        'regime_consistency_score': regime_report['regime_consistency_score'],
        'regime_consistency_label': regime_report['regime_consistency_label'],
        'dominant_regime': regime_report['dominant_regime'],
        'weakest_regime': regime_report['weakest_regime'],
        'walkforward_windows': walkforward_report['walkforward_windows'],
        'oos_status': walkforward_report['oos_status'],
        'oos_pass_rate': walkforward_report['oos_pass_rate'],
        'overfit_risk_score': walkforward_report['overfit_risk_score'],
        'overfit_risk_label': walkforward_report['overfit_risk_label'],
    }


__all__ = ['DEFAULT_READINESS_THRESHOLDS', 'evaluate_readiness', 'summarize_validation_failures']





