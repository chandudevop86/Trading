from __future__ import annotations

from typing import Any

from vinayak.metrics import calculate_performance_metrics, calculate_risk_metrics, calculate_validation_metrics, calculate_execution_metrics, generate_readiness_report


def compute_trade_metrics(rows: Any) -> dict[str, Any]:
    performance = calculate_performance_metrics(rows)
    risk, _equity_curve, _drawdown_curve = calculate_risk_metrics(rows)
    validation = calculate_validation_metrics(rows)
    execution = calculate_execution_metrics(rows)
    duplicate_prevention_proven = float(execution.get('duplicate_trade_rate', 0.0)) == 0.0
    return {
        'total_trades': performance['total_trades'],
        'win_rate': round(float(performance['win_rate']) * 100.0, 2),
        'expectancy': round(float(performance['expectancy']), 4),
        'profit_factor': round(float(performance['profit_factor']), 4),
        'max_drawdown': round(float(risk['max_drawdown']), 4),
        'avg_r_multiple': round(float(performance['risk_reward_ratio']), 4),
        'validation_pass_rate': round(float(validation['validation_pass_rate']) * 100.0, 2),
        'rejection_reasons_count': dict(validation.get('rejection_reason_breakdown', {})),
        'duplicate_prevention_proven': duplicate_prevention_proven,
    }


def evaluate_production_readiness(metrics: dict[str, Any]) -> str:
    performance = {
        'expectancy': float(metrics.get('expectancy', 0.0) or 0.0),
        'profit_factor': float(metrics.get('profit_factor', 0.0) or 0.0),
        'win_rate': float(metrics.get('win_rate', 0.0) or 0.0) / 100.0,
    }
    risk = {
        'max_drawdown_pct': float(metrics.get('max_drawdown', 0.0) or 0.0),
    }
    execution = {
        'duplicate_trade_rate': 0.0 if bool(metrics.get('duplicate_prevention_proven', False)) else 1.0,
        'execution_success_rate': 1.0,
    }
    validation = {
        'validation_pass_rate': float(metrics.get('validation_pass_rate', 0.0) or 0.0) / 100.0,
        'high_quality_setup_rate': 0.5,
    }
    system_health = {
        'pipeline_health_rate': 1.0,
        'broker_health_rate': 1.0,
        'stale_data_detected': False,
    }
    report = generate_readiness_report(performance, risk, execution, validation, system_health).to_dict()
    if report['overall_status'] == 'PASS':
        return 'READY'
    if report['overall_status'] == 'WARNING':
        return 'PAPER_ONLY'
    return 'NOT_READY'


__all__ = ['compute_trade_metrics', 'evaluate_production_readiness']
