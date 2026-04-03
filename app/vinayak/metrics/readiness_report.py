from __future__ import annotations

from typing import Any

from vinayak.metrics.schemas import ReadinessReport


DEFAULT_THRESHOLDS = {
    'expectancy_min': 0.0,
    'profit_factor_min': 1.30,
    'win_rate_min': 0.35,
    'max_drawdown_pct_max': 20.0,
    'duplicate_trade_rate_max': 0.0,
    'execution_success_rate_min': 0.95,
    'validation_pass_rate_min': 0.60,
    'high_quality_setup_rate_min': 0.50,
    'pipeline_health_rate_min': 0.95,
    'broker_health_rate_min': 0.95,
    'stale_data_detected_allowed': False,
}


def generate_readiness_report(
    performance_metrics: dict[str, Any],
    risk_metrics: dict[str, Any],
    execution_metrics: dict[str, Any],
    validation_metrics: dict[str, Any],
    system_health_metrics: dict[str, Any],
    thresholds: dict[str, Any] | None = None,
) -> ReadinessReport:
    cfg = {**DEFAULT_THRESHOLDS, **dict(thresholds or {})}
    score = 100.0
    passed: list[str] = []
    failed: list[str] = []
    warnings: list[str] = []
    actions: list[str] = []

    checks = [
        ('expectancy', float(performance_metrics.get('expectancy', 0.0)) > float(cfg['expectancy_min']), 'Expectancy must stay positive.', True),
        ('profit_factor', float(performance_metrics.get('profit_factor', 0.0)) >= float(cfg['profit_factor_min']), 'Profit factor is below target.', True),
        ('win_rate', float(performance_metrics.get('win_rate', 0.0)) >= float(cfg['win_rate_min']), 'Win rate is below target.', False),
        ('max_drawdown_pct', float(risk_metrics.get('max_drawdown_pct', 0.0)) <= float(cfg['max_drawdown_pct_max']), 'Drawdown is too high.', True),
        ('duplicate_trade_rate', float(execution_metrics.get('duplicate_trade_rate', 0.0)) <= float(cfg['duplicate_trade_rate_max']), 'Duplicate trade attempts detected.', True),
        ('execution_success_rate', float(execution_metrics.get('execution_success_rate', 0.0)) >= float(cfg['execution_success_rate_min']), 'Execution success rate is too low.', True),
        ('validation_pass_rate', float(validation_metrics.get('validation_pass_rate', 0.0)) >= float(cfg['validation_pass_rate_min']), 'Validation pass rate is too low.', False),
        ('high_quality_setup_rate', float(validation_metrics.get('high_quality_setup_rate', 0.0)) >= float(cfg['high_quality_setup_rate_min']), 'Too many weak setups are passing.', False),
        ('pipeline_health_rate', float(system_health_metrics.get('pipeline_health_rate', 0.0)) >= float(cfg['pipeline_health_rate_min']), 'Pipeline health is below target.', True),
        ('broker_health_rate', float(system_health_metrics.get('broker_health_rate', 0.0)) >= float(cfg['broker_health_rate_min']), 'Broker health is below target.', True),
        ('stale_data_detected', bool(system_health_metrics.get('stale_data_detected', False)) == bool(cfg['stale_data_detected_allowed']), 'Stale market data detected.', True),
    ]

    for name, ok, action, critical in checks:
        if ok:
            passed.append(name)
            continue
        failed.append(name)
        actions.append(action)
        score -= 18.0 if critical else 8.0
        if not critical:
            warnings.append(name)

    overall_status = 'PASS'
    if failed:
        overall_status = 'FAIL' if any(name not in warnings for name in failed) else 'WARNING'
    score = max(0.0, min(100.0, score))

    if overall_status == 'PASS':
        summary = 'This system is ready for paper-to-live promotion checks because performance, execution discipline, validation quality, and system health are inside configured thresholds.'
    else:
        reasons = ', '.join(failed[:3]) if failed else 'warning-level issues'
        summary = f'This system is NOT ready for live trading because {reasons} failed the configured thresholds.'

    return ReadinessReport(
        overall_status=overall_status,
        score=score,
        passed_checks=passed,
        failed_checks=failed,
        warnings=warnings,
        summary=summary,
        recommended_actions=actions,
    )
