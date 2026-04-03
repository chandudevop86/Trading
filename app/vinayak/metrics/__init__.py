from __future__ import annotations

from typing import Any

from vinayak.metrics.dashboard_formatters import (
    build_drawdown_dataframe,
    build_equity_curve_dataframe,
    build_execution_quality_table,
    build_kpi_cards,
    build_pass_fail_table,
    build_rejection_reason_table,
    build_strategy_comparison_table,
    build_system_health_table,
)
from vinayak.metrics.execution_metrics import calculate_execution_metrics
from vinayak.metrics.performance_metrics import calculate_performance_metrics
from vinayak.metrics.readiness_report import DEFAULT_THRESHOLDS, generate_readiness_report
from vinayak.metrics.risk_metrics import calculate_risk_metrics
from vinayak.metrics.strategy_metrics import calculate_strategy_metrics
from vinayak.metrics.system_health_metrics import calculate_system_health_metrics
from vinayak.metrics.validation_metrics import DEFAULT_SETUP_QUALITY_WEIGHTS, calculate_validation_metrics


__all__ = [
    'DEFAULT_SETUP_QUALITY_WEIGHTS',
    'DEFAULT_THRESHOLDS',
    'build_drawdown_dataframe',
    'build_equity_curve_dataframe',
    'build_execution_quality_table',
    'build_kpi_cards',
    'build_pass_fail_table',
    'build_rejection_reason_table',
    'build_strategy_comparison_table',
    'build_system_health_table',
    'calculate_execution_metrics',
    'calculate_performance_metrics',
    'calculate_risk_metrics',
    'calculate_strategy_metrics',
    'calculate_system_health_metrics',
    'calculate_validation_metrics',
    'generate_readiness_report',
    'run_full_metrics_engine',
]


def run_full_metrics_engine(
    trades: Any,
    candles: Any = None,
    health_snapshots: Any = None,
    starting_capital: float = 100000.0,
    max_daily_loss: float | None = None,
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    performance = calculate_performance_metrics(trades)
    risk, equity_curve, drawdown_curve = calculate_risk_metrics(trades, starting_capital=starting_capital, max_daily_loss=max_daily_loss)
    execution = calculate_execution_metrics(trades)
    validation = calculate_validation_metrics(trades)
    strategy = calculate_strategy_metrics(trades)
    system_health = calculate_system_health_metrics(health_snapshots, candles=candles)
    readiness = generate_readiness_report(performance, risk, execution, validation, system_health, thresholds=thresholds).to_dict()
    results = {
        'performance': performance,
        'risk': risk,
        'execution': execution,
        'validation': validation,
        'strategy': strategy,
        'system_health': system_health,
        'readiness': readiness,
        'equity_curve': build_equity_curve_dataframe(equity_curve),
        'drawdown_curve': build_drawdown_dataframe(drawdown_curve),
    }
    results['dashboard'] = {
        'kpi_cards': build_kpi_cards(results),
        'pass_fail_table': build_pass_fail_table(readiness),
        'rejection_reason_table': build_rejection_reason_table(validation),
        'strategy_comparison_table': build_strategy_comparison_table(strategy),
        'equity_curve_dataframe': results['equity_curve'],
        'drawdown_dataframe': results['drawdown_curve'],
        'execution_quality_table': build_execution_quality_table(execution),
        'system_health_table': build_system_health_table(system_health),
    }
    return results
