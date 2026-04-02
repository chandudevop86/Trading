from __future__ import annotations

from typing import Any

import pandas as pd


def build_kpi_cards(results: dict[str, Any]) -> list[dict[str, Any]]:
    performance = results.get('performance', {})
    risk = results.get('risk', {})
    execution = results.get('execution', {})
    validation = results.get('validation', {})
    readiness = results.get('readiness', {})
    return [
        {'label': 'Win Rate', 'value': round(float(performance.get('win_rate', 0.0)) * 100.0, 2), 'unit': '%'},
        {'label': 'Expectancy', 'value': round(float(performance.get('expectancy', 0.0)), 4), 'unit': ''},
        {'label': 'Profit Factor', 'value': round(float(performance.get('profit_factor', 0.0)), 4), 'unit': ''},
        {'label': 'Max Drawdown %', 'value': round(float(risk.get('max_drawdown_pct', 0.0)), 2), 'unit': '%'},
        {'label': 'Execution Success %', 'value': round(float(execution.get('execution_success_rate', 0.0)) * 100.0, 2), 'unit': '%'},
        {'label': 'Validation Pass %', 'value': round(float(validation.get('validation_pass_rate', 0.0)) * 100.0, 2), 'unit': '%'},
        {'label': 'Setup Quality %', 'value': round(float(validation.get('high_quality_setup_rate', 0.0)) * 100.0, 2), 'unit': '%'},
        {'label': 'Overall Readiness Status', 'value': readiness.get('overall_status', 'FAIL'), 'unit': ''},
    ]


def build_pass_fail_table(readiness: dict[str, Any]) -> pd.DataFrame:
    rows = [{'check': item, 'status': 'PASS'} for item in readiness.get('passed_checks', [])]
    rows.extend({'check': item, 'status': 'FAIL'} for item in readiness.get('failed_checks', []))
    return pd.DataFrame(rows)


def build_rejection_reason_table(validation_metrics: dict[str, Any]) -> pd.DataFrame:
    breakdown = validation_metrics.get('rejection_reason_breakdown', {}) or {}
    rows = [{'reason': key, 'count': value} for key, value in breakdown.items()]
    return pd.DataFrame(rows).sort_values('count', ascending=False).reset_index(drop=True) if rows else pd.DataFrame(columns=['reason', 'count'])


def build_strategy_comparison_table(strategy_metrics: dict[str, Any]) -> pd.DataFrame:
    rows = strategy_metrics.get('comparison_rows', []) or []
    return pd.DataFrame(rows)


def build_equity_curve_dataframe(equity_curve: Any) -> pd.DataFrame:
    if isinstance(equity_curve, pd.DataFrame):
        return equity_curve.copy()
    return pd.DataFrame(equity_curve or [])


def build_drawdown_dataframe(drawdown_curve: Any) -> pd.DataFrame:
    if isinstance(drawdown_curve, pd.DataFrame):
        return drawdown_curve.copy()
    return pd.DataFrame(drawdown_curve or [])


def build_execution_quality_table(execution_metrics: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame([
        {'metric': key, 'value': value} for key, value in execution_metrics.items()
    ])


def build_system_health_table(system_health_metrics: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame([
        {'metric': key, 'value': value} for key, value in system_health_metrics.items()
    ])

