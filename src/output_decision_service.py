from __future__ import annotations

from typing import Any

import pandas as pd


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


def _normalize_flag(value: Any) -> bool:
    return str(value or '').strip().upper() == 'YES'


def _pillar_row(area: str, status: str, headline: str, reason: str, next_step: str) -> dict[str, str]:
    return {
        'area': area,
        'status': status,
        'headline': headline,
        'reason': reason,
        'next_step': next_step,
    }


def build_strategy_quality_ladder(summary: dict[str, object]) -> list[dict[str, str]]:
    total_trades = _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))
    retest_only_trade_pct = _safe_float(summary.get('retest_only_trade_pct'))
    vwap_pass_pct = _safe_float(summary.get('vwap_pass_pct'))
    session_pass_pct = _safe_float(summary.get('session_pass_pct'))
    avg_zone_gate_score = _safe_float(summary.get('avg_zone_gate_score'))
    validation_passed = _normalize_flag(summary.get('validation_passed', summary.get('deployment_ready', 'NO')))
    sample_window_passed = _normalize_flag(summary.get('sample_window_passed', 'NO'))
    deployment_ready = _normalize_flag(summary.get('deployment_ready', 'NO'))
    profit_factor = _safe_float(summary.get('profit_factor'))
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    max_drawdown_pct = _safe_float(summary.get('max_drawdown_pct'))
    drawdown_proven = _normalize_flag(summary.get('drawdown_proven', 'NO'))

    rows: list[dict[str, str]] = []

    if total_trades <= 0:
        rows.append(_pillar_row('Retest-Only Entry', 'WATCH', 'No trade sample yet', 'Retest discipline cannot be judged until the system produces a clean sample.', 'Run a backtest and confirm retest-only trades stay high.'))
    elif retest_only_trade_pct >= 90:
        rows.append(_pillar_row('Retest-Only Entry', 'PASS', 'Retest discipline is strong', f'{retest_only_trade_pct:.1f}% of trades came from retest-only entries.', 'Keep retest-only entry as the default execution policy.'))
    elif retest_only_trade_pct >= 70:
        rows.append(_pillar_row('Retest-Only Entry', 'WATCH', 'Retest discipline is mixed', f'{retest_only_trade_pct:.1f}% of trades came from retest-only entries, which is still softer than ideal.', 'Tighten entry filters until weak early entries are reduced further.'))
    else:
        rows.append(_pillar_row('Retest-Only Entry', 'FAIL', 'Retest discipline is weak', f'Only {retest_only_trade_pct:.1f}% of trades came from retest-only entries.', 'Block non-retest setups and reduce impulsive early entries.'))

    if total_trades <= 0:
        rows.append(_pillar_row('VWAP + Session Filter', 'WATCH', 'No context sample yet', 'VWAP and session filters need a live sample before they can be judged.', 'Run a backtest and confirm weak session trades are removed.'))
    else:
        context_score = min(vwap_pass_pct, session_pass_pct)
        if context_score >= 85:
            rows.append(_pillar_row('VWAP + Session Filter', 'PASS', 'Trade context is clean', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%.', 'Keep morning-session and VWAP filters active.'))
        elif context_score >= 65:
            rows.append(_pillar_row('VWAP + Session Filter', 'WATCH', 'Trade context is improving', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%, but weak context trades still appear.', 'Tighten session and VWAP gates until weak context trades fall further.'))
        else:
            rows.append(_pillar_row('VWAP + Session Filter', 'FAIL', 'Trade context is weak', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%.', 'Remove more low-quality session trades and require stronger VWAP alignment.'))

    if total_trades <= 0:
        rows.append(_pillar_row('Zone Scoring', 'WATCH', 'No setup-quality sample yet', 'Zone quality needs a trade sample before the scoring filter can be judged.', 'Run a backtest and review average zone score.'))
    elif avg_zone_gate_score >= 5.0:
        rows.append(_pillar_row('Zone Scoring', 'PASS', 'Setup quality is strong', f'Average zone score is {avg_zone_gate_score:.2f}, which is above the current floor.', 'Keep weak zones filtered out before entry evaluation.'))
    elif avg_zone_gate_score >= 4.0:
        rows.append(_pillar_row('Zone Scoring', 'WATCH', 'Setup quality is acceptable but soft', f'Average zone score is {avg_zone_gate_score:.2f}, so weaker setups are still leaking through.', 'Raise the zone floor or tighten reaction quality.'))
    else:
        rows.append(_pillar_row('Zone Scoring', 'FAIL', 'Setup quality is weak', f'Average zone score is only {avg_zone_gate_score:.2f}.', 'Filter weak zones earlier and require stronger reaction quality.'))

    if total_trades <= 0:
        rows.append(_pillar_row('Validation System', 'WATCH', 'No validated sample yet', 'The system has not produced a sample large enough to prove readiness.', 'Run a 150-200 trade backtest before making any deployment decision.'))
    elif deployment_ready and validation_passed and sample_window_passed and expectancy > 0 and profit_factor > 1.3 and 0 <= max_drawdown_pct <= 12 and drawdown_proven:
        rows.append(_pillar_row('Validation System', 'PASS', 'The system is proving itself', 'Expectancy, profit factor, drawdown, and the validation window are all passing.', 'Keep validating on rolling samples before any live expansion.'))
    elif validation_passed or sample_window_passed:
        rows.append(_pillar_row('Validation System', 'WATCH', 'Validation is improving but not complete', f'Profit factor is {profit_factor:.2f}, expectancy is {expectancy:.2f}, and drawdown is {max_drawdown_pct:.2f}%.', 'Clear every blocker before considering live trading.'))
    else:
        rows.append(_pillar_row('Validation System', 'FAIL', 'The system is not proven yet', f'Profit factor is {profit_factor:.2f}, expectancy is {expectancy:.2f}, and drawdown proof is {"present" if drawdown_proven else "missing"}.', 'Stay in backtest or paper mode until the full validation stack passes.'))

    return rows


def build_quality_ladder_frame(summary: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(build_strategy_quality_ladder(summary))


def build_quality_ladder_summary(summary: dict[str, object]) -> str:
    rows = build_strategy_quality_ladder(summary)
    failed = [row for row in rows if row['status'] == 'FAIL']
    watched = [row for row in rows if row['status'] == 'WATCH']
    if failed:
        reasons = ', '.join(row['area'].lower() for row in failed[:2])
        return f'This strategy is not ready for live trading yet because {reasons} still need work.'
    if watched:
        reasons = ', '.join(row['area'].lower() for row in watched[:2])
        return f'This strategy is improving, but {reasons} still need to become more consistent.'
    return 'This strategy is behaving well across entry discipline, trade context, setup quality, and validation.'
