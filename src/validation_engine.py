from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class GoLiveValidationConfig:
    min_completed_trades: int = 100
    preferred_completed_trades: int = 200
    min_profit_factor: float = 1.3
    min_expectancy_per_trade: float = 0.0
    max_drawdown_pct: float = 12.0
    max_duplicate_trades: int = 0
    max_invalid_trades: int = 0
    max_execution_errors: int = 0
    max_material_win_rate_gap_pct: float = 8.0
    max_material_profit_factor_gap: float = 0.2
    max_material_expectancy_gap_ratio: float = 0.5
    min_paper_trades: int = 20
    rr_win_rate_buffer_pct: float = 4.0
    absolute_min_win_rate_pct: float = 35.0
    require_cooldown_controls: bool = True
    market_open: str = '09:15'
    market_close: str = '15:30'
    market_label: str = 'NSE_NIFTY_INTRADAY'


FAIL = 'FAIL'
CONDITIONAL_PASS = 'CONDITIONAL PASS'
PASS = 'PASS'


MALFORMED_REASONS = {
    'INVALID_SIDE',
    'INVALID_TIMESTAMP',
    'INVALID_ENTRY',
    'INVALID_STOP_LOSS',
    'INVALID_TARGET',
    'BUY_LEVELS_NOT_ORDERED',
    'SELL_LEVELS_NOT_ORDERED',
    'MISSING_PRICE',
    'MISSING_QUANTITY',
    'MISSING_TIMESTAMP',
    'MISSING_STOP_LOSS',
    'MISSING_TARGET',
    'INVALID_TRADE_LEVELS',
}


def default_nifty_intraday_go_live_config() -> GoLiveValidationConfig:
    """Strict go-live configuration for Nifty intraday validation."""
    return GoLiveValidationConfig()


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
    return str(value or '').strip().upper() in {'YES', 'TRUE', '1', 'PASS'}


def required_win_rate_pct(rr_ratio: float, *, buffer_pct: float = 4.0, absolute_floor_pct: float = 35.0) -> float:
    reward_ratio = max(float(rr_ratio or 0.0), 0.5)
    breakeven = 100.0 / (reward_ratio + 1.0)
    return round(max(float(absolute_floor_pct), breakeven + float(buffer_pct)), 2)


def _summary_metrics(summary: dict[str, Any], config: GoLiveValidationConfig) -> dict[str, Any]:
    avg_rr = _safe_float(summary.get('avg_rr', summary.get('rr_ratio', 0.0)), 0.0)
    if avg_rr <= 0:
        avg_rr = 2.0
    duplicate_trade_count = _safe_int(
        summary.get('duplicate_trade_count', summary.get('duplicate_rejections', summary.get('duplicate_count', 0)))
    )
    invalid_trade_count = _safe_int(
        summary.get('invalid_trade_count', summary.get('malformed_trade_count', summary.get('invalid_malformed_trade_count', 0)))
    )
    execution_error_count = _safe_int(
        summary.get('execution_error_count', summary.get('execution_crash_count', summary.get('error_count', 0)))
    )
    total_trades = _safe_int(summary.get('completed_trades', summary.get('total_trades', summary.get('trades', 0))))
    cooldown_controls_enforced = _normalize_flag(
        summary.get('cooldown_controls_enforced', summary.get('duplicate_controls_enforced', 'NO'))
    )
    return {
        'strategy': str(summary.get('strategy', summary.get('strategy_name', 'SYSTEM')) or 'SYSTEM'),
        'completed_trades': total_trades,
        'win_rate_pct': _safe_float(summary.get('win_rate_pct', summary.get('win_rate', 0.0))),
        'profit_factor': float('inf') if str(summary.get('profit_factor', '')).strip().lower() == 'inf' else _safe_float(summary.get('profit_factor', 0.0)),
        'expectancy_per_trade': _safe_float(summary.get('expectancy_per_trade', summary.get('avg_pnl', 0.0))),
        'max_drawdown_pct': _safe_float(summary.get('max_drawdown_pct', 0.0)),
        'avg_rr': avg_rr,
        'duplicate_trade_count': duplicate_trade_count,
        'invalid_trade_count': invalid_trade_count,
        'execution_error_count': execution_error_count,
        'cooldown_controls_enforced': cooldown_controls_enforced,
        'market_session': str(summary.get('market_session', f'{config.market_open}-{config.market_close}') or f'{config.market_open}-{config.market_close}'),
    }


def _rule_result(name: str, status: str, actual: Any, threshold: Any, message: str, source: str) -> dict[str, Any]:
    return {
        'rule': name,
        'status': status,
        'actual': actual,
        'threshold': threshold,
        'message': message,
        'source': source,
    }


def evaluate_go_live(
    backtest_summary: dict[str, Any],
    paper_summary: dict[str, Any],
    config: GoLiveValidationConfig | None = None,
) -> dict[str, Any]:
    """Evaluate strict go-live readiness for Nifty intraday deployment."""
    cfg = config or default_nifty_intraday_go_live_config()
    backtest = _summary_metrics(backtest_summary, cfg)
    paper = _summary_metrics(paper_summary, cfg)
    required_win_rate = required_win_rate_pct(
        max(backtest['avg_rr'], paper['avg_rr']),
        buffer_pct=cfg.rr_win_rate_buffer_pct,
        absolute_floor_pct=cfg.absolute_min_win_rate_pct,
    )
    breakeven_win_rate = required_win_rate_pct(
        max(backtest['avg_rr'], paper['avg_rr']),
        buffer_pct=0.0,
        absolute_floor_pct=0.0,
    )

    checks: list[dict[str, Any]] = []
    blocking_reasons: list[str] = []
    warnings: list[str] = []

    expected_session = f'{cfg.market_open}-{cfg.market_close}'
    if backtest['market_session'] != expected_session or paper['market_session'] != expected_session:
        checks.append(_rule_result('session_context', FAIL, {'backtest': backtest['market_session'], 'paper': paper['market_session']}, expected_session, 'Go-live validation is calibrated only for normal NSE hours 09:15-15:30.', 'system'))
        blocking_reasons.append('session_context_mismatch')
    else:
        checks.append(_rule_result('session_context', PASS, {'backtest': backtest['market_session'], 'paper': paper['market_session']}, expected_session, 'Validation context matches normal NSE market hours 09:15-15:30.', 'system'))

    if backtest['completed_trades'] < cfg.min_completed_trades:
        checks.append(_rule_result('minimum_trade_confidence', FAIL, backtest['completed_trades'], cfg.min_completed_trades, 'Need at least 100 completed backtest trades before live use.', 'backtest'))
        blocking_reasons.append('backtest_trade_count_below_minimum')
    elif backtest['completed_trades'] < cfg.preferred_completed_trades:
        checks.append(_rule_result('preferred_trade_depth', CONDITIONAL_PASS, backtest['completed_trades'], cfg.preferred_completed_trades, 'Trade count is usable but below the preferred 200-trade sample.', 'backtest'))
        warnings.append('backtest_trade_count_below_preferred')
    else:
        checks.append(_rule_result('trade_sample_size', PASS, backtest['completed_trades'], cfg.preferred_completed_trades, 'Trade sample size is sufficient for stricter Nifty intraday review.', 'backtest'))

    if backtest['expectancy_per_trade'] <= cfg.min_expectancy_per_trade:
        checks.append(_rule_result('backtest_expectancy', FAIL, backtest['expectancy_per_trade'], f'>{cfg.min_expectancy_per_trade}', 'Backtest expectancy must be positive.', 'backtest'))
        blocking_reasons.append('backtest_expectancy_not_positive')
    else:
        checks.append(_rule_result('backtest_expectancy', PASS, backtest['expectancy_per_trade'], f'>{cfg.min_expectancy_per_trade}', 'Backtest expectancy is positive.', 'backtest'))

    if paper['completed_trades'] < cfg.min_paper_trades:
        checks.append(_rule_result('paper_trade_count', CONDITIONAL_PASS, paper['completed_trades'], cfg.min_paper_trades, 'Paper trading sample is too small for live approval.', 'paper'))
        warnings.append('paper_trade_count_below_minimum')
    elif paper['expectancy_per_trade'] <= cfg.min_expectancy_per_trade:
        checks.append(_rule_result('paper_expectancy', FAIL, paper['expectancy_per_trade'], f'>{cfg.min_expectancy_per_trade}', 'Paper expectancy must stay positive.', 'paper'))
        blocking_reasons.append('paper_expectancy_not_positive')
    else:
        checks.append(_rule_result('paper_expectancy', PASS, paper['expectancy_per_trade'], f'>{cfg.min_expectancy_per_trade}', 'Paper expectancy is positive.', 'paper'))

    for source_name, metrics in (('backtest', backtest), ('paper', paper)):
        if metrics['profit_factor'] <= cfg.min_profit_factor:
            status = FAIL if source_name == 'backtest' else CONDITIONAL_PASS
            checks.append(_rule_result(f'{source_name}_profit_factor', status, metrics['profit_factor'], f'>{cfg.min_profit_factor}', f'{source_name.title()} profit factor is too weak for go-live.', source_name))
            if status == FAIL:
                blocking_reasons.append(f'{source_name}_profit_factor_below_threshold')
            else:
                warnings.append(f'{source_name}_profit_factor_below_threshold')
        else:
            checks.append(_rule_result(f'{source_name}_profit_factor', PASS, metrics['profit_factor'], f'>{cfg.min_profit_factor}', f'{source_name.title()} profit factor passes the threshold.', source_name))

        if metrics['max_drawdown_pct'] > cfg.max_drawdown_pct:
            checks.append(_rule_result(f'{source_name}_drawdown', FAIL, metrics['max_drawdown_pct'], f'<={cfg.max_drawdown_pct}', f'{source_name.title()} drawdown exceeds the strict intraday limit.', source_name))
            blocking_reasons.append(f'{source_name}_drawdown_above_limit')
        else:
            checks.append(_rule_result(f'{source_name}_drawdown', PASS, metrics['max_drawdown_pct'], f'<={cfg.max_drawdown_pct}', f'{source_name.title()} drawdown stays within the configured limit.', source_name))

        if metrics['duplicate_trade_count'] > cfg.max_duplicate_trades:
            checks.append(_rule_result(f'{source_name}_duplicates', FAIL, metrics['duplicate_trade_count'], cfg.max_duplicate_trades, f'{source_name.title()} duplicate trades must be zero.', source_name))
            blocking_reasons.append(f'{source_name}_duplicate_trades_present')
        else:
            checks.append(_rule_result(f'{source_name}_duplicates', PASS, metrics['duplicate_trade_count'], cfg.max_duplicate_trades, f'{source_name.title()} duplicate trade count is clean.', source_name))

        if metrics['invalid_trade_count'] > cfg.max_invalid_trades:
            checks.append(_rule_result(f'{source_name}_invalid_trades', FAIL, metrics['invalid_trade_count'], cfg.max_invalid_trades, f'{source_name.title()} invalid or malformed trade count must be zero.', source_name))
            blocking_reasons.append(f'{source_name}_invalid_trades_present')
        else:
            checks.append(_rule_result(f'{source_name}_invalid_trades', PASS, metrics['invalid_trade_count'], cfg.max_invalid_trades, f'{source_name.title()} invalid trade count is clean.', source_name))

    if paper['execution_error_count'] > cfg.max_execution_errors:
        checks.append(_rule_result('paper_execution_errors', FAIL, paper['execution_error_count'], cfg.max_execution_errors, 'Paper execution reported crashes or broker/runtime errors.', 'paper'))
        blocking_reasons.append('paper_execution_errors_present')
    else:
        checks.append(_rule_result('paper_execution_errors', PASS, paper['execution_error_count'], cfg.max_execution_errors, 'Paper execution completed without crashes.', 'paper'))

    if cfg.require_cooldown_controls and (not backtest['cooldown_controls_enforced'] or not paper['cooldown_controls_enforced']):
        checks.append(_rule_result('cooldown_duplicate_controls', FAIL, {'backtest': backtest['cooldown_controls_enforced'], 'paper': paper['cooldown_controls_enforced']}, True, 'Cooldown and duplicate controls must be actively enforced before go-live.', 'system'))
        blocking_reasons.append('cooldown_duplicate_controls_not_enforced')
    else:
        checks.append(_rule_result('cooldown_duplicate_controls', PASS, {'backtest': backtest['cooldown_controls_enforced'], 'paper': paper['cooldown_controls_enforced']}, True, 'Cooldown and duplicate controls are enforced.', 'system'))

    weakest_win_rate = min(backtest['win_rate_pct'], paper['win_rate_pct'] if paper['completed_trades'] > 0 else backtest['win_rate_pct'])
    if weakest_win_rate < breakeven_win_rate:
        checks.append(_rule_result('win_rate_vs_rr', FAIL, weakest_win_rate, f'>={breakeven_win_rate}', 'Win rate is below the RR-adjusted breakeven threshold.', 'system'))
        blocking_reasons.append('win_rate_below_rr_breakeven')
    elif weakest_win_rate < required_win_rate:
        checks.append(_rule_result('win_rate_vs_rr', CONDITIONAL_PASS, weakest_win_rate, f'>={required_win_rate}', 'Win rate is above breakeven but still soft relative to RR.', 'system'))
        warnings.append('win_rate_soft_relative_to_rr')
    else:
        checks.append(_rule_result('win_rate_vs_rr', PASS, weakest_win_rate, f'>={required_win_rate}', 'Win rate is strong enough relative to RR.', 'system'))

    win_rate_gap = abs(backtest['win_rate_pct'] - paper['win_rate_pct'])
    profit_factor_gap = abs(backtest['profit_factor'] - paper['profit_factor']) if backtest['profit_factor'] != float('inf') and paper['profit_factor'] != float('inf') else 0.0
    expectancy_gap_ratio = 0.0
    if abs(backtest['expectancy_per_trade']) > 0:
        expectancy_gap_ratio = abs(backtest['expectancy_per_trade'] - paper['expectancy_per_trade']) / abs(backtest['expectancy_per_trade'])
    if (
        paper['completed_trades'] > 0
        and (
            win_rate_gap > cfg.max_material_win_rate_gap_pct
            or profit_factor_gap > cfg.max_material_profit_factor_gap
            or expectancy_gap_ratio > cfg.max_material_expectancy_gap_ratio
        )
    ):
        checks.append(_rule_result('paper_vs_backtest_alignment', CONDITIONAL_PASS, {'win_rate_gap_pct': round(win_rate_gap, 2), 'profit_factor_gap': round(profit_factor_gap, 2), 'expectancy_gap_ratio': round(expectancy_gap_ratio, 2)}, {'max_win_rate_gap_pct': cfg.max_material_win_rate_gap_pct, 'max_profit_factor_gap': cfg.max_material_profit_factor_gap, 'max_expectancy_gap_ratio': cfg.max_material_expectancy_gap_ratio}, 'Paper results differ materially from backtest and need more observation.', 'system'))
        warnings.append('paper_results_materially_different_from_backtest')
    else:
        checks.append(_rule_result('paper_vs_backtest_alignment', PASS, {'win_rate_gap_pct': round(win_rate_gap, 2), 'profit_factor_gap': round(profit_factor_gap, 2), 'expectancy_gap_ratio': round(expectancy_gap_ratio, 2)}, {'max_win_rate_gap_pct': cfg.max_material_win_rate_gap_pct, 'max_profit_factor_gap': cfg.max_material_profit_factor_gap, 'max_expectancy_gap_ratio': cfg.max_material_expectancy_gap_ratio}, 'Paper and backtest remain within configured drift limits.', 'system'))

    if blocking_reasons:
        decision_status = 'FAIL_NOT_READY'
        recommended_next_action = 'Keep the system off live capital, fix blocking rules, and continue Nifty intraday backtest/paper validation within 09:15-15:30 session controls.'
    elif warnings:
        decision_status = 'PAPER_ONLY'
        recommended_next_action = 'Continue paper trading until warnings clear and the sample moves closer to 200 clean trades with stable paper/backtest alignment.'
    else:
        decision_status = 'PASS_FOR_SMALL_CAPITAL'
        recommended_next_action = 'Eligible for small-capital live deployment during NSE market hours 09:15-15:30 with existing hard risk limits unchanged.'

    passed_checks = [item['rule'] for item in checks if item['status'] == PASS]
    failed_checks = [item['rule'] for item in checks if item['status'] == FAIL]
    conditional_checks = [item['rule'] for item in checks if item['status'] == CONDITIONAL_PASS]
    human_summary = (
        f"Go-live status: {decision_status}. Passed checks: {len(passed_checks)}. "
        f"Failed checks: {len(failed_checks)}. Next action: {recommended_next_action}"
    )

    return {
        'decision_status': decision_status,
        'pass_fail_by_rule': {item['rule']: item for item in checks},
        'pass_fail_checklist': checks,
        'blocking_reasons': blocking_reasons,
        'warnings': warnings,
        'recommended_next_action': recommended_next_action,
        'human_readable_summary': human_summary,
        'ui_summary': {
            'go_live_status': decision_status,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'next_action': recommended_next_action,
        },
        'context': {
            'market': cfg.market_label,
            'market_hours': f'{cfg.market_open}-{cfg.market_close}',
            'required_win_rate_pct': required_win_rate,
            'rr_breakeven_win_rate_pct': breakeven_win_rate,
        },
        'config': asdict(cfg),
        'backtest_metrics': backtest,
        'paper_metrics': paper,
    }


def write_validation_summary_json(path: str | Path, evaluation: dict[str, Any]) -> Path:
    """Write full go-live validation details to JSON."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(evaluation, indent=2, sort_keys=True), encoding='utf-8')
    return output_path


def write_pass_fail_checklist_csv(path: str | Path, evaluation: dict[str, Any]) -> Path:
    """Write rule-level validation checklist to CSV."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [dict(row) for row in evaluation.get('pass_fail_checklist', [])]
    if not rows:
        output_path.write_text('', encoding='utf-8')
        return output_path
    with output_path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return output_path
