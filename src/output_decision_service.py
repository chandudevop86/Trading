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


def _has_quality_sample(total_trades: int, minimum: int = 150) -> bool:
    return int(total_trades) >= int(minimum)




def _split_blockers(summary: dict[str, object]) -> list[str]:
    raw = str(summary.get('deployment_blockers', '') or '').strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split(';') if part.strip()]


def _blocker_details(blocker: str) -> dict[str, str]:
    token = str(blocker or '').strip().upper()
    if token.startswith('MIN_TRADES<'):
        return {
            'severity': 'RED',
            'headline': 'Not enough trades yet',
            'plain_english': 'The sample is too small to trust.',
            'fix': 'Run a clean backtest until the system has 150 to 200 validated trades.',
        }
    if token in {'NEGATIVE_EXPECTANCY'} or token.startswith('EXPECTANCY<='):
        return {
            'severity': 'RED',
            'headline': 'Expectancy is not positive',
            'plain_english': 'Average trade quality is still negative.',
            'fix': 'Remove weak setups first and keep only the cleanest retest entries.',
        }
    if token.startswith('EXPECTANCY_STABILITY_GAP>'):
        return {
            'severity': 'YELLOW',
            'headline': 'Expectancy is unstable',
            'plain_english': 'Performance changes too much across the sample.',
            'fix': 'Reduce setup variety and keep only stable session and VWAP-aligned trades.',
        }
    if token.startswith('SECOND_HALF_EXPECTANCY'):
        return {
            'severity': 'YELLOW',
            'headline': 'Later sample performance is weaker',
            'plain_english': 'The strategy degrades in the second half of the sample.',
            'fix': 'Tighten filters until later trades are as clean as early trades.',
        }
    if token.startswith('PROFIT_FACTOR<=') or token.startswith('PROFIT_FACTOR<'):
        return {
            'severity': 'RED',
            'headline': 'Profit factor is below target',
            'plain_english': 'Winners are not strong enough compared with losers.',
            'fix': 'Filter weak zones and weak retests so losing trades fall faster than winning trades.',
        }
    if token.startswith('WIN_RATE<'):
        return {
            'severity': 'YELLOW',
            'headline': 'Win rate is below target',
            'plain_english': 'Too many trades are failing for the current reward-to-risk profile.',
            'fix': 'Cut low-quality entries and only keep the best retest and context setups.',
        }
    if token.startswith('AVG_RR<'):
        return {
            'severity': 'YELLOW',
            'headline': 'Reward-to-risk is too weak',
            'plain_english': 'Winning trades are not paying enough for the risk taken.',
            'fix': 'Improve zone quality and trade location before allowing entries.',
        }
    if token.startswith('MAX_DD_PCT>'):
        return {
            'severity': 'RED',
            'headline': 'Drawdown is too high',
            'plain_english': 'Risk is too large for the current edge quality.',
            'fix': 'Reduce overtrading and weak entries until drawdown falls below the validation limit.',
        }
    if token == 'DRAWDOWN_NOT_PROVEN':
        return {
            'severity': 'YELLOW',
            'headline': 'Drawdown proof is missing',
            'plain_english': 'The sample does not yet prove how the system behaves under stress.',
            'fix': 'Keep testing until the sample contains real losing periods and a trustworthy drawdown path.',
        }
    if token.startswith('DUPLICATES'):
        return {
            'severity': 'RED',
            'headline': 'Duplicate trades are still present',
            'plain_english': 'One-signal-one-trade discipline is not fully stable yet.',
            'fix': 'Keep duplicate prevention and cooldown rules strict until duplicates stay at zero.',
        }
    if token.startswith('INVALID_TRADES'):
        return {
            'severity': 'RED',
            'headline': 'Invalid trade rows are present',
            'plain_english': 'Some trade records are malformed or missing required fields.',
            'fix': 'Reject malformed rows before execution and keep logs clean.',
        }
    return {
        'severity': 'YELLOW',
        'headline': blocker.replace('_', ' ').title(),
        'plain_english': 'This validation blocker still needs attention.',
        'fix': 'Clear this blocker before any deployment decision.',
    }


def build_blocker_frame(summary: dict[str, object]) -> pd.DataFrame:
    blockers = _split_blockers(summary)
    if not blockers:
        return pd.DataFrame([
            {'severity': 'GREEN', 'headline': 'No active blockers', 'plain_english': 'Current validation blockers are clear.', 'fix': 'Keep monitoring rolling samples.'}
        ])
    rows = []
    for blocker in blockers:
        details = _blocker_details(blocker)
        rows.append({
            'severity': details['severity'],
            'headline': details['headline'],
            'plain_english': details['plain_english'],
            'fix': details['fix'],
            'raw_blocker': blocker,
        })
    return pd.DataFrame(rows)


def build_top_fix_actions(summary: dict[str, object], limit: int = 3) -> list[str]:
    blockers = _split_blockers(summary)
    if not blockers:
        return ['Keep the current paper-trading checks running and review rolling samples.']
    ranked: list[str] = []
    for blocker in blockers:
        fix = _blocker_details(blocker)['fix']
        if fix not in ranked:
            ranked.append(fix)
        if len(ranked) >= int(limit):
            break
    return ranked


def build_plain_english_next_action(summary: dict[str, object]) -> str:
    blockers = _split_blockers(summary)
    if not blockers:
        return 'The validation stack is clear. Continue paper trading and keep monitoring rolling samples.'
    details = [_blocker_details(blocker) for blocker in blockers]
    top_reasons = ', '.join(detail['headline'].lower() for detail in details[:2])
    top_fix = build_top_fix_actions(summary, limit=1)[0]
    return f'This strategy is not ready yet because {top_reasons}. {top_fix}'

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
    has_quality_sample = _has_quality_sample(total_trades)

    if not has_quality_sample:
        rows.append(_pillar_row('Retest-Only Entry', 'WATCH', 'No trade sample yet', 'Retest discipline cannot be judged until the system produces a clean sample.', 'Run a backtest and confirm retest-only trades stay high.'))
    elif retest_only_trade_pct >= 90:
        rows.append(_pillar_row('Retest-Only Entry', 'PASS', 'Retest discipline is strong', f'{retest_only_trade_pct:.1f}% of trades came from retest-only entries.', 'Keep retest-only entry as the default execution policy.'))
    elif retest_only_trade_pct >= 70:
        rows.append(_pillar_row('Retest-Only Entry', 'WATCH', 'Retest discipline is mixed', f'{retest_only_trade_pct:.1f}% of trades came from retest-only entries, which is still softer than ideal.', 'Tighten entry filters until weak early entries are reduced further.'))
    else:
        rows.append(_pillar_row('Retest-Only Entry', 'FAIL', 'Retest discipline is weak', f'Only {retest_only_trade_pct:.1f}% of trades came from retest-only entries.', 'Block non-retest setups and reduce impulsive early entries.'))

    if not has_quality_sample:
        rows.append(_pillar_row('VWAP + Session Filter', 'WATCH', 'No context sample yet', 'VWAP and session filters need a live sample before they can be judged.', 'Run a backtest and confirm weak session trades are removed.'))
    else:
        context_score = min(vwap_pass_pct, session_pass_pct)
        if context_score >= 85:
            rows.append(_pillar_row('VWAP + Session Filter', 'PASS', 'Trade context is clean', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%.', 'Keep morning-session and VWAP filters active.'))
        elif context_score >= 65:
            rows.append(_pillar_row('VWAP + Session Filter', 'WATCH', 'Trade context is improving', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%, but weak context trades still appear.', 'Tighten session and VWAP gates until weak context trades fall further.'))
        else:
            rows.append(_pillar_row('VWAP + Session Filter', 'FAIL', 'Trade context is weak', f'VWAP pass is {vwap_pass_pct:.1f}% and session pass is {session_pass_pct:.1f}%.', 'Remove more low-quality session trades and require stronger VWAP alignment.'))

    if not has_quality_sample:
        rows.append(_pillar_row('Zone Scoring', 'WATCH', 'No setup-quality sample yet', 'Zone quality needs a trade sample before the scoring filter can be judged.', 'Run a backtest and review average zone score.'))
    elif avg_zone_gate_score >= 5.0:
        rows.append(_pillar_row('Zone Scoring', 'PASS', 'Setup quality is strong', f'Average zone score is {avg_zone_gate_score:.2f}, which is above the current floor.', 'Keep weak zones filtered out before entry evaluation.'))
    elif avg_zone_gate_score >= 4.0:
        rows.append(_pillar_row('Zone Scoring', 'WATCH', 'Setup quality is acceptable but soft', f'Average zone score is {avg_zone_gate_score:.2f}, so weaker setups are still leaking through.', 'Raise the zone floor or tighten reaction quality.'))
    else:
        rows.append(_pillar_row('Zone Scoring', 'FAIL', 'Setup quality is weak', f'Average zone score is only {avg_zone_gate_score:.2f}.', 'Filter weak zones earlier and require stronger reaction quality.'))

    if not has_quality_sample or not sample_window_passed:
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
    blockers = _split_blockers(summary)
    if blockers:
        return build_plain_english_next_action(summary)
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



__all__ = [
    'build_blocker_frame',
    'build_plain_english_next_action',
    'build_quality_ladder_frame',
    'build_quality_ladder_summary',
    'build_strategy_quality_ladder',
    'build_top_fix_actions',
]
