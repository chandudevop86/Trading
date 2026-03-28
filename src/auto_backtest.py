from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.backtest_engine import nifty_intraday_validation_config, summarize_trade_log
from src.execution_engine import summarize_execution_result
from src.live_ohlcv import fetch_live_ohlcv, write_csv
from src.strategy_evaluator import rank_strategy_summaries
from src.strategy_tuning import apply_strategy_benchmark, optimizer_report_rows
from src.strategy_service import StrategyContext, generate_strategy_rows
from src.trading_workflows import build_backtest_workflow, run_live_candidates, run_paper_candidates
from src.runtime_persistence import persist_rows
from src.validation_engine import (
    default_nifty_intraday_go_live_config,
    evaluate_go_live,
    write_pass_fail_checklist_csv,
    write_validation_summary_json,
)
from src.crisis_risk_engine import (
    apply_crisis_overrides,
    default_nifty_crisis_config,
    detect_market_stress,
    evaluate_live_permission,
    write_crisis_summary_json,
)

DEFAULT_VALIDATION_THRESHOLDS: dict[str, float] = {
    'min_trades': 150.0,
    'max_trades': 200.0,
    'min_win_rate_pct': 38.0,
    'min_profit_factor': 1.3,
    'min_expectancy': 0.0,
    'max_drawdown_pct': 12.0,
    'min_net_pnl': 0.0,
    'max_expectancy_stability_gap_ratio': 0.5,
}


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _event_time_key(row: dict[str, Any]) -> str:
    return str(
        row.get('exit_time')
        or row.get('entry_time')
        or row.get('signal_time')
        or row.get('timestamp')
        or ''
    )


def _build_equity_curve_rows(
    strategy: str,
    rows: list[dict[str, Any]],
    *,
    starting_equity: float,
) -> list[dict[str, Any]]:
    ordered_rows = sorted(rows, key=_event_time_key)
    cumulative_pnl = 0.0
    equity = float(starting_equity)
    peak_equity = equity
    curve_rows: list[dict[str, Any]] = [
        {
            'strategy': strategy,
            'point_no': 0,
            'event_time': '',
            'trade_pnl': 0.0,
            'cumulative_pnl': 0.0,
            'equity': round(equity, 2),
            'peak_equity': round(peak_equity, 2),
            'drawdown': 0.0,
            'drawdown_pct': 0.0,
        }
    ]

    for idx, row in enumerate(ordered_rows, start=1):
        trade_pnl = _safe_float(row.get('pnl'))
        cumulative_pnl += trade_pnl
        equity = float(starting_equity) + cumulative_pnl
        peak_equity = max(peak_equity, equity)
        drawdown = peak_equity - equity
        drawdown_pct = (drawdown / peak_equity) * 100.0 if peak_equity else 0.0
        curve_rows.append(
            {
                'strategy': strategy,
                'point_no': idx,
                'event_time': _event_time_key(row),
                'trade_pnl': round(trade_pnl, 2),
                'cumulative_pnl': round(cumulative_pnl, 2),
                'equity': round(equity, 2),
                'peak_equity': round(peak_equity, 2),
                'drawdown': round(drawdown, 2),
                'drawdown_pct': round(drawdown_pct, 2),
            }
        )

    return curve_rows


def _profit_factor(gross_profit: float, gross_loss_abs: float) -> float | str:
    if gross_loss_abs > 0:
        return round(gross_profit / gross_loss_abs, 4)
    if gross_profit > 0:
        return 'INF'
    return 0.0


def _score_bucket(score: float) -> str:
    if score >= 13:
        return '13+'
    if score >= 10:
        return '10-12'
    if score >= 7:
        return '7-9'
    return '0-6'


def _trade_score(record: dict[str, Any]) -> float:
    for key in ('mtf_score', 'score', 'total_score', 'zone_score', 'zone_strength_score', 'zone_gate_score'):
        raw = str(record.get(key, '') or '').strip()
        if raw:
            return _safe_float(record.get(key))
    return 0.0


def _has_mtf_scores(trades: list[dict[str, Any]]) -> bool:
    return any(str(trade.get('mtf_score', '') or '').strip() for trade in trades)


def _mtf_score_bucket(score: float) -> str:
    if score >= 16:
        return '16-20'
    if score >= 12:
        return '12-15'
    if score >= 9:
        return '9-11'
    return '0-8'


def _mtf_score_bucket_metrics(trades: list[dict[str, Any]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-8', '9-11', '12-15', '16-20']
    bucket_map: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in buckets}
    for trade in trades:
        bucket_map[_mtf_score_bucket(_safe_float(trade.get('mtf_score')))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        total = len(bucket_trades)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _build_equity_curve_rows('MTF_BUCKET', bucket_trades, starting_equity=float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'wins': float(wins),
            'losses': float(losses),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'total_pnl': round(sum(pnl_values), 2),
            'avg_pnl': round(expectancy, 2),
            'expectancy': round(expectancy, 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},wins:{int(values['wins'])},losses:{int(values['losses'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _breakout_score_bucket(score: float) -> str:
    if score >= 15:
        return '15+'
    if score >= 12:
        return '12-14'
    if score >= 9:
        return '9-11'
    return '0-8'


def _breakout_score_bucket_metrics(trades: list[dict[str, Any]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-8', '9-11', '12-14', '15+']
    breakout_trades = [trade for trade in trades if str(trade.get('strategy', '')).upper() == 'BREAKOUT']
    bucket_map: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in buckets}
    for trade in breakout_trades:
        bucket_map[_breakout_score_bucket(_trade_score(trade))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        total = len(bucket_trades)
        equity_rows = _build_equity_curve_rows('BREAKOUT_BUCKET', bucket_trades, starting_equity=float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'avg_pnl': round((sum(pnl_values) / total), 2) if total else 0.0,
            'expectancy': round((sum(pnl_values) / total), 2) if total else 0.0,
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _score_bucket_metrics(trades: list[dict[str, Any]], capital: float) -> tuple[dict[str, dict[str, float]], str]:
    buckets = ['0-6', '7-9', '10-12', '13+']
    bucket_map: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in buckets}
    for trade in trades:
        bucket_map[_score_bucket(_trade_score(trade))].append(trade)

    analysis: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        bucket_trades = bucket_map[bucket]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in bucket_trades]
        wins = sum(1 for value in pnl_values if value > 0)
        losses = sum(1 for value in pnl_values if value < 0)
        total = len(bucket_trades)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _build_equity_curve_rows('BUCKET', bucket_trades, starting_equity=float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        analysis[bucket] = {
            'trades': float(total),
            'wins': float(wins),
            'losses': float(losses),
            'win_rate': round((wins / total) * 100.0, 2) if total else 0.0,
            'total_pnl': round(sum(pnl_values), 2),
            'avg_pnl': round(expectancy, 2),
            'expectancy': round(expectancy, 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
        }

    serialized = '; '.join(
        f"{bucket}=trades:{int(values['trades'])},wins:{int(values['wins'])},losses:{int(values['losses'])},win_rate:{values['win_rate']:.2f},avg_pnl:{values['avg_pnl']:.2f},expectancy:{values['expectancy']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for bucket, values in analysis.items()
    )
    return analysis, serialized


def _threshold_filter_metrics(trades: list[dict[str, Any]], capital: float) -> tuple[dict[str, dict[str, float]], str, str]:
    thresholds = [('ALL', 0.0), ('8+', 8.0), ('10+', 10.0), ('12+', 12.0)]
    analysis: dict[str, dict[str, float]] = {}
    best_label = 'ALL'
    best_key = (-10**9, -10**9, 10**9, -10**9)
    for label, threshold in thresholds:
        filtered = trades if threshold <= 0 else [trade for trade in trades if _trade_score(trade) >= threshold]
        pnl_values = [_safe_float(trade.get('pnl')) for trade in filtered]
        wins = sum(1 for value in pnl_values if value > 0)
        total = len(filtered)
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss_abs = abs(sum(value for value in pnl_values if value < 0))
        profit_factor = gross_profit / gross_loss_abs if gross_loss_abs > 0 else float('inf') if gross_profit > 0 else 0.0
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _build_equity_curve_rows('FILTER', filtered, starting_equity=float(capital)) if total else []
        max_drawdown_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        win_rate = round((wins / total) * 100.0, 2) if total else 0.0
        analysis[label] = {
            'threshold': threshold,
            'trades': float(total),
            'win_rate': win_rate,
            'total_pnl': round(sum(pnl_values), 2),
            'expectancy': round(expectancy, 2),
            'max_drawdown_pct': round(float(max_drawdown_pct), 2),
            'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
        }
        ranking_pf = 999.0 if profit_factor == float('inf') else float(profit_factor)
        candidate_key = (round(expectancy, 6), ranking_pf, -float(max_drawdown_pct), -float(total))
        if total > 0 and candidate_key > best_key:
            best_key = candidate_key
            best_label = label

    serialized = '; '.join(
        f"{label}=trades:{int(values['trades'])},win_rate:{values['win_rate']:.2f},expectancy:{values['expectancy']:.2f},pnl:{values['total_pnl']:.2f},max_dd_pct:{values['max_drawdown_pct']:.2f},pf:{values['profit_factor']}"
        for label, values in analysis.items()
    )
    return analysis, serialized, best_label


def _binary_segment_metrics(rows: list[dict[str, Any]], key: str, positive_value: str, capital: float) -> str:
    groups = {'YES': [], 'NO': []}
    for row in rows:
        groups['YES' if str(row.get(key, '')).strip().upper() == positive_value else 'NO'].append(row)
    parts = []
    for label in ['YES', 'NO']:
        sample = groups[label]
        pnl_values = [_safe_float(item.get('pnl')) for item in sample]
        total = len(sample)
        wins = sum(1 for value in pnl_values if value > 0)
        expectancy = (sum(pnl_values) / total) if total else 0.0
        equity_rows = _equity_curve_rows(sample, float(capital)) if 'backtest_engine' in __file__ and total else _build_equity_curve_rows('SEGMENT', sample, starting_equity=float(capital)) if total else []
        max_dd_pct = max((row.get('drawdown_pct', 0.0) for row in equity_rows), default=0.0)
        parts.append(f"{label}=trades:{total},win_rate:{((wins / total) * 100.0) if total else 0.0:.2f},expectancy:{expectancy:.2f},max_dd_pct:{float(max_dd_pct):.2f}")
    return '; '.join(parts)


def _score_progression_flags(bucket_analysis: dict[str, dict[str, float]]) -> tuple[str, str]:
    progression = ['0-6', '7-9', '10-12', '13+']
    active = [bucket for bucket in progression if bucket_analysis.get(bucket, {}).get('trades', 0.0) > 0]
    if len(active) < 2:
        return 'INSUFFICIENT_DATA', 'INSUFFICIENT_DATA'
    win_rates = [float(bucket_analysis[bucket]['win_rate']) for bucket in active]
    expectancies = [float(bucket_analysis[bucket]['expectancy']) for bucket in active]
    win_rate_improves = all(curr >= prev for prev, curr in zip(win_rates, win_rates[1:]))
    expectancy_improves = all(curr >= prev for prev, curr in zip(expectancies, expectancies[1:]))
    return ('YES' if win_rate_improves else 'NO'), ('YES' if expectancy_improves else 'NO')


def _pnl_summary(
    strategy: str,
    rows: list[dict[str, Any]],
    *,
    starting_equity: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pnl_values = [_safe_float(r.get('pnl')) for r in rows]
    total_pnl = round(sum(pnl_values), 2)
    win_values = [p for p in pnl_values if p > 0]
    loss_values = [p for p in pnl_values if p < 0]
    wins = len(win_values)
    losses = len(loss_values)
    total = len(rows)
    total_cost = round(sum(_safe_float(r.get('trading_cost')) for r in rows), 2)
    gross_total_pnl = round(sum(_safe_float(r.get('gross_pnl', r.get('pnl'))) for r in rows), 2)
    gross_profit = round(sum(win_values), 2)
    gross_loss_abs = round(abs(sum(loss_values)), 2)
    equity_curve_rows = _build_equity_curve_rows(strategy, rows, starting_equity=starting_equity)
    max_drawdown = max((float(r.get('drawdown', 0.0) or 0.0) for r in equity_curve_rows), default=0.0)
    max_drawdown_pct = max((float(r.get('drawdown_pct', 0.0) or 0.0) for r in equity_curve_rows), default=0.0)
    expectancy_per_trade = round(total_pnl / total, 2) if total else 0.0
    first_half_count = total // 2
    first_half_values = pnl_values[:first_half_count]
    second_half_values = pnl_values[first_half_count:]
    first_half_expectancy = round(sum(first_half_values) / len(first_half_values), 2) if first_half_values else 0.0
    second_half_expectancy = round(sum(second_half_values) / len(second_half_values), 2) if second_half_values else 0.0
    expectancy_stability_gap_ratio = round(abs(first_half_expectancy - second_half_expectancy) / abs(expectancy_per_trade), 4) if abs(expectancy_per_trade) > 0 else 0.0
    drawdown_proven = max_drawdown_pct > 0 and losses > 0
    retest_only_count = sum(1 for row in rows if str(row.get('retest_only_entry', '')).strip().upper() == 'YES')
    vwap_pass_count = sum(1 for row in rows if str(row.get('vwap_gate', '')).strip().upper() == 'PASS')
    session_pass_count = sum(1 for row in rows if str(row.get('session_gate', '')).strip().upper() == 'PASS')
    zone_gate_scores = [_safe_float(row.get('zone_gate_score')) for row in rows if str(row.get('zone_gate_score', '')).strip() != '']
    avg_zone_gate_score = round(sum(zone_gate_scores) / len(zone_gate_scores), 2) if zone_gate_scores else 0.0
    bucket_analysis, score_bucket_analysis = _score_bucket_metrics(rows, float(starting_equity))
    threshold_analysis, threshold_filter_analysis, best_threshold_label = _threshold_filter_metrics(rows, float(starting_equity))
    higher_score_improves_win_rate, higher_score_improves_expectancy = _score_progression_flags(bucket_analysis)
    mtf_bucket_analysis, mtf_score_bucket_analysis = _mtf_score_bucket_metrics(rows, float(starting_equity)) if _has_mtf_scores(rows) else ({}, '')
    breakout_bucket_analysis, breakout_score_bucket_analysis = _breakout_score_bucket_metrics(rows, float(starting_equity))
    overlap_performance = _binary_segment_metrics(rows, 'overlap_with_fvg', 'YES', float(starting_equity))
    freshness_performance = _binary_segment_metrics(rows, 'fresh_zone', 'YES', float(starting_equity))
    summary = {
        'strategy': strategy,
        'trades': total,
        'total_trades': total,
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round((wins / total) * 100.0, 2) if total else 0.0,
        'avg_win': round(sum(win_values) / len(win_values), 2) if win_values else 0.0,
        'avg_loss': round(sum(loss_values) / len(loss_values), 2) if loss_values else 0.0,
        'gross_profit': gross_profit,
        'gross_loss': gross_loss_abs,
        'profit_factor': _profit_factor(gross_profit, gross_loss_abs),
        'max_drawdown': round(max_drawdown, 2),
        'max_drawdown_pct': round(max_drawdown_pct, 2),
        'starting_equity': round(float(starting_equity), 2),
        'ending_equity': round(float(starting_equity) + sum(pnl_values), 2),
        'equity_curve_points': max(0, len(equity_curve_rows) - 1),
        'gross_total_pnl': gross_total_pnl,
        'total_trading_cost': total_cost,
        'total_pnl': total_pnl,
        'avg_pnl': expectancy_per_trade,
        'expectancy_per_trade': expectancy_per_trade,
        'first_half_expectancy_per_trade': first_half_expectancy,
        'second_half_expectancy_per_trade': second_half_expectancy,
        'expectancy_stability_gap_ratio': expectancy_stability_gap_ratio,
        'drawdown_proven': 'YES' if drawdown_proven else 'NO',
        'retest_only_trades': retest_only_count,
        'retest_only_trade_pct': round((retest_only_count / total) * 100.0, 2) if total else 0.0,
        'vwap_pass_trades': vwap_pass_count,
        'vwap_pass_pct': round((vwap_pass_count / total) * 100.0, 2) if total else 0.0,
        'session_pass_trades': session_pass_count,
        'session_pass_pct': round((session_pass_count / total) * 100.0, 2) if total else 0.0,
        'avg_zone_gate_score': avg_zone_gate_score,
        'score_bucket_analysis': score_bucket_analysis,
        'threshold_filter_analysis': threshold_filter_analysis,
        'mtf_score_bucket_analysis': mtf_score_bucket_analysis,
        'breakout_score_bucket_analysis': breakout_score_bucket_analysis,
        'overlap_performance': overlap_performance,
        'freshness_performance': freshness_performance,
        'best_min_score_threshold': best_threshold_label,
        'higher_score_improves_win_rate': higher_score_improves_win_rate,
        'higher_score_improves_expectancy': higher_score_improves_expectancy,
        'positive_expectancy': 'YES' if expectancy_per_trade > 0 else 'NO',
    }
    return summary, equity_curve_rows
def _validation_thresholds(args: argparse.Namespace) -> dict[str, float]:
    thresholds = dict(DEFAULT_VALIDATION_THRESHOLDS)
    for key in thresholds:
        value = getattr(args, key, None)
        if value is None:
            continue
        thresholds[key] = float(value)
    thresholds['min_trades'] = max(thresholds['min_trades'], 0.0)
    thresholds['max_trades'] = max(thresholds['max_trades'], thresholds['min_trades'])
    thresholds['max_drawdown_pct'] = max(thresholds['max_drawdown_pct'], 0.0)
    thresholds['max_expectancy_stability_gap_ratio'] = max(thresholds['max_expectancy_stability_gap_ratio'], 0.0)
    return thresholds


def _validation_reasons(summary: dict[str, Any], thresholds: dict[str, float]) -> list[str]:
    trades = _safe_int(summary.get('total_trades', summary.get('trades')))
    win_rate_pct = _safe_float(summary.get('win_rate_pct', summary.get('win_rate')))
    profit_factor_raw = summary.get('profit_factor')
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else _safe_float(profit_factor_raw)
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    second_half_expectancy = _safe_float(summary.get('second_half_expectancy_per_trade'))
    expectancy_stability_gap_ratio = _safe_float(summary.get('expectancy_stability_gap_ratio'))
    max_drawdown_pct = _safe_float(summary.get('max_drawdown_pct'))
    total_pnl = _safe_float(summary.get('total_pnl'))
    drawdown_proven = str(summary.get('drawdown_proven', 'NO')).strip().upper() == 'YES'
    reasons: list[str] = []
    if trades < int(thresholds['min_trades']):
        reasons.append('too_few_trades')
    if trades > int(thresholds['max_trades']):
        reasons.append('trade_count_above_validation_window')
    if win_rate_pct < thresholds['min_win_rate_pct']:
        reasons.append('win_rate_below_threshold')
    if profit_factor != float('inf') and profit_factor <= thresholds['min_profit_factor']:
        reasons.append('profit_factor_below_threshold')
    if expectancy <= thresholds['min_expectancy']:
        reasons.append('expectancy_not_positive' if thresholds['min_expectancy'] <= 0 else 'expectancy_below_threshold')
    if second_half_expectancy <= thresholds['min_expectancy']:
        reasons.append('second_half_expectancy_not_positive')
    if expectancy_stability_gap_ratio > thresholds['max_expectancy_stability_gap_ratio']:
        reasons.append('expectancy_unstable')
    if max_drawdown_pct > thresholds['max_drawdown_pct']:
        reasons.append('drawdown_above_limit')
    if not drawdown_proven:
        reasons.append('drawdown_not_proven')
    if total_pnl < thresholds['min_net_pnl']:
        reasons.append('net_pnl_below_threshold')
    return reasons
def _apply_validation(summary: dict[str, Any], thresholds: dict[str, float]) -> dict[str, Any]:
    item = dict(summary)
    reasons = _validation_reasons(item, thresholds)
    validation_status = 'PASS' if not reasons else 'FAIL'
    validation_reasons = '|'.join(reasons)
    promotable = validation_status == 'PASS'
    item['validation_status'] = validation_status
    item['validation_reasons'] = validation_reasons
    item['validation_reasons_list'] = list(reasons)
    item['promotable'] = 'YES' if promotable else 'NO'
    item['deployment_ready'] = 'YES' if promotable else 'NO'
    existing_blockers = str(item.get('deployment_blockers', '') or '').strip()
    validation_blockers = validation_reasons.replace('|', '; ')
    item['deployment_blockers'] = '; '.join(part for part in [existing_blockers, validation_blockers] if part)
    return item


def _validation_report_rows(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    report_rows: list[dict[str, Any]] = []
    for row in summary_rows:
        report_rows.append(
            {
                'strategy': row.get('strategy', ''),
                'trades': row.get('total_trades', row.get('trades', 0)),
                'win_rate_pct': row.get('win_rate_pct', row.get('win_rate', 0.0)),
                'profit_factor': row.get('profit_factor', 0.0),
                'expectancy_per_trade': row.get('expectancy_per_trade', 0.0),
                'first_half_expectancy_per_trade': row.get('first_half_expectancy_per_trade', 0.0),
                'second_half_expectancy_per_trade': row.get('second_half_expectancy_per_trade', 0.0),
                'expectancy_stability_gap_ratio': row.get('expectancy_stability_gap_ratio', 0.0),
                'max_drawdown_pct': row.get('max_drawdown_pct', 0.0),
                'drawdown_proven': row.get('drawdown_proven', 'NO'),
                'retest_only_trades': row.get('retest_only_trades', 0),
                'retest_only_trade_pct': row.get('retest_only_trade_pct', 0.0),
                'vwap_pass_trades': row.get('vwap_pass_trades', 0),
                'vwap_pass_pct': row.get('vwap_pass_pct', 0.0),
                'session_pass_trades': row.get('session_pass_trades', 0),
                'session_pass_pct': row.get('session_pass_pct', 0.0),
                'avg_zone_gate_score': row.get('avg_zone_gate_score', 0.0),
                'total_pnl': row.get('total_pnl', 0.0),
                'validation_status': row.get('validation_status', 'FAIL'),
                'validation_reasons': row.get('validation_reasons', ''),
                'promotable': row.get('promotable', 'NO'),
            }
        )
    return report_rows


def _build_candidate_map(candidates: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for candidate in candidates:
        strategy = str(candidate.get('strategy', '') or '').strip().upper()
        grouped.setdefault(strategy, []).append(candidate)
    return grouped


def _execution_candidates_for_mode(
    summary_rows: list[dict[str, Any]],
    candidate_map: dict[str, list[dict[str, object]]],
    *,
    execution_type: str,
    allow_live_on_pass: bool,
    allow_paper_on_fail: bool,
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    for row in summary_rows:
        strategy = str(row.get('strategy', '') or '').strip().upper()
        validation_status = str(row.get('validation_status', 'FAIL') or 'FAIL').upper()
        validation_reasons = str(row.get('validation_reasons', '') or '').strip() or 'validation_failed'
        strategy_candidates = candidate_map.get(strategy, [])
        if not strategy_candidates:
            continue
        if execution_type == 'LIVE':
            if validation_status != 'PASS':
                print(f"[PROMOTION] Strategy {strategy} blocked from live execution: {validation_reasons}")
                continue
            if not allow_live_on_pass:
                print(f"[PROMOTION] Strategy {strategy} blocked from live execution: allow_live_on_pass_flag_required")
                continue
            print(f"[PROMOTION] Strategy {strategy} approved for live execution")
            selected.extend(strategy_candidates)
            continue
        if execution_type == 'PAPER':
            if validation_status == 'PASS':
                print(f"[PROMOTION] Strategy {strategy} approved for paper execution")
                selected.extend(strategy_candidates)
                continue
            if allow_paper_on_fail:
                print(f"[PROMOTION] Strategy {strategy} approved for paper execution via override: {validation_reasons}")
                selected.extend(strategy_candidates)
                continue
            print(f"[PROMOTION] Strategy {strategy} blocked from paper execution: {validation_reasons}")
    return selected


def _best_promotable_strategy(ranked_summary_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in ranked_summary_rows:
        if str(row.get('validation_status', 'FAIL')).upper() == 'PASS':
            return row
    return None


def _build_breakout_bias_evaluation(
    bias_required_summary: dict[str, Any],
    bias_optional_summary: dict[str, Any],
) -> dict[str, Any]:
    pnl_delta = round(
        _safe_float(bias_required_summary.get('total_pnl')) - _safe_float(bias_optional_summary.get('total_pnl')),
        2,
    )
    win_rate_delta = round(
        _safe_float(bias_required_summary.get('win_rate_pct')) - _safe_float(bias_optional_summary.get('win_rate_pct')),
        2,
    )
    trades_delta = int(bias_required_summary.get('trades', 0) or 0) - int(bias_optional_summary.get('trades', 0) or 0)
    if pnl_delta > 0:
        better_mode = 'BIAS_REQUIRED'
    elif pnl_delta < 0:
        better_mode = 'BIAS_OPTIONAL'
    else:
        better_mode = 'TIE'
    return {
        'mode_a': str(bias_required_summary.get('strategy', 'BREAKOUT')),
        'mode_b': str(bias_optional_summary.get('strategy', 'BREAKOUT_NO_BIAS')),
        'pnl_delta': pnl_delta,
        'win_rate_delta_pct': win_rate_delta,
        'trades_delta': trades_delta,
        'better_mode': better_mode,
    }


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text('', encoding='utf-8')
        try:
            persist_rows(path, [], write_mode='replace')
        except Exception:
            pass
        return
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    try:
        persist_rows(path, [dict(row) for row in rows], write_mode='replace')
    except Exception:
        pass


def _append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with path.open('a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    try:
        persist_rows(path, [dict(row) for row in rows], write_mode='replace')
    except Exception:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Auto backtest all bots and write paper logs with timeframe metadata')
    parser.add_argument('--symbol', default='^NSEI')
    parser.add_argument('--interval', default='5m', choices=['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'])
    parser.add_argument('--period', default='3mo')
    parser.add_argument('--capital', type=float, default=100000.0)
    parser.add_argument('--risk-pct', type=float, default=0.01)
    parser.add_argument('--rr-ratio', type=float, default=2.0)
    parser.add_argument('--mode', default='Balanced', choices=['Conservative', 'Balanced', 'Aggressive'])
    parser.add_argument('--trailing-sl-pct', type=float, default=0.0, help='Trailing stop percent, e.g. 0.005 = 0.5%%')
    parser.add_argument('--pivot-window', type=int, default=2)
    parser.add_argument('--entry-cutoff', default='11:30')
    parser.add_argument('--cost-bps', type=float, default=0.0)
    parser.add_argument('--fixed-cost-per-trade', type=float, default=0.0)
    parser.add_argument('--max-daily-loss', type=float, default=0.0)
    parser.add_argument('--max-trades-per-day', type=int, default=1)
    parser.add_argument('--execution-symbol', default='NIFTY')
    parser.add_argument('--data-output', type=Path, default=Path('data/live_ohlcv.csv'))
    parser.add_argument('--summary-output', type=Path, default=Path('data/backtest_results_all.csv'))
    parser.add_argument('--summary-history-output', type=Path, default=Path('data/backtest_results_history.csv'))
    parser.add_argument('--ranking-output', type=Path, default=Path('data/strategy_expectancy_report.csv'))
    parser.add_argument('--optimizer-output', type=Path, default=Path('data/strategy_optimizer_report.csv'))
    parser.add_argument('--validation-output', type=Path, default=Path('data/backtest_validation.csv'))
    parser.add_argument('--validation-report-output', type=Path, default=Path('data/backtest_validation_report.csv'))
    parser.add_argument('--deployable-summary-output', type=Path, default=Path('data/deployable_summary.csv'))
    parser.add_argument('--go-live-output-json', type=Path, default=Path('data/go_live_validation_summary.json'))
    parser.add_argument('--go-live-checklist-output', type=Path, default=Path('data/go_live_validation_checklist.csv'))
    parser.add_argument('--crisis-output-json', type=Path, default=Path('data/crisis_risk_summary.json'))
    parser.add_argument('--equity-curve-output', type=Path, default=Path('data/backtest_equity_curves.csv'))
    parser.add_argument('--paper-log-output', type=Path, default=Path('data/paper_trading_logs_all.csv'))
    parser.add_argument('--execution-type', default='PAPER', choices=['PAPER', 'LIVE', 'NONE'])
    parser.add_argument('--allow-live-on-pass', action='store_true')
    parser.add_argument('--allow-paper-on-fail', action='store_true')
    parser.add_argument('--min-trades', type=float, default=None)
    parser.add_argument('--max-trades', type=float, default=None)
    parser.add_argument('--min-win-rate-pct', type=float, default=None)
    parser.add_argument('--min-profit-factor', type=float, default=None)
    parser.add_argument('--min-expectancy', type=float, default=None)
    parser.add_argument('--max-drawdown-pct', type=float, default=None)
    parser.add_argument('--max-expectancy-stability-gap-ratio', type=float, default=None)
    parser.add_argument('--min-net-pnl', type=float, default=None)
    parser.add_argument('--live-log-output', type=Path, default=Path('data/live_trading_logs_all.csv'))
    parser.add_argument('--live-broker', default='DHAN', choices=['DHAN', 'NONE'])
    parser.add_argument('--security-map', type=Path, default=Path('data/dhan_security_map.csv'))
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Any]:
    from src.amd_fvg_sd_bot import generate_trades as generate_amd_fvg_sd_trades
    from src.breakout_bot import generate_trades as generate_breakout_trades
    from src.breakout_bot import load_candles
    from src.indicator_bot import generate_trades as generate_indicator_trades
    from src.indicator_bot import IndicatorConfig, generate_indicator_rows

    run_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    capital = float(getattr(args, 'capital', 100000.0) or 100000.0)
    risk_pct = float(getattr(args, 'risk_pct', 0.01) or 0.01)
    rr_ratio = float(getattr(args, 'rr_ratio', 2.0) or 2.0)
    mode = str(getattr(args, 'mode', 'Balanced') or 'Balanced').strip() or 'Balanced'
    trailing_sl_pct = float(getattr(args, 'trailing_sl_pct', 0.0) or 0.0)
    pivot_window = int(getattr(args, 'pivot_window', 2) or 2)
    entry_cutoff = str(getattr(args, 'entry_cutoff', '11:30') or '11:30')
    cost_bps = float(getattr(args, 'cost_bps', 0.0) or 0.0)
    fixed_cost_per_trade = float(getattr(args, 'fixed_cost_per_trade', 0.0) or 0.0)
    max_trades_per_day = int(getattr(args, 'max_trades_per_day', 1) or 1)
    max_daily_loss_value = float(getattr(args, 'max_daily_loss', 0.0) or 0.0)
    max_daily_loss = max_daily_loss_value if max_daily_loss_value > 0 else None
    equity_curve_output = Path(getattr(args, 'equity_curve_output', Path('data/backtest_equity_curves.csv')))
    validation_thresholds = _validation_thresholds(args)

    rows = fetch_live_ohlcv(args.symbol, args.interval, args.period)
    if not rows:
        raise ValueError('No OHLCV rows fetched.')
    write_csv(rows, str(args.data_output))
    timeframe = str(getattr(args, 'interval', '') or '')
    data_start = str(rows[0].get('timestamp', '')) if rows else ''
    data_end = str(rows[-1].get('timestamp', '')) if rows else ''
    crisis_config = default_nifty_crisis_config()
    initial_crisis_evaluation = detect_market_stress(rows, {}, crisis_config, timeframe=timeframe)
    print(
        f"[CRISIS] Initial state={initial_crisis_evaluation.get('stress_state')} "
        f"reasons={'|'.join(initial_crisis_evaluation.get('blocking_reasons', []) or initial_crisis_evaluation.get('warnings', []) or ['NONE'])}"
    )

    candles = load_candles(rows)
    if not candles:
        raise ValueError('No candles generated from OHLCV rows.')

    indicator_cfg = IndicatorConfig()
    breakout_context = StrategyContext(
        strategy='Breakout',
        candles=rows,
        candle_rows=candles,
        capital=capital,
        risk_pct=risk_pct * 100.0,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        symbol=args.symbol,
        mode=mode,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    demand_supply_context = StrategyContext(
        strategy='Demand Supply',
        candles=rows,
        candle_rows=candles,
        capital=capital,
        risk_pct=risk_pct * 100.0,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        symbol=args.symbol,
        mode=mode,
        pivot_window=pivot_window,
        entry_cutoff=entry_cutoff,
    )
    one_trade_context = StrategyContext(
        strategy='One Trade/Day',
        candles=rows,
        candle_rows=candles,
        capital=capital,
        risk_pct=risk_pct * 100.0,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        symbol=args.symbol,
        mode=mode,
        entry_cutoff=entry_cutoff,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    btst_context = StrategyContext(
        strategy='BTST',
        candles=rows,
        candle_rows=candles,
        capital=capital,
        risk_pct=risk_pct * 100.0,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        symbol=args.symbol,
        mode=mode,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
    )
    breakout_rows = generate_strategy_rows(breakout_context)
    breakout_no_bias_rows = generate_breakout_trades(
        candles,
        capital=capital,
        risk_pct=risk_pct,
        rr_ratio=rr_ratio,
        trailing_sl_pct=trailing_sl_pct,
        cost_bps=cost_bps,
        fixed_cost_per_trade=fixed_cost_per_trade,
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
        use_first_hour_bias=False,
        filter_choppy_days=True,
    )
    ds_rows = generate_strategy_rows(demand_supply_context)
    indicator_trade_rows = generate_indicator_trades(rows, capital=capital, risk_pct=risk_pct, rr_ratio=rr_ratio, config=indicator_cfg)
    amd_rows = generate_amd_fvg_sd_trades(rows, capital=capital, risk_pct=risk_pct, rr_ratio=rr_ratio, mode=mode)
    indicator_rows = generate_indicator_rows(candles, config=indicator_cfg)
    one_trade_rows = generate_strategy_rows(one_trade_context)
    btst_rows = generate_strategy_rows(btst_context)
    summary_and_curves = [
        _pnl_summary('BREAKOUT', breakout_rows, starting_equity=capital),
        _pnl_summary('BREAKOUT_NO_BIAS', breakout_no_bias_rows, starting_equity=capital),
        _pnl_summary('DEMAND_SUPPLY', ds_rows, starting_equity=capital),
        _pnl_summary('INDICATOR', indicator_trade_rows, starting_equity=capital),
        _pnl_summary('AMD_FVG_SD', amd_rows, starting_equity=capital),
        _pnl_summary('ONE_TRADE_DAY', one_trade_rows, starting_equity=capital),
        _pnl_summary('BTST', btst_rows, starting_equity=capital),
    ]
    summary_rows = [summary for summary, _ in summary_and_curves]
    summary_rows = [apply_strategy_benchmark(summary) for summary in summary_rows]
    breakout_bias_evaluation = _build_breakout_bias_evaluation(summary_rows[0], summary_rows[1])

    for summary in summary_rows:
        summary['timeframe'] = timeframe
        summary['data_start'] = data_start
        summary['data_end'] = data_end
        summary['run_at_utc'] = run_at
        summary['equity_curve_output'] = str(equity_curve_output)

    summary_rows = [_apply_validation(summary, validation_thresholds) for summary in summary_rows]
    ranked_summary_rows = rank_strategy_summaries(summary_rows)
    optimizer_rows = optimizer_report_rows(ranked_summary_rows)
    validation_report_rows = _validation_report_rows(summary_rows)
    promotable_rows = [row for row in ranked_summary_rows if str(row.get('validation_status', 'FAIL')).upper() == 'PASS']
    best_promotable = _best_promotable_strategy(ranked_summary_rows)

    equity_curve_rows = [curve_row for _, curve_rows in summary_and_curves for curve_row in curve_rows]
    for curve_row in equity_curve_rows:
        curve_row['timeframe'] = timeframe
        curve_row['data_start'] = data_start
        curve_row['data_end'] = data_end
        curve_row['run_at_utc'] = run_at

    _write_rows(args.summary_output, summary_rows)
    _append_rows(args.summary_history_output, summary_rows)
    _write_rows(args.ranking_output, ranked_summary_rows)
    _write_rows(args.optimizer_output, optimizer_rows)
    _write_rows(Path(getattr(args, 'validation_report_output', Path('data/backtest_validation_report.csv'))), validation_report_rows)
    _write_rows(Path(getattr(args, 'deployable_summary_output', Path('data/deployable_summary.csv'))), promotable_rows)
    _write_rows(equity_curve_output, equity_curve_rows)

    for summary in summary_rows:
        if str(summary.get('validation_status', 'FAIL')).upper() == 'PASS':
            print(f"[VALIDATION] Strategy {summary['strategy']} PASSED")
        else:
            print(f"[VALIDATION] Strategy {summary['strategy']} FAILED: {summary.get('validation_reasons', '')}")
    if not promotable_rows:
        print('[RESULT] No promotable strategies found')

    validation_summary: dict[str, Any] = {}

    breakout_workflow = build_backtest_workflow(breakout_rows, 'Breakout (15m)', args.execution_symbol)
    ds_workflow = build_backtest_workflow(ds_rows, 'Demand/Supply', args.execution_symbol)
    ind_workflow = build_backtest_workflow(indicator_trade_rows, 'Indicator (RSI/ADX/MACD+VWAP)', args.execution_symbol)
    amd_workflow = build_backtest_workflow(amd_rows, 'AMD + FVG + Supply/Demand', args.execution_symbol)
    one_workflow = build_backtest_workflow(one_trade_rows, 'One Trade/Day (All Indicators)', args.execution_symbol)
    btst_workflow = build_backtest_workflow(btst_rows, 'BTST', args.execution_symbol)
    all_candidates = (
        breakout_workflow.execution_candidates
        + ds_workflow.execution_candidates
        + ind_workflow.execution_candidates
        + amd_workflow.execution_candidates
        + one_workflow.execution_candidates
        + btst_workflow.execution_candidates
    )

    for candidate in all_candidates:
        candidate['timeframe'] = timeframe
        candidate['data_start'] = data_start
        candidate['data_end'] = data_end
        candidate['backtest_run_at_utc'] = run_at

    requested_execution_type = str(getattr(args, 'execution_type', 'PAPER') or 'PAPER').strip().upper()
    execution_type = requested_execution_type
    allow_live_on_pass = bool(getattr(args, 'allow_live_on_pass', False))
    allow_paper_on_fail = bool(getattr(args, 'allow_paper_on_fail', False))
    candidate_map = _build_candidate_map(all_candidates)
    candidates = _execution_candidates_for_mode(
        ranked_summary_rows,
        candidate_map,
        execution_type=execution_type,
        allow_live_on_pass=allow_live_on_pass,
        allow_paper_on_fail=allow_paper_on_fail,
    ) if execution_type in {'PAPER', 'LIVE'} else []
    crisis_overrides = apply_crisis_overrides(
        candidates,
        initial_crisis_evaluation,
        crisis_config,
        requested_execution_type=requested_execution_type,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
    )
    execution_type = str(crisis_overrides.get('execution_type', execution_type) or execution_type).upper()
    candidates = list(crisis_overrides.get('candidates', candidates))
    max_trades_per_day_override = crisis_overrides.get('max_trades_per_day')
    if max_trades_per_day_override is not None:
        max_trades_per_day = int(max_trades_per_day_override)
    max_daily_loss = crisis_overrides.get('max_daily_loss', max_daily_loss)
    max_open_trades = crisis_overrides.get('max_open_trades')
    print(
        f"[CRISIS] Routed execution={execution_type} "
        f"notes={'|'.join(crisis_overrides.get('override_notes', []) or ['NONE'])}"
    )

    executed_log_path = args.paper_log_output
    paper_rows: list[dict[str, object]] = []
    execution_summary: dict[str, Any] = {
        'requested_execution_type': requested_execution_type,
        'execution_type': execution_type,
        'execution_rows': 0,
        'executed_count': 0,
        'blocked_count': 0,
        'skipped_count': 0,
        'duplicate_trade_count': 0,
        'invalid_trade_count': 0,
        'execution_error_count': 0,
        'execution_crash_count': 0,
        'blocked_reason_counts': {},
        'cooldown_controls_enforced': 'NO',
        'duplicate_controls_enforced': 'NO',
        'paper_execution_crashes': 'NO',
    }
    if execution_type == 'NONE':
        paper_rows = []
    elif execution_type == 'LIVE':
        executed_log_path = getattr(args, 'live_log_output', Path('data/live_trading_logs_all.csv'))
        if candidates:
            live_broker = str(getattr(args, 'live_broker', 'DHAN') or 'DHAN').strip().upper()
            security_map_path = getattr(args, 'security_map', Path('data/dhan_security_map.csv'))
            security_map = None
            if live_broker != 'NONE':
                try:
                    from src.dhan_api import load_security_map  # type: ignore
                    security_map = load_security_map(security_map_path)
                except Exception:
                    security_map = None
            live_result = run_live_candidates(
                candidates,
                output_path=executed_log_path,
                deduplicate=True,
                broker_name=live_broker,
                security_map=security_map,
                max_trades_per_day=max_trades_per_day,
                max_daily_loss=max_daily_loss,
                max_open_trades=max_open_trades,
            )
            paper_rows = list(getattr(live_result.execution_result, 'rows', []))
            execution_summary = summarize_execution_result(live_result.execution_result, deduplicate_enabled=True, execution_type='LIVE')
        else:
            print('[RESULT] No eligible live candidates after validation gates')
    else:
        execution_type = 'PAPER'
        if candidates:
            paper_result = run_paper_candidates(
                candidates,
                output_path=args.paper_log_output,
                deduplicate=True,
                max_trades_per_day=max_trades_per_day,
                max_daily_loss=max_daily_loss,
                max_open_trades=max_open_trades,
            )
            paper_rows = list(getattr(paper_result.execution_result, 'rows', []))
            execution_summary = summarize_execution_result(paper_result.execution_result, deduplicate_enabled=True, execution_type='PAPER')
        else:
            print('[RESULT] No eligible paper candidates after validation gates')
    validation_output = Path(getattr(args, 'validation_output', Path('data/backtest_validation.csv')))
    validation_summary_output = validation_output.with_name(f'{validation_output.stem}_summary{validation_output.suffix}')
    if execution_type in {'PAPER', 'LIVE'} and paper_rows:
        validation_summary = summarize_trade_log(
            paper_rows,
            capital=capital,
            strategy_name='PAPER_VALIDATION',
            summary_output=validation_summary_output,
            validation_output=validation_output,
            validation=nifty_intraday_validation_config(),
            duplicate_controls_enforced=True,
        )
    elif not paper_rows:
        validation_summary = summarize_trade_log(
            [],
            capital=capital,
            strategy_name='PAPER_VALIDATION',
            summary_output=validation_summary_output,
            validation_output=validation_output,
            validation=nifty_intraday_validation_config(),
            duplicate_controls_enforced=execution_type == 'NONE',
        )

    selected_backtest_summary = best_promotable or (ranked_summary_rows[0] if ranked_summary_rows else {})
    merged_paper_summary = {**validation_summary, **execution_summary}
    go_live_evaluation = evaluate_go_live(
        selected_backtest_summary,
        merged_paper_summary,
        default_nifty_intraday_go_live_config(),
    )
    go_live_output_json = write_validation_summary_json(
        getattr(args, 'go_live_output_json', Path('data/go_live_validation_summary.json')),
        go_live_evaluation,
    )
    go_live_checklist_output = write_pass_fail_checklist_csv(
        getattr(args, 'go_live_checklist_output', Path('data/go_live_validation_checklist.csv')),
        go_live_evaluation,
    )
    final_crisis_evaluation = detect_market_stress(rows, execution_summary, crisis_config, timeframe=timeframe)
    live_permission_evaluation = evaluate_live_permission(
        go_live_evaluation,
        final_crisis_evaluation,
        crisis_config,
        requested_execution_type=requested_execution_type,
        allow_live_on_pass=allow_live_on_pass,
    )
    crisis_summary = {
        'initial_crisis_evaluation': initial_crisis_evaluation,
        'final_crisis_evaluation': final_crisis_evaluation,
        'execution_overrides': crisis_overrides,
        'live_permission': live_permission_evaluation,
    }
    crisis_output_json = write_crisis_summary_json(
        getattr(args, 'crisis_output_json', Path('data/crisis_risk_summary.json')),
        crisis_summary,
    )


    return {
        'summary_rows': summary_rows,
        'ranked_summary_rows': ranked_summary_rows,
        'promotable_rows': promotable_rows,
        'recommended_strategy': best_promotable,
        'ranking_output': str(args.ranking_output),
        'optimizer_output': str(args.optimizer_output),
        'optimizer_rows': optimizer_rows,
        'validation_output': str(validation_output),
        'validation_report_output': str(getattr(args, 'validation_report_output', Path('data/backtest_validation_report.csv'))),
        'deployable_summary_output': str(getattr(args, 'deployable_summary_output', Path('data/deployable_summary.csv'))),
        'validation_summary_output': str(validation_summary_output),
        'validation_summary': validation_summary,
        'execution_summary': execution_summary,
        'go_live_evaluation': go_live_evaluation,
        'crisis_summary': crisis_summary,
        'live_permission_evaluation': live_permission_evaluation,
        'go_live_output_json': str(go_live_output_json),
        'go_live_checklist_output': str(go_live_checklist_output),
        'crisis_output_json': str(crisis_output_json),
        'equity_curve_rows': equity_curve_rows,
        'equity_curve_output': str(equity_curve_output),
        'requested_execution_type': requested_execution_type,
        'execution_type': execution_type,
        'executed_log_path': str(executed_log_path),
        'executed_rows_count': len(paper_rows),
        'paper_rows_count': len(paper_rows),
        'timeframe': timeframe,
        'data_points': len(rows),
        'data_start': data_start,
        'data_end': data_end,
        'breakout_bias_evaluation': breakout_bias_evaluation,
    }


def main() -> None:
    args = parse_args()
    out = run(args)
    print(f"Backtest timeframe: {out['timeframe']}")
    print(f"Data points: {out['data_points']} | Start: {out['data_start']} | End: {out['data_end']}")
    print(f"Execution: {out.get('execution_type')} | Rows written: {out.get('executed_rows_count')} | Log: {out.get('executed_log_path')}")
    bias_evaluation = out.get('breakout_bias_evaluation', {})
    if bias_evaluation:
        print(
            f"Breakout bias evaluation: better={bias_evaluation.get('better_mode')} "
            f"pnl_delta={bias_evaluation.get('pnl_delta')} "
            f"win_rate_delta={bias_evaluation.get('win_rate_delta_pct')}% "
            f"trades_delta={bias_evaluation.get('trades_delta')}"
        )
    visible_rows = out.get('promotable_rows', [])
    hidden_fail_count = max(0, len(out.get('summary_rows', [])) - len(visible_rows))
    if not visible_rows:
        print('Promotable strategies: none')
    if hidden_fail_count or visible_rows:
        print(
            f"Top-level UI simplified: promotable_count={len(visible_rows)} hidden_failed_count={hidden_fail_count}; "
            f"see ranking/validation reports for full per-strategy diagnostics"
        )
    best = out.get('recommended_strategy')
    if best:
        print(
            f"Best promotable strategy: rank={best.get('rank')} strategy={best.get('strategy')} "
            f"score={best.get('real_backtest_score')} expectancy={best.get('expectancy_per_trade')} "
            f"second_half_exp={best.get('second_half_expectancy_per_trade')} pf={best.get('profit_factor')} "
            f"max_dd={best.get('max_drawdown')} dd_proven={best.get('drawdown_proven')} "
            f"exp_gap={best.get('expectancy_stability_gap_ratio')} validation={best.get('validation_status')}"
        )
    else:
        print('Best promotable strategy: none')
    print(f"Strategy ranking: {out.get('ranking_output', '')}")
    print(f"Deployable summary: {out.get('deployable_summary_output', '')}")
    print(f"Optimizer report: {out.get('optimizer_output', '')}")
    go_live_ui = out.get('go_live_evaluation', {}).get('ui_summary', {})
    print(f"Go-live status: {go_live_ui.get('go_live_status', 'UNKNOWN')}")
    print(f"Passed checks: {', '.join(go_live_ui.get('passed_checks', [])) or 'NONE'}")
    print(f"Failed checks: {', '.join(go_live_ui.get('failed_checks', [])) or 'NONE'}")
    print(f"Next action: {go_live_ui.get('next_action', '')}")
    crisis_summary = out.get('crisis_summary', {})
    final_crisis = crisis_summary.get('final_crisis_evaluation', {})
    live_permission = out.get('live_permission_evaluation', {})
    print(f"Crisis state: {final_crisis.get('stress_state', 'UNKNOWN')}")
    print(f"Crisis reasons: {', '.join(final_crisis.get('blocking_reasons', []) or final_crisis.get('warnings', [])) or 'NONE'}")
    print(f"Live permission: {live_permission.get('decision_status', 'UNKNOWN')} -> {live_permission.get('recommended_execution_type', 'UNKNOWN')}")
    print(f"Validation report: {out.get('validation_report_output', '')}")
    print(f"Go-live JSON: {out.get('go_live_output_json', '')}")
    print(f"Go-live checklist: {out.get('go_live_checklist_output', '')}")
    print(f"Crisis JSON: {out.get('crisis_output_json', '')}")
    print(f"Equity curves: {out['equity_curve_output']}")


if __name__ == '__main__':
    main()



