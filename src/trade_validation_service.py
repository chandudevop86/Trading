from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import sqrt
from statistics import median, pstdev
from typing import Any

import pandas as pd


SMALL_VALUE = 1e-9
MIN_TRADES = 40
MIN_EXPECTANCY = 0.0
MIN_PROFIT_FACTOR = 1.2
MAX_DRAWDOWN_PCT = 10.0
MIN_RECOVERY_FACTOR = 1.5
MAX_LONGEST_DRAWDOWN_STREAK_WARNING = 10
OUTSIZED_WINNER_WARNING_PCT = 0.35
RECENT_TRADE_WINDOW = 8
MIN_TRADES_FOR_ADVANCED_VALIDATION = 50
MIN_SHARPE = 0.5
MIN_SORTINO = 0.7
MAX_CONSECUTIVE_LOSSES = 6
MIN_STREAK_QUALITY_SCORE = 5.5
MIN_REGIME_CONSISTENCY_SCORE = 5.5
MAX_ONE_TRADE_PROFIT_CONCENTRATION = 0.30
MAX_TOP3_PROFIT_CONCENTRATION = 0.60
LIVE_READY_MIN_TRADES = 100
LIVE_READY_MIN_SHARPE = 1.0
LIVE_READY_MIN_SORTINO = 1.2
LIVE_READY_MIN_PROFIT_FACTOR = 1.5
LIVE_READY_MAX_DRAWDOWN_PCT = 8.0


@dataclass(frozen=True, slots=True)
class TradeEvaluationRecord:
    strategy: str
    symbol: str
    execution_type: str
    side: str
    signal_time: datetime | None
    entry_time: datetime | None
    exit_time: datetime | None
    entry_price: float
    exit_price: float
    pnl: float
    trade_status: str
    execution_status: str
    duplicate_reason: str
    validation_error: str


@dataclass(frozen=True, slots=True)
class PaperReadinessConfig:
    min_trades: int = MIN_TRADES
    min_expectancy_per_trade: float = MIN_EXPECTANCY
    min_profit_factor: float = MIN_PROFIT_FACTOR
    max_drawdown_pct: float = MAX_DRAWDOWN_PCT
    min_recovery_factor: float = MIN_RECOVERY_FACTOR
    min_trades_for_advanced_validation: int = MIN_TRADES_FOR_ADVANCED_VALIDATION
    min_sharpe: float = MIN_SHARPE
    min_sortino: float = MIN_SORTINO
    max_consecutive_losses: int = MAX_CONSECUTIVE_LOSSES
    min_streak_quality_score: float = MIN_STREAK_QUALITY_SCORE
    min_regime_consistency_score: float = MIN_REGIME_CONSISTENCY_SCORE
    max_one_trade_profit_concentration: float = MAX_ONE_TRADE_PROFIT_CONCENTRATION
    max_top3_profit_concentration: float = MAX_TOP3_PROFIT_CONCENTRATION
    max_execution_errors: int = 0
    max_duplicate_trades: int = 0
    max_invalid_trades: int = 0
    require_positive_expectancy: bool = True


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def parse_trade_timestamp(value: Any) -> datetime | None:
    raw = str(value or '').strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = None
    if parsed is None:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _record_counts_as_trade(record: TradeEvaluationRecord) -> bool:
    if record.execution_status in {'EXECUTED', 'FILLED', 'SENT', 'CLOSED', 'EXITED'}:
        return True
    if record.trade_status in {'EXECUTED', 'OPEN', 'CLOSED', 'PENDING_EXECUTION'}:
        return True
    if record.entry_time is not None and record.side in {'BUY', 'SELL'} and record.duplicate_reason == '' and record.validation_error == '' and record.trade_status not in {'BLOCKED', 'ERROR'}:
        return True
    return False


def _trade_is_closed(record: TradeEvaluationRecord) -> bool:
    if record.exit_time is not None:
        return True
    if record.execution_status in {'CLOSED', 'EXITED'}:
        return True
    if abs(record.pnl) > 0 and _record_counts_as_trade(record):
        return True
    return False


def standardize_trade_record(row: dict[str, Any]) -> TradeEvaluationRecord:
    entry_price = safe_float(row.get('entry_price', row.get('entry', row.get('price', 0.0))))
    exit_price = safe_float(row.get('exit_price', row.get('price', 0.0)))
    return TradeEvaluationRecord(
        strategy=str(row.get('strategy', row.get('source_strategy', 'TRADE')) or 'TRADE'),
        symbol=str(row.get('symbol', 'UNKNOWN') or 'UNKNOWN'),
        execution_type=str(row.get('execution_type', 'BACKTEST') or 'BACKTEST').upper(),
        side=str(row.get('side', row.get('type', '')) or '').upper(),
        signal_time=parse_trade_timestamp(row.get('signal_time', row.get('timestamp'))),
        entry_time=parse_trade_timestamp(row.get('entry_time', row.get('timestamp', row.get('signal_time')))),
        exit_time=parse_trade_timestamp(row.get('exit_time')),
        entry_price=entry_price,
        exit_price=exit_price,
        pnl=safe_float(row.get('pnl', 0.0)),
        trade_status=str(row.get('trade_status', '') or '').upper(),
        execution_status=str(row.get('execution_status', row.get('status', '')) or '').upper(),
        duplicate_reason=str(row.get('duplicate_reason', row.get('rejection_reason', '')) or '').upper(),
        validation_error=str(row.get('validation_error', row.get('rejection_reason', '')) or '').upper(),
    )


def standardize_trade_records(rows: list[dict[str, Any]]) -> list[TradeEvaluationRecord]:
    return [standardize_trade_record(dict(row)) for row in rows]


def _safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / max(float(denominator), SMALL_VALUE)


def _profit_factor_value(gross_profit: float, gross_loss_abs: float) -> tuple[float | str, str]:
    if gross_loss_abs <= 0 and gross_profit > 0:
        return 'inf', 'No losing trades in sample; profit factor is unbounded.'
    if gross_loss_abs <= 0:
        return 0.0, 'No losing trades were recorded, so profit factor is not informative yet.'
    return round(gross_profit / gross_loss_abs, 2), ''


def _equity_metrics(pnl_values: list[float]) -> dict[str, Any]:
    equity_curve: list[float] = []
    peak_curve: list[float] = []
    drawdown_amount_series: list[float] = []
    drawdown_pct_series: list[float] = []
    running_equity = 0.0
    running_peak = 0.0
    max_drawdown_amount = 0.0
    max_drawdown_pct = 0.0
    longest_drawdown_streak = 0
    current_drawdown_streak = 0

    for pnl in pnl_values:
        running_equity += pnl
        running_peak = max(running_peak, running_equity)
        drawdown_amount = running_equity - running_peak
        drawdown_pct = (drawdown_amount / running_peak) * 100.0 if running_peak > 0 else 0.0
        equity_curve.append(round(running_equity, 2))
        peak_curve.append(round(running_peak, 2))
        drawdown_amount_series.append(round(drawdown_amount, 2))
        drawdown_pct_series.append(round(drawdown_pct, 2))
        max_drawdown_amount = min(max_drawdown_amount, drawdown_amount)
        max_drawdown_pct = min(max_drawdown_pct, drawdown_pct)
        if drawdown_amount < 0:
            current_drawdown_streak += 1
            longest_drawdown_streak = max(longest_drawdown_streak, current_drawdown_streak)
        else:
            current_drawdown_streak = 0

    return {
        'equity_curve': equity_curve,
        'peak_equity_curve': peak_curve,
        'drawdown_amount_series': drawdown_amount_series,
        'drawdown_pct_series': drawdown_pct_series,
        'max_drawdown_amount': round(max_drawdown_amount, 2),
        'max_drawdown_pct': round(abs(max_drawdown_pct), 2),
        'longest_drawdown_streak': int(longest_drawdown_streak),
    }
def _streak_lengths(pnl_values: list[float], positive: bool) -> list[int]:
    streaks: list[int] = []
    current = 0
    for pnl in pnl_values:
        match = pnl > 0 if positive else pnl < 0
        if match:
            current += 1
        elif current > 0:
            streaks.append(current)
            current = 0
    if current > 0:
        streaks.append(current)
    return streaks


def _max_consecutive_counts(pnl_values: list[float]) -> tuple[int, int, int, int, float, float]:
    consecutive_wins_max = 0
    consecutive_losses_max = 0
    current_wins = 0
    current_losses = 0
    for pnl in pnl_values:
        if pnl > 0:
            current_wins += 1
            current_losses = 0
        elif pnl < 0:
            current_losses += 1
            current_wins = 0
        else:
            current_wins = 0
            current_losses = 0
        consecutive_wins_max = max(consecutive_wins_max, current_wins)
        consecutive_losses_max = max(consecutive_losses_max, current_losses)
    win_streaks = _streak_lengths(pnl_values, True)
    loss_streaks = _streak_lengths(pnl_values, False)
    avg_win_streak = round(sum(win_streaks) / len(win_streaks), 2) if win_streaks else 0.0
    avg_loss_streak = round(sum(loss_streaks) / len(loss_streaks), 2) if loss_streaks else 0.0
    return consecutive_wins_max, consecutive_losses_max, current_wins, current_losses, avg_win_streak, avg_loss_streak


def _closed_trade_pairs(rows: list[dict[str, Any]]) -> list[tuple[TradeEvaluationRecord, dict[str, Any]]]:
    pairs = [(standardize_trade_record(dict(row)), dict(row)) for row in rows]
    closed = [pair for pair in pairs if _record_counts_as_trade(pair[0]) and _trade_is_closed(pair[0])]
    return sorted(closed, key=lambda item: item[0].exit_time or item[0].entry_time or item[0].signal_time or datetime.min)


def _return_series(closed_pairs: list[tuple[TradeEvaluationRecord, dict[str, Any]]], average_loss_magnitude: float) -> list[float]:
    returns: list[float] = []
    normalizer_fallback = max(average_loss_magnitude, 1.0)
    for record, _ in closed_pairs:
        base = abs(record.entry_price) if abs(record.entry_price) > 0 else normalizer_fallback
        returns.append(record.pnl / max(base, SMALL_VALUE))
    return returns


def _ratio_label(value: float | str, *, weak: float, usable: float, good: float) -> str:
    if str(value).strip().lower() == 'inf':
        return 'STRONG'
    numeric = safe_float(value)
    if numeric < 0:
        return 'POOR'
    if numeric < weak:
        return 'WEAK'
    if numeric < usable:
        return 'USABLE'
    if numeric < good:
        return 'GOOD'
    return 'STRONG'


def _risk_adjusted_metrics(closed_pairs: list[tuple[TradeEvaluationRecord, dict[str, Any]]], average_loss_magnitude: float) -> dict[str, Any]:
    returns = _return_series(closed_pairs, average_loss_magnitude)
    if not returns:
        return {
            'return_series': [],
            'mean_return': 0.0,
            'std_return': 0.0,
            'sharpe_ratio': 0.0,
            'sharpe_label': 'POOR',
            'downside_return_series': [],
            'downside_deviation': 0.0,
            'sortino_ratio': 0.0,
            'sortino_label': 'POOR',
        }
    mean_return = sum(returns) / len(returns)
    std_return = pstdev(returns) if len(returns) > 1 else 0.0
    sharpe_ratio = round(mean_return / max(std_return, SMALL_VALUE), 2) if len(returns) >= 2 else 0.0
    downside_returns = [value for value in returns if value < 0]
    downside_deviation = sqrt(sum(value * value for value in downside_returns) / len(downside_returns)) if downside_returns else 0.0
    if len(returns) < 2:
        sortino_ratio: float | str = 0.0
    elif downside_returns:
        sortino_ratio = round(mean_return / max(downside_deviation, SMALL_VALUE), 2)
    elif mean_return > 0:
        sortino_ratio = 'inf'
    else:
        sortino_ratio = 0.0
    return {
        'return_series': [round(value, 6) for value in returns],
        'mean_return': round(mean_return, 6),
        'std_return': round(std_return, 6),
        'sharpe_ratio': sharpe_ratio,
        'sharpe_label': _ratio_label(sharpe_ratio, weak=0.5, usable=1.0, good=1.5),
        'downside_return_series': [round(value, 6) for value in downside_returns],
        'downside_deviation': round(downside_deviation, 6),
        'sortino_ratio': sortino_ratio,
        'sortino_label': _ratio_label(sortino_ratio, weak=0.7, usable=1.2, good=1.8),
    }


def _streak_quality_label(score: float) -> str:
    if score >= 7.5:
        return 'STABLE'
    if score >= 5.5:
        return 'ACCEPTABLE'
    if score >= 3.5:
        return 'UNSTABLE'
    return 'DANGEROUS'


def _streak_metrics(pnl_values: list[float], wins: int, losses: int) -> dict[str, Any]:
    consecutive_wins_max, consecutive_losses_max, current_win_streak, current_loss_streak, avg_win_streak, avg_loss_streak = _max_consecutive_counts(pnl_values)
    win_streaks = _streak_lengths(pnl_values, True)
    loss_streaks = _streak_lengths(pnl_values, False)
    streak_balance_score = round(max(0.0, 10.0 - max(consecutive_losses_max - consecutive_wins_max, 0) * 1.2), 2)
    loss_cluster_score = round(max(0.0, 10.0 - (consecutive_losses_max * 1.0) - (avg_loss_streak * 0.8)), 2)
    dominant_win_streak_share = consecutive_wins_max / max(wins, 1)
    win_distribution_score = round(max(0.0, 10.0 - max(dominant_win_streak_share - 0.4, 0.0) * 15.0), 2)
    streak_quality_score = round((streak_balance_score * 0.30) + (loss_cluster_score * 0.45) + (win_distribution_score * 0.25), 2)
    return {
        'consecutive_wins_max': consecutive_wins_max,
        'consecutive_losses_max': consecutive_losses_max,
        'current_win_streak': current_win_streak,
        'current_loss_streak': current_loss_streak,
        'number_of_win_streaks': len(win_streaks),
        'average_win_streak_length': avg_win_streak,
        'number_of_loss_streaks': len(loss_streaks),
        'average_loss_streak_length': avg_loss_streak,
        'streak_balance_score': streak_balance_score,
        'loss_cluster_score': loss_cluster_score,
        'win_distribution_score': win_distribution_score,
        'streak_quality_score': streak_quality_score,
        'streak_quality_label': _streak_quality_label(streak_quality_score),
    }


def _row_regime(row: dict[str, Any]) -> str:
    volatility_ratio = safe_float(row.get('volatility_ratio', row.get('atr_pct', 0.0)))
    atr_pct = safe_float(row.get('atr_pct'))
    opening_vol = safe_float(row.get('opening_volatility_pct'))
    market_state = str(row.get('market_state', row.get('volatility_market_state', '')) or '').strip().upper()
    trend_bias = str(row.get('trend_bias', row.get('day_bias', '')) or '').strip().upper()
    trend_alignment = str(row.get('trend_alignment', row.get('market_structure_label', '')) or '').strip().upper()
    if volatility_ratio >= 1.35 or atr_pct >= 0.8 or opening_vol >= 0.5 or market_state in {'EXPLOSIVE', 'HIGH_VOL'}:
        return 'high-volatility'
    if (0 < volatility_ratio <= 0.9) or (0 < atr_pct < 0.3) or (0 < opening_vol < 0.2) or market_state in {'QUIET', 'LOW_VOL'}:
        return 'low-volatility'
    if trend_bias in {'BULLISH', 'BEARISH', 'UP', 'DOWN'} or trend_alignment in {'YES', 'HH_HL', 'LH_LL', 'TRENDING'}:
        return 'trending'
    return 'ranging'

def _regime_pass_fail(metrics: dict[str, Any], cfg: PaperReadinessConfig) -> tuple[str, list[str]]:
    reasons: list[str] = []
    trades = safe_int(metrics.get('total_trades'))
    expectancy = safe_float(metrics.get('expectancy_per_trade'))
    pf = metrics.get('profit_factor', 0.0)
    pf_value = float('inf') if str(pf).strip().lower() == 'inf' else safe_float(pf)
    dd = safe_float(metrics.get('max_drawdown_pct'))
    if trades < 10:
        return 'NEED_MORE_DATA', ['small regime sample']
    if expectancy <= 0:
        reasons.append('negative expectancy')
    if pf_value != float('inf') and pf_value < cfg.min_profit_factor:
        reasons.append('profit factor below threshold')
    if dd > cfg.max_drawdown_pct:
        reasons.append('drawdown too large')
    return ('FAIL', reasons) if reasons else ('PASS', [])


def _regime_consistency_label(score: float) -> str:
    if score >= 7.5:
        return 'ROBUST'
    if score >= 5.5:
        return 'DECENT'
    if score >= 3.5:
        return 'FRAGILE'
    return 'DEPENDENT'


def _regime_metrics(rows: list[dict[str, Any]], cfg: PaperReadinessConfig) -> dict[str, Any]:
    closed_pairs = _closed_trade_pairs(rows)
    regime_groups: dict[str, list[dict[str, Any]]] = {'trending': [], 'ranging': [], 'high-volatility': [], 'low-volatility': []}
    for record, raw in closed_pairs:
        merged = dict(raw)
        merged.setdefault('pnl', record.pnl)
        regime_groups[_row_regime(merged)].append(merged)

    regime_metrics: dict[str, dict[str, Any]] = {}
    traded_regimes: list[tuple[str, dict[str, Any]]] = []
    for regime, regime_rows in regime_groups.items():
        metrics = calculate_trade_metrics(regime_rows, strategy_name=regime.upper()) if regime_rows else {
            'total_trades': 0,
            'net_profit': 0.0,
            'expectancy_per_trade': 0.0,
            'profit_factor': 0.0,
            'win_rate': 0.0,
            'max_drawdown_pct': 0.0,
        }
        status, warnings = _regime_pass_fail(metrics, cfg)
        regime_metrics[regime] = {
            'total_trades': safe_int(metrics.get('closed_trades', metrics.get('total_trades'))),
            'net_profit': round(safe_float(metrics.get('net_profit', metrics.get('total_pnl'))), 2),
            'expectancy_per_trade': round(safe_float(metrics.get('expectancy_per_trade')), 2),
            'profit_factor': metrics.get('profit_factor', 0.0),
            'win_rate': round(safe_float(metrics.get('win_rate')), 2),
            'max_drawdown_pct': round(safe_float(metrics.get('max_drawdown_pct')), 2),
            'pass_fail_status': status,
            'warnings': warnings,
        }
        if regime_metrics[regime]['total_trades'] > 0:
            traded_regimes.append((regime, regime_metrics[regime]))

    regime_pass_count = sum(1 for _, metrics in traded_regimes if metrics['pass_fail_status'] == 'PASS')
    regime_fail_count = sum(1 for _, metrics in traded_regimes if metrics['pass_fail_status'] == 'FAIL')
    dominant_regime = max(traded_regimes, key=lambda item: item[1]['net_profit'])[0] if traded_regimes else 'none'
    weakest_regime = min(traded_regimes, key=lambda item: item[1]['expectancy_per_trade'])[0] if traded_regimes else 'none'
    profitable_regimes = sum(1 for _, metrics in traded_regimes if metrics['net_profit'] > 0 and metrics['expectancy_per_trade'] > 0)
    regime_profit_total = sum(max(metrics['net_profit'], 0.0) for _, metrics in traded_regimes)
    dominant_profit = max((max(metrics['net_profit'], 0.0) for _, metrics in traded_regimes), default=0.0)
    regime_profit_concentration = round(_safe_ratio(dominant_profit, regime_profit_total), 4) if regime_profit_total > 0 else 0.0
    breadth_score = _safe_ratio(profitable_regimes, max(len(traded_regimes), 1)) * 10.0 if traded_regimes else 0.0
    penalty = regime_fail_count * 1.2 + max(regime_profit_concentration - 0.50, 0.0) * 10.0
    regime_consistency_score = round(max(0.0, min(10.0, breadth_score - penalty + 2.0)), 2)
    return {
        'regime_metrics': regime_metrics,
        'regime_pass_count': regime_pass_count,
        'regime_fail_count': regime_fail_count,
        'dominant_regime': dominant_regime,
        'weakest_regime': weakest_regime,
        'regime_consistency_score': regime_consistency_score,
        'regime_consistency_label': _regime_consistency_label(regime_consistency_score),
        'regime_profit_concentration': regime_profit_concentration,
    }


def _advanced_validation_metrics(rows: list[dict[str, Any]], summary: dict[str, Any], cfg: PaperReadinessConfig) -> dict[str, Any]:
    closed_pairs = _closed_trade_pairs(rows)
    average_loss = safe_float(summary.get('average_loss'))
    pnl_values = [pair[0].pnl for pair in closed_pairs]
    risk_adjusted = _risk_adjusted_metrics(closed_pairs, average_loss)
    streaks = _streak_metrics(pnl_values, safe_int(summary.get('wins')), safe_int(summary.get('losses')))
    regimes = _regime_metrics(rows, cfg)
    winning_pnl = sorted((pair[0].pnl for pair in closed_pairs if pair[0].pnl > 0), reverse=True)
    gross_profit = safe_float(summary.get('gross_profit'))
    one_trade_profit_concentration = round(_safe_ratio(winning_pnl[0], gross_profit), 4) if gross_profit > 0 and winning_pnl else 0.0
    top_3_trade_profit_concentration = round(_safe_ratio(sum(winning_pnl[:3]), gross_profit), 4) if gross_profit > 0 and winning_pnl else 0.0
    warnings: list[str] = []
    if one_trade_profit_concentration > cfg.max_one_trade_profit_concentration:
        warnings.append(f'One trade contributed {one_trade_profit_concentration * 100:.0f}% of total profits.')
    if top_3_trade_profit_concentration > cfg.max_top3_profit_concentration:
        warnings.append(f'Top 3 trades contributed {top_3_trade_profit_concentration * 100:.0f}% of total profits.')
    if regimes['regime_profit_concentration'] > 0.60:
        warnings.append(f"Most profits came from {regimes['dominant_regime']} regime.")
    if streaks['consecutive_losses_max'] > cfg.max_consecutive_losses:
        warnings.append('Losses cluster too aggressively for confident live deployment.')
    if streaks['win_distribution_score'] < 4.5:
        warnings.append('Performance depends on a few win streaks.')
    if regimes['weakest_regime'] != 'none' and regimes['regime_fail_count'] > 0:
        warnings.append(f"Strategy underperforms in {regimes['weakest_regime']} regime.")
    return {
        **risk_adjusted,
        **streaks,
        **regimes,
        'one_trade_profit_concentration': one_trade_profit_concentration,
        'top_3_trade_profit_concentration': top_3_trade_profit_concentration,
        'advanced_validation_warnings': warnings,
    }


def _validation_warnings(summary: dict[str, Any], cfg: PaperReadinessConfig) -> list[str]:
    warnings: list[str] = []
    closed_trades = safe_int(summary.get('closed_trades'))
    if closed_trades < max(int(cfg.min_trades), 1) * 2:
        warnings.append('Sample size is still small for strong confidence.')
    largest_win_pct = safe_float(summary.get('largest_win_pct_of_total_profit'))
    if largest_win_pct > OUTSIZED_WINNER_WARNING_PCT * 100.0:
        warnings.append('Results concentrated in one outsized winner.')
    recent_pnl = summary.get('recent_closed_pnl', []) or []
    if recent_pnl and sum(float(value) for value in recent_pnl) <= 0:
        warnings.append('Recent closed trades are not profitable.')
    if safe_int(summary.get('longest_drawdown_streak')) > MAX_LONGEST_DRAWDOWN_STREAK_WARNING:
        warnings.append('Equity curve shows a long drawdown streak.')
    if safe_float(summary.get('pnl_std_dev')) > max(abs(safe_float(summary.get('expectancy_per_trade'))) * 4.0, 1.0):
        warnings.append('Equity curve looks unstable relative to expectancy.')
    if closed_trades < int(cfg.min_trades):
        warnings.append('Trade sample is too small for intraday validation.')
    if safe_float(summary.get('one_trade_profit_concentration')) > cfg.max_one_trade_profit_concentration:
        warnings.append('Results depend too much on one oversized winner.')
    if safe_float(summary.get('top_3_trade_profit_concentration')) > cfg.max_top3_profit_concentration:
        warnings.append('Results depend too much on the top 3 winners.')
    if safe_float(summary.get('regime_profit_concentration')) > 0.60:
        warnings.append('Strategy is regime-dependent.')
    if str(summary.get('profit_factor_note', '')).strip():
        warnings.append(str(summary['profit_factor_note']).strip())
    for extra in summary.get('advanced_validation_warnings', []) or []:
        if extra not in warnings:
            warnings.append(extra)
    return warnings

def _confidence_label(summary: dict[str, Any], status: str, cfg: PaperReadinessConfig) -> str:
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    expectancy_pct = safe_float(summary.get('expectancy_pct'))
    profit_factor = summary.get('profit_factor', 0.0)
    profit_factor_value = float('inf') if str(profit_factor).strip().lower() == 'inf' else safe_float(profit_factor)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    recovery_factor = safe_float(summary.get('recovery_factor'))
    sharpe_ratio = summary.get('sharpe_ratio', 0.0)
    sharpe_value = float('inf') if str(sharpe_ratio).strip().lower() == 'inf' else safe_float(sharpe_ratio)
    sortino_ratio = summary.get('sortino_ratio', 0.0)
    sortino_value = float('inf') if str(sortino_ratio).strip().lower() == 'inf' else safe_float(sortino_ratio)
    regime_consistency = safe_float(summary.get('regime_consistency_score'))
    total_trades = safe_int(summary.get('closed_trades', summary.get('total_trades')))
    if status == 'NEED_MORE_DATA':
        return 'NEED_MORE_DATA'
    if status == 'FAIL':
        if expectancy <= 0 and profit_factor_value < 1.0 and max_drawdown_pct > cfg.max_drawdown_pct:
            return 'HARD FAIL'
        return 'FAIL'
    near_threshold = (
        expectancy_pct <= 0.10
        or profit_factor_value < (cfg.min_profit_factor + 0.1)
        or max_drawdown_pct > (cfg.max_drawdown_pct * 0.85)
        or recovery_factor < (cfg.min_recovery_factor + 0.3)
        or sharpe_value < (cfg.min_sharpe + 0.15)
        or sortino_value < (cfg.min_sortino + 0.15)
        or regime_consistency < (cfg.min_regime_consistency_score + 0.5)
    )
    if total_trades >= cfg.min_trades * 2 and expectancy_pct >= 0.5 and profit_factor_value >= 2.0 and max_drawdown_pct <= cfg.max_drawdown_pct * 0.6 and recovery_factor >= 2.0 and sharpe_value >= 1.0 and sortino_value >= 1.2 and regime_consistency >= 6.5:
        return 'STRONG PASS'
    if near_threshold:
        return 'BORDERLINE'
    return 'PASS'


def calculate_trade_metrics(rows: list[dict[str, Any]], *, strategy_name: str = 'TRADE_SYSTEM') -> dict[str, Any]:
    records = standardize_trade_records(rows)
    trade_records = [record for record in records if _record_counts_as_trade(record)]
    closed_records = [record for record in trade_records if _trade_is_closed(record)]
    ordered_closed = sorted(closed_records, key=lambda record: record.exit_time or record.entry_time or record.signal_time or datetime.min)
    pnl_values = [float(record.pnl) for record in ordered_closed]
    winning_pnl = [value for value in pnl_values if value > 0]
    losing_pnl = [value for value in pnl_values if value < 0]
    wins = len(winning_pnl)
    losses = len(losing_pnl)
    closed_trade_count = len(ordered_closed)
    win_rate_ratio = wins / closed_trade_count if closed_trade_count else 0.0
    loss_rate = losses / closed_trade_count if closed_trade_count else 0.0
    average_win = round(sum(winning_pnl) / wins, 2) if wins else 0.0
    average_loss_magnitude = round(abs(sum(losing_pnl)) / losses, 2) if losses else 0.0
    average_loss = round(sum(losing_pnl) / losses, 2) if losses else 0.0
    payoff_ratio = round(_safe_ratio(average_win, average_loss_magnitude), 2) if average_loss_magnitude > 0 else (float('inf') if average_win > 0 else 0.0)
    expectancy_per_trade = round((win_rate_ratio * average_win) - (loss_rate * average_loss_magnitude), 2) if closed_trade_count else 0.0
    expectancy_pct = round(_safe_ratio(expectancy_per_trade, average_loss_magnitude) * 100.0, 2) if average_loss_magnitude > 0 else (0.0 if expectancy_per_trade <= 0 else 100.0)
    gross_profit = round(sum(winning_pnl), 2)
    gross_loss = round(sum(losing_pnl), 2)
    gross_loss_abs = abs(gross_loss)
    profit_factor, profit_factor_note = _profit_factor_value(gross_profit, gross_loss_abs)
    net_profit = round(sum(pnl_values), 2)
    equity = _equity_metrics(pnl_values)
    recovery_factor = round(_safe_ratio(net_profit, abs(equity['max_drawdown_amount'])), 2) if abs(equity['max_drawdown_amount']) > 0 else (0.0 if net_profit <= 0 else float('inf'))
    consecutive_wins_max, consecutive_losses_max, current_win_streak, current_loss_streak, avg_win_streak, avg_loss_streak = _max_consecutive_counts(pnl_values)
    largest_win = round(max(winning_pnl), 2) if winning_pnl else 0.0
    largest_loss = round(min(losing_pnl), 2) if losing_pnl else 0.0
    largest_win_pct_of_total_profit = round(_safe_ratio(largest_win, gross_profit) * 100.0, 2) if gross_profit > 0 else 0.0
    median_trade_pnl = round(float(median(pnl_values)), 2) if pnl_values else 0.0
    pnl_std_dev = round(float(pstdev(pnl_values)), 2) if len(pnl_values) > 1 else 0.0
    duplicate_trade_count = sum(1 for record in records if record.duplicate_reason.startswith('DUPLICATE_'))
    execution_error_count = sum(1 for record in records if record.execution_status == 'ERROR' or record.validation_error == 'BROKER_ERROR')
    invalid_trade_count = sum(1 for record in records if bool(record.validation_error and record.validation_error != 'BROKER_ERROR'))
    recent_closed_pnl = pnl_values[-RECENT_TRADE_WINDOW:]

    summary = {
        'strategy': strategy_name,
        'total_trades': len(trade_records),
        'closed_trades': closed_trade_count,
        'open_trades': max(len(trade_records) - closed_trade_count, 0),
        'wins': wins,
        'losses': losses,
        'win_rate': round(win_rate_ratio * 100.0, 2),
        'win_rate_ratio': round(win_rate_ratio, 4),
        'loss_rate': round(loss_rate, 4),
        'loss_rate_pct': round(loss_rate * 100.0, 2),
        'avg_win': average_win,
        'average_win': average_win,
        'avg_loss': average_loss,
        'average_loss': average_loss_magnitude,
        'reward_risk_ratio': payoff_ratio,
        'payoff_ratio': payoff_ratio,
        'gross_profit': gross_profit,
        'gross_loss': gross_loss,
        'net_profit': net_profit,
        'total_pnl': net_profit,
        'expectancy_per_trade': expectancy_per_trade,
        'expectancy_pct': expectancy_pct,
        'profit_factor': profit_factor,
        'profit_factor_note': profit_factor_note,
        'largest_win': largest_win,
        'largest_loss': largest_loss,
        'largest_win_pct_of_total_profit': largest_win_pct_of_total_profit,
        'consecutive_wins_max': consecutive_wins_max,
        'consecutive_losses_max': consecutive_losses_max,
        'current_win_streak': current_win_streak,
        'current_loss_streak': current_loss_streak,
        'average_win_streak_length': avg_win_streak,
        'average_loss_streak_length': avg_loss_streak,
        'longest_win_streak': consecutive_wins_max,
        'longest_loss_streak': consecutive_losses_max,
        'median_trade_pnl': median_trade_pnl,
        'pnl_std_dev': pnl_std_dev,
        'duplicate_trade_count': duplicate_trade_count,
        'execution_error_count': execution_error_count,
        'invalid_trade_count': invalid_trade_count,
        'recent_closed_pnl': [round(value, 2) for value in recent_closed_pnl],
        'drawdown_proven': 'YES' if abs(equity['max_drawdown_amount']) > 0 and losses > 0 else 'NO',
        **equity,
    }
    summary['max_drawdown'] = abs(summary['max_drawdown_amount'])
    return summary


def metrics_frame(summary: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {'metric': 'Total trades', 'value': safe_int(summary.get('total_trades'))},
        {'metric': 'Closed trades', 'value': safe_int(summary.get('closed_trades'))},
        {'metric': 'Expectancy / trade', 'value': round(safe_float(summary.get('expectancy_per_trade')), 2)},
        {'metric': 'Profit factor', 'value': summary.get('profit_factor', 0.0)},
        {'metric': 'Sharpe', 'value': summary.get('sharpe_ratio', 0.0)},
        {'metric': 'Sortino', 'value': summary.get('sortino_ratio', 0.0)},
        {'metric': 'Max drawdown %', 'value': round(safe_float(summary.get('max_drawdown_pct')), 2)},
        {'metric': 'Streak quality', 'value': round(safe_float(summary.get('streak_quality_score')), 2)},
        {'metric': 'Regime consistency', 'value': round(safe_float(summary.get('regime_consistency_score')), 2)},
        {'metric': 'Decision', 'value': str(summary.get('pass_fail_status', 'NA'))},
        {'metric': 'Go live', 'value': str(summary.get('go_live_status', 'NA'))},
        {'metric': 'Confidence', 'value': str(summary.get('deployment_confidence_label', summary.get('confidence_label', 'NA')))},
    ]
    return pd.DataFrame(rows)

def terminal_lines(summary: dict[str, Any]) -> list[str]:
    decision = str(summary.get('pass_fail_status', 'NA'))
    confidence = str(summary.get('confidence_label', 'NA'))
    go_live = str(summary.get('go_live_status', 'NA'))
    lines = [
        'Validation Summary',
        f"Total trades: {safe_int(summary.get('total_trades'))}",
        f"- Trades: {safe_int(summary.get('closed_trades', summary.get('total_trades')))}",
        f"- Expectancy: {safe_float(summary.get('expectancy_per_trade')):.2f}",
        f"- Profit Factor: {summary.get('profit_factor', 0.0)}",
        f"- Max Drawdown: {safe_float(summary.get('max_drawdown_pct')):.2f}%",
        f"- Recovery Factor: {summary.get('recovery_factor', 0.0)}",
        f"- Decision: {decision} ({confidence})",
        'Advanced Validation Summary',
        f"- Sharpe: {summary.get('sharpe_ratio', 0.0)}",
        f"- Sortino: {summary.get('sortino_ratio', 0.0)}",
        f"- Streak Quality: {safe_float(summary.get('streak_quality_score')):.2f}",
        f"- Regime Consistency: {safe_float(summary.get('regime_consistency_score')):.2f}",
        f"- Dominant Regime: {summary.get('dominant_regime', 'none')}",
        f"- Weakest Regime: {summary.get('weakest_regime', 'none')}",
        f"- Go Live Status: {go_live}",
    ]
    for reason in summary.get('pass_fail_reasons', []) or []:
        lines.append(f"- {reason}")
    for reason in summary.get('go_live_reasons', []) or []:
        lines.append(f"- {reason}")
    for warning in summary.get('warnings', []) or []:
        lines.append(f"- WARNING: {warning}")
    for warning in summary.get('go_live_warnings', []) or []:
        if warning not in summary.get('warnings', []):
            lines.append(f"- WARNING: {warning}")
    return lines


def evaluate_paper_readiness(summary: dict[str, Any], config: PaperReadinessConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperReadinessConfig()
    total_trades = safe_int(summary.get('closed_trades', summary.get('total_trades')))
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    recovery_factor = summary.get('recovery_factor', 0.0)
    recovery_factor_value = float('inf') if str(recovery_factor).strip().lower() == 'inf' else safe_float(recovery_factor)
    execution_error_count = safe_int(summary.get('execution_error_count'))
    duplicate_trade_count = safe_int(summary.get('duplicate_trade_count'))
    invalid_trade_count = safe_int(summary.get('invalid_trade_count'))

    reasons: list[str] = []
    blocker_messages: list[str] = []
    status = 'PASS'
    if total_trades < cfg.min_trades:
        status = 'NEED_MORE_DATA'
        reasons.append(f'NEED_MORE_DATA: only {total_trades} trades')
        blocker_messages.append('not enough paper trades yet')
    largest_win_pct = safe_float(summary.get('largest_win_pct_of_total_profit'))
    if cfg.require_positive_expectancy and expectancy <= cfg.min_expectancy_per_trade:
        reasons.append('FAIL: negative expectancy')
        blocker_messages.append('expectancy is not positive yet')
    if profit_factor != float('inf') and profit_factor < cfg.min_profit_factor:
        reasons.append('FAIL: profit factor below threshold')
        blocker_messages.append('profit factor is below target')
    if max_drawdown_pct > cfg.max_drawdown_pct:
        reasons.append('FAIL: drawdown too large')
        blocker_messages.append('drawdown is above the current paper limit')
    if recovery_factor_value != float('inf') and recovery_factor_value < cfg.min_recovery_factor:
        reasons.append('FAIL: recovery factor below threshold')
        blocker_messages.append('recovery factor is below target')
    if execution_error_count > cfg.max_execution_errors:
        reasons.append('FAIL: execution errors present')
        blocker_messages.append('execution errors are still present')
    if duplicate_trade_count > cfg.max_duplicate_trades:
        reasons.append('FAIL: duplicate trades not fully blocked')
        blocker_messages.append('duplicate trades were not fully blocked')
    if invalid_trade_count > cfg.max_invalid_trades:
        reasons.append('FAIL: invalid trade rows still present')
        blocker_messages.append('invalid trade rows are still present')
    if status != 'NEED_MORE_DATA' and largest_win_pct > OUTSIZED_WINNER_WARNING_PCT * 100.0:
        reasons.append('FAIL: results depend too much on one oversized winner')
        blocker_messages.append('results are too concentrated in one oversized winner')
    if status == 'PASS' and reasons:
        status = 'FAIL'

    warnings = _validation_warnings(summary, cfg)
    confidence_label = _confidence_label(summary, status, cfg)
    if status == 'PASS':
        blocker_text = ''
        readiness_summary = 'Validation passed. The strategy currently meets the paper-trading validation gate.'
        next_step = 'Continue paper deployment and keep monitoring before any live decision.'
    elif status == 'NEED_MORE_DATA':
        blocker_text = '; '.join(blocker_messages or ['not enough paper trades yet'])
        readiness_summary = f'Validation needs more data because {blocker_text.lower()}.'
        next_step = 'Stay in paper trading. Keep collecting closed trades before making any pass/fail decision.'
    else:
        blocker_text = '; '.join(blocker_messages or reasons)
        readiness_summary = f'Validation failed because {blocker_text.lower()}.'
        next_step = 'Stay in paper trading. Clear the listed blockers before any live decision.'

    return {
        'paper_ready': 'YES' if status == 'PASS' else 'NO',
        'paper_readiness_status': 'READY_FOR_PAPER_PHASE' if status == 'PASS' else 'NOT_READY_FOR_PAPER_PHASE',
        'paper_readiness_blockers': blocker_text,
        'paper_readiness_summary': readiness_summary,
        'paper_readiness_next_step': next_step,
        'pass_fail_status': status,
        'pass_fail_reasons': reasons,
        'warnings': warnings,
        'confidence_label': confidence_label,
    }


def evaluate_go_live_readiness(summary: dict[str, Any], config: PaperReadinessConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperReadinessConfig()
    trades = safe_int(summary.get('closed_trades', summary.get('total_trades')))
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    pf_raw = summary.get('profit_factor', 0.0)
    pf = float('inf') if str(pf_raw).strip().lower() == 'inf' else safe_float(pf_raw)
    dd = safe_float(summary.get('max_drawdown_pct'))
    sharpe = safe_float(summary.get('sharpe_ratio'))
    sortino_raw = summary.get('sortino_ratio', 0.0)
    sortino = float('inf') if str(sortino_raw).strip().lower() == 'inf' else safe_float(sortino_raw)
    streak_quality = safe_float(summary.get('streak_quality_score'))
    regime_consistency = safe_float(summary.get('regime_consistency_score'))
    one_trade_concentration = safe_float(summary.get('one_trade_profit_concentration'))
    top3_concentration = safe_float(summary.get('top_3_trade_profit_concentration'))
    pass_fail_status = str(summary.get('pass_fail_status', 'FAIL'))

    reasons: list[str] = []
    warnings: list[str] = list(summary.get('advanced_validation_warnings', []) or [])
    go_live_status = 'PAPER_ONLY'

    if pass_fail_status == 'FAIL' or expectancy <= 0 or (pf != float('inf') and pf < cfg.min_profit_factor) or dd > cfg.max_drawdown_pct or streak_quality < 3.5 or regime_consistency < 3.5:
        go_live_status = 'REJECT'
        if expectancy <= 0:
            reasons.append('REJECT: negative expectancy')
        if pf != float('inf') and pf < cfg.min_profit_factor:
            reasons.append('REJECT: profit factor below minimum')
        if dd > cfg.max_drawdown_pct:
            reasons.append('REJECT: drawdown too high')
        if streak_quality < 3.5:
            reasons.append('REJECT: streak quality is dangerous')
        if regime_consistency < 3.5:
            reasons.append('REJECT: regime consistency is too poor')
    elif trades < cfg.min_trades_for_advanced_validation or one_trade_concentration > cfg.max_one_trade_profit_concentration or top3_concentration > cfg.max_top3_profit_concentration or regime_consistency < cfg.min_regime_consistency_score:
        go_live_status = 'PAPER_ONLY'
        if trades < cfg.min_trades_for_advanced_validation:
            reasons.append('PAPER_ONLY: advanced sample size is too small')
        if one_trade_concentration > cfg.max_one_trade_profit_concentration:
            reasons.append('PAPER_ONLY: too much dependence on one winner')
        if top3_concentration > cfg.max_top3_profit_concentration:
            reasons.append('PAPER_ONLY: top 3 winners dominate results')
        if regime_consistency < cfg.min_regime_consistency_score:
            reasons.append('PAPER_ONLY: regime behavior is too inconsistent')
    elif sharpe < cfg.min_sharpe or sortino < cfg.min_sortino or streak_quality < cfg.min_streak_quality_score:
        go_live_status = 'SHADOW_LIVE'
        if sharpe < cfg.min_sharpe:
            reasons.append('SHADOW_LIVE: Sharpe ratio is still weak')
        if sortino < cfg.min_sortino:
            reasons.append('SHADOW_LIVE: Sortino ratio is still weak')
        if streak_quality < cfg.min_streak_quality_score:
            reasons.append('SHADOW_LIVE: streak quality is still weak')
    elif trades >= LIVE_READY_MIN_TRADES and sharpe >= LIVE_READY_MIN_SHARPE and sortino >= LIVE_READY_MIN_SORTINO and (pf == float('inf') or pf >= LIVE_READY_MIN_PROFIT_FACTOR) and dd <= LIVE_READY_MAX_DRAWDOWN_PCT and streak_quality >= 7.0 and regime_consistency >= 6.5 and one_trade_concentration <= 0.25 and top3_concentration <= 0.55:
        go_live_status = 'LIVE_READY'
        reasons.append('LIVE_READY: strong risk-adjusted returns and acceptable stability')
    else:
        go_live_status = 'SMALL_LIVE'
        reasons.append('SMALL_LIVE: metrics pass, but keep capital allocation small')

    if safe_int(summary.get('consecutive_losses_max')) > cfg.max_consecutive_losses:
        warnings.append('Consecutive loss streaks are too long for confident live deployment.')
    if safe_float(summary.get('regime_profit_concentration')) > 0.60:
        warnings.append(f"Most profits came from {summary.get('dominant_regime', 'one')} regime.")

    if go_live_status == 'LIVE_READY':
        deployment_confidence_label = 'HIGH'
    elif go_live_status == 'SMALL_LIVE':
        deployment_confidence_label = 'MEDIUM'
    elif go_live_status == 'SHADOW_LIVE':
        deployment_confidence_label = 'CAUTIOUS'
    elif go_live_status == 'PAPER_ONLY':
        deployment_confidence_label = 'LOW'
    else:
        deployment_confidence_label = 'REJECT'

    return {
        'go_live_status': go_live_status,
        'go_live_reasons': reasons,
        'go_live_warnings': warnings,
        'deployment_confidence_label': deployment_confidence_label,
    }

def build_trade_evaluation_summary(
    rows: list[dict[str, Any]],
    *,
    strategy_name: str = 'TRADE_SYSTEM',
    readiness_config: PaperReadinessConfig | None = None,
) -> dict[str, Any]:
    cfg = readiness_config or PaperReadinessConfig()
    summary = calculate_trade_metrics(rows, strategy_name=strategy_name)
    summary.update(_advanced_validation_metrics(rows, summary, cfg))
    readiness = evaluate_paper_readiness(summary, cfg)
    summary.update(readiness)
    summary.update(evaluate_go_live_readiness(summary, cfg))
    summary['terminal_metrics_lines'] = terminal_lines(summary)
    summary['validation_summary_lines'] = list(summary['terminal_metrics_lines'])
    summary['dashboard_metrics_rows'] = metrics_frame(summary).to_dict(orient='records')
    summary['trade_evaluation_records'] = [asdict(record) for record in standardize_trade_records(rows)]
    return summary


__all__ = [
    'MIN_TRADES',
    'MIN_EXPECTANCY',
    'MIN_PROFIT_FACTOR',
    'MAX_DRAWDOWN_PCT',
    'MIN_RECOVERY_FACTOR',
    'MIN_TRADES_FOR_ADVANCED_VALIDATION',
    'MIN_SHARPE',
    'MIN_SORTINO',
    'MAX_CONSECUTIVE_LOSSES',
    'MIN_STREAK_QUALITY_SCORE',
    'MIN_REGIME_CONSISTENCY_SCORE',
    'MAX_ONE_TRADE_PROFIT_CONCENTRATION',
    'MAX_TOP3_PROFIT_CONCENTRATION',
    'TradeEvaluationRecord',
    'PaperReadinessConfig',
    'safe_float',
    'safe_int',
    'parse_trade_timestamp',
    'standardize_trade_record',
    'standardize_trade_records',
    'calculate_trade_metrics',
    'metrics_frame',
    'terminal_lines',
    'evaluate_paper_readiness',
    'evaluate_go_live_readiness',
    'build_trade_evaluation_summary',
]
