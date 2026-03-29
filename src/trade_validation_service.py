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

WF_TRAIN_WINDOW_TRADES = 40
WF_TEST_WINDOW_TRADES = 20
WF_STEP_SIZE_TRADES = 20
MIN_WALKFORWARD_WINDOWS = 4
MIN_OOS_TRADES = 30
MIN_OOS_PASS_RATE = 0.60
MAX_EXPECTANCY_DEGRADATION_PCT = 0.40
MAX_PROFIT_FACTOR_DEGRADATION_PCT = 0.30
MAX_DRAWDOWN_EXPANSION_PCT = 0.50
MAX_WINDOW_PROFIT_CONCENTRATION = 0.50
MIN_MONITORING_READINESS_SCORE = 6.0


def _window_metrics_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = calculate_trade_metrics(rows, strategy_name='WALKFORWARD_WINDOW')
    trades = safe_int(summary.get('closed_trades', summary.get('total_trades')))
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    pf_raw = summary.get('profit_factor', 0.0)
    pf = float('inf') if str(pf_raw).strip().lower() == 'inf' else safe_float(pf_raw)
    dd = safe_float(summary.get('max_drawdown_pct'))
    return {
        'trades': trades,
        'net_profit': round(safe_float(summary.get('net_profit', summary.get('total_pnl'))), 2),
        'expectancy': expectancy,
        'profit_factor': summary.get('profit_factor', 0.0),
        'max_drawdown_pct': dd,
        'pass_fail_status': 'PASS' if trades > 0 and expectancy > 0 and (pf == float('inf') or pf >= MIN_PROFIT_FACTOR) and dd <= MAX_DRAWDOWN_PCT else 'FAIL',
        'warnings': [],
    }


def _degradation_pct(is_value: float, oos_value: float) -> float:
    if abs(is_value) <= SMALL_VALUE:
        return 0.0 if abs(oos_value) <= SMALL_VALUE else 1.0
    return round(max((is_value - oos_value) / max(abs(is_value), SMALL_VALUE), 0.0), 4)


def _expansion_pct(is_value: float, oos_value: float) -> float:
    base = max(abs(is_value), 1.0)
    return round(max((oos_value - is_value) / base, 0.0), 4)


def _oos_label(expectancy_deg: float, pf_deg: float, dd_expand: float) -> str:
    if expectancy_deg <= 0.20 and pf_deg <= 0.15 and dd_expand <= 0.25:
        return 'STABLE_OOS'
    if expectancy_deg <= MAX_EXPECTANCY_DEGRADATION_PCT and pf_deg <= MAX_PROFIT_FACTOR_DEGRADATION_PCT and dd_expand <= MAX_DRAWDOWN_EXPANSION_PCT:
        return 'ACCEPTABLE_DEGRADATION'
    if expectancy_deg <= 0.70 and pf_deg <= 0.55 and dd_expand <= 0.80:
        return 'WEAK_OOS'
    return 'OVERFIT_RISK'


def _walkforward_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pairs = _closed_trade_pairs(rows)
    if len(pairs) < (WF_TRAIN_WINDOW_TRADES + WF_TEST_WINDOW_TRADES):
        return {
            'walkforward_windows': 0,
            'walkforward_results': [],
            'walkforward_pass_count': 0,
            'walkforward_fail_count': 0,
            'walkforward_consistency_score': 0.0,
            'oos_total_trades': 0,
            'oos_pass_count': 0,
            'oos_fail_count': 0,
            'oos_pass_rate': 0.0,
            'oos_status': 'OOS_NEED_MORE_DATA',
            'oos_reasons': ['OOS_NEED_MORE_DATA: not enough closed trades for walk-forward validation'],
            'oos_warnings': ['Too few OOS trades for strong live confidence.'],
            'train_test_instability_score': 10.0,
            'edge_decay_score': 10.0,
            'window_profit_concentration': 0.0,
            'parameter_fragility_warning': '',
            'overfit_risk_score': 10.0,
            'overfit_risk_label': 'HIGH',
        }

    results: list[dict[str, Any]] = []
    start = 0
    while start + WF_TRAIN_WINDOW_TRADES + WF_TEST_WINDOW_TRADES <= len(pairs):
        train_pairs = pairs[start:start + WF_TRAIN_WINDOW_TRADES]
        test_pairs = pairs[start + WF_TRAIN_WINDOW_TRADES:start + WF_TRAIN_WINDOW_TRADES + WF_TEST_WINDOW_TRADES]
        train_rows = [row for _, row in train_pairs]
        test_rows = [row for _, row in test_pairs]
        is_metrics = _window_metrics_from_rows(train_rows)
        oos_metrics = _window_metrics_from_rows(test_rows)
        expectancy_deg = _degradation_pct(safe_float(is_metrics.get('expectancy')), safe_float(oos_metrics.get('expectancy')))
        is_pf_raw = is_metrics.get('profit_factor', 0.0)
        oos_pf_raw = oos_metrics.get('profit_factor', 0.0)
        is_pf = 99.0 if str(is_pf_raw).strip().lower() == 'inf' else safe_float(is_pf_raw)
        oos_pf = 99.0 if str(oos_pf_raw).strip().lower() == 'inf' else safe_float(oos_pf_raw)
        pf_deg = _degradation_pct(is_pf, oos_pf)
        dd_expand = _expansion_pct(safe_float(is_metrics.get('max_drawdown_pct')), safe_float(oos_metrics.get('max_drawdown_pct')))
        trade_drop = _degradation_pct(float(safe_int(is_metrics.get('trades'))), float(safe_int(oos_metrics.get('trades'))))
        label = _oos_label(expectancy_deg, pf_deg, dd_expand)
        warnings: list[str] = []
        if label == 'OVERFIT_RISK':
            warnings.append('Repeated IS/OOS degradation suggests overfit risk.')
        if expectancy_deg > MAX_EXPECTANCY_DEGRADATION_PCT:
            warnings.append(f'Expectancy degraded {expectancy_deg * 100:.0f}% out of sample.')
        results.append({
            'train_start': str(train_pairs[0][0].entry_time or train_pairs[0][0].signal_time or ''),
            'train_end': str(train_pairs[-1][0].exit_time or train_pairs[-1][0].entry_time or ''),
            'test_start': str(test_pairs[0][0].entry_time or test_pairs[0][0].signal_time or ''),
            'test_end': str(test_pairs[-1][0].exit_time or test_pairs[-1][0].entry_time or ''),
            'trades': oos_metrics['trades'],
            'net_profit': oos_metrics['net_profit'],
            'expectancy': oos_metrics['expectancy'],
            'profit_factor': oos_metrics['profit_factor'],
            'max_drawdown_pct': oos_metrics['max_drawdown_pct'],
            'is_net_profit': is_metrics['net_profit'],
            'oos_net_profit': oos_metrics['net_profit'],
            'is_expectancy': is_metrics['expectancy'],
            'oos_expectancy': oos_metrics['expectancy'],
            'is_profit_factor': is_metrics['profit_factor'],
            'oos_profit_factor': oos_metrics['profit_factor'],
            'is_drawdown_pct': is_metrics['max_drawdown_pct'],
            'oos_drawdown_pct': oos_metrics['max_drawdown_pct'],
            'is_trade_count': is_metrics['trades'],
            'oos_trade_count': oos_metrics['trades'],
            'expectancy_degradation_pct': expectancy_deg,
            'profit_factor_degradation_pct': pf_deg,
            'drawdown_expansion_pct': dd_expand,
            'trade_count_drop_pct': trade_drop,
            'oos_comparison_label': label,
            'pass_fail_status': 'PASS' if oos_metrics['pass_fail_status'] == 'PASS' and label in {'STABLE_OOS', 'ACCEPTABLE_DEGRADATION'} else 'FAIL',
            'warnings': warnings,
        })
        start += WF_STEP_SIZE_TRADES

    pass_count = sum(1 for item in results if item['pass_fail_status'] == 'PASS')
    fail_count = len(results) - pass_count
    oos_total_trades = sum(safe_int(item.get('oos_trade_count')) for item in results)
    oos_pass_rate_ratio = round(pass_count / len(results), 4) if results else 0.0
    oos_reasons: list[str] = []
    oos_warnings: list[str] = []
    if len(results) < MIN_WALKFORWARD_WINDOWS or oos_total_trades < MIN_OOS_TRADES:
        oos_status = 'OOS_NEED_MORE_DATA'
        oos_reasons.append('OOS_NEED_MORE_DATA: too few walk-forward windows or OOS trades')
    elif oos_pass_rate_ratio < 0.40 or fail_count > pass_count:
        oos_status = 'OOS_FAIL'
        oos_reasons.append('OOS_FAIL: majority of out-of-sample windows fail')
    elif oos_pass_rate_ratio < MIN_OOS_PASS_RATE:
        oos_status = 'OOS_BORDERLINE'
        oos_reasons.append('OOS_BORDERLINE: out-of-sample edge is mixed')
    else:
        oos_status = 'OOS_PASS'
        oos_reasons.append('OOS_PASS: majority of out-of-sample windows remain acceptable')

    expectancy_degs = [safe_float(item.get('expectancy_degradation_pct')) for item in results]
    later_half = results[len(results) // 2:] if results else []
    early_half = results[:max(len(results) // 2, 1)] if results else []
    early_expectancy = sum(safe_float(item.get('oos_expectancy')) for item in early_half) / max(len(early_half), 1)
    later_expectancy = sum(safe_float(item.get('oos_expectancy')) for item in later_half) / max(len(later_half), 1)
    edge_decay_score = round(min(max(_degradation_pct(early_expectancy, later_expectancy) * 10.0, 0.0), 10.0), 2) if results else 10.0
    instability_events = sum(1 for item in results if safe_float(item.get('is_expectancy')) > 0 and safe_float(item.get('oos_expectancy')) <= 0)
    train_test_instability_score = round(min((instability_events / max(len(results), 1)) * 10.0, 10.0), 2)
    oos_net_profits = [safe_float(item.get('oos_net_profit')) for item in results]
    positive_oos_profit = sum(value for value in oos_net_profits if value > 0)
    window_profit_concentration = round(max(oos_net_profits) / max(positive_oos_profit, SMALL_VALUE), 4) if positive_oos_profit > 0 and oos_net_profits else 0.0
    if window_profit_concentration > MAX_WINDOW_PROFIT_CONCENTRATION:
        oos_warnings.append('OOS profitability is concentrated in one window.')
    if edge_decay_score >= 5.0:
        oos_warnings.append('Edge weakens in later windows.')
    if any(value > MAX_EXPECTANCY_DEGRADATION_PCT for value in expectancy_degs):
        oos_warnings.append('Repeated IS/OOS degradation suggests overfit risk.')

    overfit_risk_score = round(min((train_test_instability_score * 0.35) + (edge_decay_score * 0.30) + (window_profit_concentration * 10.0 * 0.35), 10.0), 2)
    if overfit_risk_score >= 7.5:
        overfit_label = 'HIGH'
    elif overfit_risk_score >= 5.0:
        overfit_label = 'MODERATE'
    else:
        overfit_label = 'LOW'

    walkforward_consistency_score = round(max(min((oos_pass_rate_ratio * 10.0) - (overfit_risk_score * 0.35), 10.0), 0.0), 2)
    return {
        'walkforward_windows': len(results),
        'walkforward_results': results,
        'walkforward_pass_count': pass_count,
        'walkforward_fail_count': fail_count,
        'walkforward_consistency_score': walkforward_consistency_score,
        'oos_total_trades': oos_total_trades,
        'oos_pass_count': pass_count,
        'oos_fail_count': fail_count,
        'oos_pass_rate': round(oos_pass_rate_ratio * 100.0, 2),
        'oos_status': oos_status,
        'oos_reasons': oos_reasons,
        'oos_warnings': oos_warnings,
        'train_test_instability_score': train_test_instability_score,
        'edge_decay_score': edge_decay_score,
        'window_profit_concentration': window_profit_concentration,
        'parameter_fragility_warning': '',
        'overfit_risk_score': overfit_risk_score,
        'overfit_risk_label': overfit_label,
    }


def _monitoring_readiness() -> dict[str, Any]:
    from pathlib import Path

    base = Path(__file__).resolve().parent
    checks = {
        'signal logging': (base / 'trading_core.py').exists(),
        'trade execution logging': (base / 'execution_engine.py').exists(),
        'order status tracking': (base / 'execution_engine.py').exists(),
        'error logging': (base / 'execution_engine.py').exists() and (base / 'operational_daemon.py').exists(),
        'rejection reason logging': (base / 'execution_engine.py').exists(),
        'PnL tracking': (base / 'backtest_engine.py').exists(),
        'drawdown tracking': (base / 'trade_validation_service.py').exists(),
        'daily summary generation': (base / 'runtime_reporting_service.py').exists() or (base / 'reporting_service.py').exists(),
        'alert hooks': (base / 'telegram_notifier.py').exists(),
        'kill-switch support': (base / 'trade_safety.py').exists() or (base / 'deployment_guard.py').exists(),
    }
    available = [name for name, present in checks.items() if present]
    missing = [name for name, present in checks.items() if not present]
    score = round(len(available), 2)
    if score >= 9:
        label = 'STRONG'
    elif score >= 7:
        label = 'GOOD'
    elif score >= 4:
        label = 'BASIC'
    else:
        label = 'NOT_READY'
    return {
        'monitoring_readiness_score': score,
        'monitoring_readiness_label': label,
        'monitoring_available_items': available,
        'monitoring_missing_items': missing,
    }


def _promotion_decision(summary: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    warnings: list[str] = []
    next_actions: list[str] = []
    pass_fail = str(summary.get('pass_fail_status', 'FAIL'))
    oos_status = str(summary.get('oos_status', 'OOS_NEED_MORE_DATA'))
    overfit = safe_float(summary.get('overfit_risk_score'))
    monitoring_score = safe_float(summary.get('monitoring_readiness_score'))
    go_live_status = str(summary.get('go_live_status', 'PAPER_ONLY'))

    if pass_fail == 'FAIL' or oos_status == 'OOS_FAIL' or overfit >= 7.5:
        status = 'REJECT'
        reasons.append('Validation, OOS stability, or overfit risk is unacceptable.')
        next_actions.append('Rework the strategy and rerun walk-forward validation.')
    elif pass_fail == 'NEED_MORE_DATA' or oos_status == 'OOS_NEED_MORE_DATA':
        status = 'RESEARCH_ONLY'
        reasons.append('There is not enough stable out-of-sample evidence yet.')
        next_actions.append('Collect more trades and more walk-forward windows.')
    elif oos_status == 'OOS_BORDERLINE' or monitoring_score < MIN_MONITORING_READINESS_SCORE:
        status = 'BACKTEST_OK'
        reasons.append('Backtest is usable, but OOS or monitoring is not strong enough yet.')
        next_actions.append('Improve monitoring coverage and verify more OOS windows.')
    elif go_live_status == 'PAPER_ONLY':
        status = 'PAPER_READY'
        reasons.append('Validation passes well enough for paper deployment.')
        next_actions.append('Run controlled paper trading with daily review.')
    elif go_live_status == 'SHADOW_LIVE':
        status = 'LIVE_SHADOW_READY'
        reasons.append('Validation is decent enough for shadow-live observation.')
        next_actions.append('Run live-shadow monitoring with no or minimal execution risk.')
    elif go_live_status in {'SMALL_LIVE', 'LIVE_READY'} and monitoring_score >= 8.0 and oos_status == 'OOS_PASS':
        status = 'SMALL_LIVE_CANDIDATE'
        reasons.append('Validation, OOS stability, and monitoring are aligned for a very small deployment candidate.')
        next_actions.append('Use strict caps and keep active monitoring on every session.')
    else:
        status = 'PAPER_CONTINUE'
        reasons.append('Paper deployment should continue before any promotion.')
        next_actions.append('Keep paper trading and monitor OOS stability.')

    if safe_float(summary.get('window_profit_concentration')) > MAX_WINDOW_PROFIT_CONCENTRATION:
        warnings.append('OOS profitability concentrated in one window.')
    if summary.get('monitoring_missing_items'):
        warnings.append('Monitoring is missing some operational controls.')
    return {
        'promotion_status': status,
        'promotion_reasons': reasons,
        'promotion_warnings': warnings,
        'next_required_actions': next_actions,
    }


def terminal_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        'Validation Summary',
        f"Total trades: {safe_int(summary.get('total_trades'))}",
        f"- Trades: {safe_int(summary.get('closed_trades', summary.get('total_trades')))}",
        f"- Expectancy: {safe_float(summary.get('expectancy_per_trade')):.2f}",
        f"- Profit Factor: {summary.get('profit_factor', 0.0)}",
        f"- Max Drawdown: {safe_float(summary.get('max_drawdown_pct')):.2f}%",
        f"- Recovery Factor: {summary.get('recovery_factor', 0.0)}",
        f"- Decision: {summary.get('pass_fail_status', 'NA')} ({summary.get('confidence_label', 'NA')})",
        'Advanced Validation Summary',
        f"- Sharpe: {summary.get('sharpe_ratio', 0.0)}",
        f"- Sortino: {summary.get('sortino_ratio', 0.0)}",
        f"- Streak Quality: {safe_float(summary.get('streak_quality_score')):.2f}",
        f"- Regime Consistency: {safe_float(summary.get('regime_consistency_score')):.2f}",
        f"- Dominant Regime: {summary.get('dominant_regime', 'none')}",
        f"- Weakest Regime: {summary.get('weakest_regime', 'none')}",
        f"- Go Live Status: {summary.get('go_live_status', 'NA')}",
        'Walk-Forward Validation Summary',
        f"- Windows: {safe_int(summary.get('walkforward_windows'))}",
        f"- OOS Pass Rate: {safe_float(summary.get('oos_pass_rate')):.0f}%",
        f"- OOS Status: {summary.get('oos_status', 'NA')}",
        f"- Overfit Risk: {summary.get('overfit_risk_label', 'NA')}",
        f"- Monitoring Readiness: {summary.get('monitoring_readiness_label', 'NA')}",
        f"- Promotion Status: {summary.get('promotion_status', 'NA')}",
    ]
    for collection in ('pass_fail_reasons', 'go_live_reasons', 'oos_reasons'):
        for reason in summary.get(collection, []) or []:
            lines.append(f'- {reason}')
    for collection in ('warnings', 'go_live_warnings', 'oos_warnings', 'promotion_warnings'):
        for warning in summary.get(collection, []) or []:
            if f'- WARNING: {warning}' not in lines:
                lines.append(f'- WARNING: {warning}')
    return lines


def build_trade_evaluation_summary(
    rows: list[dict[str, Any]],
    *,
    strategy_name: str = 'TRADE_SYSTEM',
    readiness_config: PaperReadinessConfig | None = None,
) -> dict[str, Any]:
    cfg = readiness_config or PaperReadinessConfig()
    summary = calculate_trade_metrics(rows, strategy_name=strategy_name)
    summary.update(_advanced_validation_metrics(rows, summary, cfg))
    summary.update(_walkforward_metrics(rows))
    summary.update(_monitoring_readiness())
    readiness = evaluate_paper_readiness(summary, cfg)
    summary.update(readiness)
    summary.update(evaluate_go_live_readiness(summary, cfg))
    summary.update(_promotion_decision(summary))
    summary['terminal_metrics_lines'] = terminal_lines(summary)
    summary['validation_summary_lines'] = list(summary['terminal_metrics_lines'])
    summary['dashboard_metrics_rows'] = metrics_frame(summary).to_dict(orient='records')
    summary['trade_evaluation_records'] = [asdict(record) for record in standardize_trade_records(rows)]
    return summary


__all__ = [
    'MIN_TRADES', 'MIN_EXPECTANCY', 'MIN_PROFIT_FACTOR', 'MAX_DRAWDOWN_PCT', 'MIN_RECOVERY_FACTOR',
    'MIN_TRADES_FOR_ADVANCED_VALIDATION', 'MIN_SHARPE', 'MIN_SORTINO', 'MAX_CONSECUTIVE_LOSSES',
    'MIN_STREAK_QUALITY_SCORE', 'MIN_REGIME_CONSISTENCY_SCORE', 'MAX_ONE_TRADE_PROFIT_CONCENTRATION',
    'MAX_TOP3_PROFIT_CONCENTRATION', 'WF_TRAIN_WINDOW_TRADES', 'WF_TEST_WINDOW_TRADES', 'WF_STEP_SIZE_TRADES',
    'MIN_WALKFORWARD_WINDOWS', 'MIN_OOS_TRADES', 'MIN_OOS_PASS_RATE', 'TradeEvaluationRecord', 'PaperReadinessConfig',
    'safe_float', 'safe_int', 'parse_trade_timestamp', 'standardize_trade_record', 'standardize_trade_records',
    'calculate_trade_metrics', 'metrics_frame', 'terminal_lines', 'evaluate_paper_readiness',
    'evaluate_go_live_readiness', 'build_trade_evaluation_summary'
]

MIN_CLEAN_TRADES_WARN = 100
MIN_CLEAN_TRADES_PASS = 200
MIN_EXPECTANCY_STABILITY_SCORE = 6.0
MIN_PF_STABILITY_SCORE = 6.0
STRICT_MAX_DRAWDOWN_PCT = 15.0


def _is_clean_trade_pair(record: TradeEvaluationRecord, raw: dict[str, Any]) -> bool:
    if not _record_counts_as_trade(record):
        return False
    if not _trade_is_closed(record):
        return False
    if record.entry_time is None or record.exit_time is None:
        return False
    if record.side not in {'BUY', 'SELL'}:
        return False
    if abs(record.entry_price) <= 0 or (record.exit_price == 0 and abs(record.pnl) == 0):
        return False
    if record.duplicate_reason:
        return False
    if record.validation_error:
        return False
    quantity = raw.get('quantity', 1)
    if str(quantity).strip() != '':
        try:
            if int(float(quantity)) <= 0:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _clean_trade_pairs(rows: list[dict[str, Any]]) -> tuple[list[tuple[TradeEvaluationRecord, dict[str, Any]]], int, int]:
    pairs = _closed_trade_pairs(rows)
    clean_pairs = [pair for pair in pairs if _is_clean_trade_pair(pair[0], pair[1])]
    valid_trades = len(pairs)
    rejected_trades = max(len(rows) - len(clean_pairs), 0)
    return clean_pairs, valid_trades, rejected_trades


def _segment_count(trade_count: int) -> int:
    if trade_count < 40:
        return 0
    if trade_count >= 240:
        return 6
    if trade_count >= 160:
        return 5
    return 4


def _segment_slices(items: list[Any], segment_count: int) -> list[list[Any]]:
    if not items or segment_count <= 0:
        return []
    size = max(len(items) // segment_count, 1)
    slices: list[list[Any]] = []
    start = 0
    for segment_idx in range(segment_count):
        end = len(items) if segment_idx == segment_count - 1 else min(len(items), start + size)
        chunk = items[start:end]
        if chunk:
            slices.append(chunk)
        start = end
    return slices


def _segment_expectancy(pnl_values: list[float]) -> float:
    if not pnl_values:
        return 0.0
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    count = len(pnl_values)
    win_rate = len(wins) / count if count else 0.0
    loss_rate = len(losses) / count if count else 0.0
    average_win = sum(wins) / len(wins) if wins else 0.0
    average_loss = abs(sum(losses)) / len(losses) if losses else 0.0
    return round((win_rate * average_win) - (loss_rate * average_loss), 2)


def _segment_profit_factor(pnl_values: list[float]) -> float | str:
    profits = sum(value for value in pnl_values if value > 0)
    losses = abs(sum(value for value in pnl_values if value < 0))
    if profits > 0 and losses <= SMALL_VALUE:
        return 'inf'
    if losses <= SMALL_VALUE:
        return 0.0
    return round(profits / losses, 2)


def _rolling_expectancy(pnl_values: list[float]) -> list[float]:
    if not pnl_values:
        return []
    window = max(min(len(pnl_values) // 4, 25), 10)
    if len(pnl_values) < window:
        return [round(_segment_expectancy(pnl_values), 2)]
    values: list[float] = []
    for start in range(0, len(pnl_values) - window + 1):
        values.append(round(_segment_expectancy(pnl_values[start:start + window]), 2))
    return values


def _rolling_profit_factor(pnl_values: list[float]) -> list[float | str]:
    if not pnl_values:
        return []
    window = max(min(len(pnl_values) // 4, 25), 10)
    if len(pnl_values) < window:
        return [_segment_profit_factor(pnl_values)]
    values: list[float | str] = []
    for start in range(0, len(pnl_values) - window + 1):
        values.append(_segment_profit_factor(pnl_values[start:start + window]))
    return values


def _trade_data_quality(clean_trades: int, valid_trades: int, total_trades: int) -> tuple[float, str]:
    if total_trades <= 0:
        return 0.0, 'POOR'
    clean_ratio = clean_trades / max(total_trades, 1)
    valid_ratio = valid_trades / max(total_trades, 1)
    sample_bonus = 1.0 if clean_trades >= MIN_CLEAN_TRADES_PASS else 0.6 if clean_trades >= MIN_CLEAN_TRADES_WARN else 0.25
    score = round(min(((clean_ratio * 6.0) + (valid_ratio * 3.0) + sample_bonus), 10.0), 2)
    if score >= 8.5:
        label = 'STRONG'
    elif score >= 6.5:
        label = 'GOOD'
    elif score >= 4.5:
        label = 'LIMITED'
    else:
        label = 'POOR'
    return score, label


def _stability_score(values: list[float], positive_ratio: float, threshold: float) -> float:
    if not values:
        return 0.0
    dispersion = pstdev(values) if len(values) > 1 else 0.0
    avg = abs(sum(values) / len(values)) if values else 0.0
    stability = max(0.0, 10.0 - min((dispersion / max(avg, 1.0)) * 4.0, 6.0))
    ratio_boost = min(max((positive_ratio - threshold) * 10.0, 0.0), 4.0)
    return round(min(stability + ratio_boost, 10.0), 2)


def calculate_trade_metrics(rows: list[dict[str, Any]], *, strategy_name: str = 'TRADE_SYSTEM') -> dict[str, Any]:
    records = standardize_trade_records(rows)
    trade_records = [record for record in records if _record_counts_as_trade(record)]
    clean_pairs, valid_trades, rejected_trades = _clean_trade_pairs(rows)
    ordered_clean = sorted(clean_pairs, key=lambda item: item[0].exit_time or item[0].entry_time or item[0].signal_time or datetime.min)
    clean_records = [pair[0] for pair in ordered_clean]
    pnl_values = [float(record.pnl) for record in clean_records]
    winning_pnl = [value for value in pnl_values if value > 0]
    losing_pnl = [value for value in pnl_values if value < 0]
    clean_trade_count = len(clean_records)
    wins = len(winning_pnl)
    losses = len(losing_pnl)
    win_rate_ratio = wins / clean_trade_count if clean_trade_count else 0.0
    loss_rate = losses / clean_trade_count if clean_trade_count else 0.0
    average_win = round(sum(winning_pnl) / wins, 2) if wins else 0.0
    average_loss_magnitude = round(abs(sum(losing_pnl)) / losses, 2) if losses else 0.0
    average_loss = round(sum(losing_pnl) / losses, 2) if losses else 0.0
    payoff_ratio = round(_safe_ratio(average_win, average_loss_magnitude), 2) if average_loss_magnitude > 0 else (float('inf') if average_win > 0 else 0.0)
    expectancy_per_trade = round((win_rate_ratio * average_win) - (loss_rate * average_loss_magnitude), 2) if clean_trade_count else 0.0
    expectancy_pct = round(_safe_ratio(expectancy_per_trade, average_loss_magnitude) * 100.0, 2) if average_loss_magnitude > 0 else (0.0 if expectancy_per_trade <= 0 else 100.0)
    gross_profit = round(sum(winning_pnl), 2)
    gross_loss = round(sum(losing_pnl), 2)
    gross_loss_abs = abs(gross_loss)
    profit_factor, profit_factor_note = _profit_factor_value(gross_profit, gross_loss_abs)
    net_profit = round(sum(pnl_values), 2)
    equity = _equity_metrics(pnl_values)
    average_drawdown_pct = round(sum(abs(value) for value in equity['drawdown_pct_series']) / max(len(equity['drawdown_pct_series']), 1), 2) if equity['drawdown_pct_series'] else 0.0
    recovery_factor = round(_safe_ratio(net_profit, abs(equity['max_drawdown_amount'])), 2) if abs(equity['max_drawdown_amount']) > 0 else (0.0 if net_profit <= 0 else float('inf'))
    drawdown_stability_score = round(max(0.0, 10.0 - min(safe_float(equity['max_drawdown_pct']) / max(STRICT_MAX_DRAWDOWN_PCT, 1.0) * 5.0, 7.0) - min(equity['longest_drawdown_streak'] / 3.0, 3.0)), 2)
    consecutive_wins_max, consecutive_losses_max, current_win_streak, current_loss_streak, avg_win_streak, avg_loss_streak = _max_consecutive_counts(pnl_values)
    largest_win = round(max(winning_pnl), 2) if winning_pnl else 0.0
    largest_loss = round(min(losing_pnl), 2) if losing_pnl else 0.0
    largest_win_pct_of_total_profit = round(_safe_ratio(largest_win, gross_profit) * 100.0, 2) if gross_profit > 0 else 0.0
    top_3_wins_pct = round(_safe_ratio(sum(sorted(winning_pnl, reverse=True)[:3]), gross_profit) * 100.0, 2) if gross_profit > 0 and winning_pnl else 0.0
    median_trade_pnl = round(float(median(pnl_values)), 2) if pnl_values else 0.0
    pnl_std_dev = round(float(pstdev(pnl_values)), 2) if len(pnl_values) > 1 else 0.0
    duplicate_trade_count = sum(1 for record in records if record.duplicate_reason.startswith('DUPLICATE_'))
    execution_error_count = sum(1 for record in records if record.execution_status == 'ERROR' or record.validation_error == 'BROKER_ERROR')
    invalid_trade_count = sum(1 for record in records if bool(record.validation_error and record.validation_error != 'BROKER_ERROR'))
    recent_closed_pnl = pnl_values[-RECENT_TRADE_WINDOW:]

    segment_count = _segment_count(clean_trade_count)
    pnl_segments = _segment_slices(pnl_values, segment_count)
    segment_expectancy_list = [round(_segment_expectancy(segment), 2) for segment in pnl_segments]
    segment_profit_factor_list = [_segment_profit_factor(segment) for segment in pnl_segments]
    segment_net_profit_list = [round(sum(segment), 2) for segment in pnl_segments]
    positive_expectancy_segments = sum(1 for value in segment_expectancy_list if value > 0)
    negative_expectancy_segments = sum(1 for value in segment_expectancy_list if value <= 0)
    positive_expectancy_segment_ratio = round(positive_expectancy_segments / max(len(segment_expectancy_list), 1), 4) if segment_expectancy_list else 0.0
    pf_positive_segments = sum(1 for value in segment_profit_factor_list if (str(value).strip().lower() == 'inf') or safe_float(value) > 1.0)
    positive_pf_segment_ratio = round(pf_positive_segments / max(len(segment_profit_factor_list), 1), 4) if segment_profit_factor_list else 0.0
    rolling_expectancy = _rolling_expectancy(pnl_values)
    rolling_profit_factor = _rolling_profit_factor(pnl_values)
    expectancy_stability_score = _stability_score(segment_expectancy_list, positive_expectancy_segment_ratio, 0.60)
    pf_numeric_segments = [99.0 if str(value).strip().lower() == 'inf' else safe_float(value) for value in segment_profit_factor_list]
    profit_factor_stability_score = _stability_score(pf_numeric_segments, positive_pf_segment_ratio, 0.60)
    expectancy_label = 'NEGATIVE' if expectancy_per_trade <= 0 else 'STRONG' if expectancy_stability_score >= 8.0 and positive_expectancy_segment_ratio >= 0.75 else 'STABLE' if expectancy_stability_score >= MIN_EXPECTANCY_STABILITY_SCORE else 'UNSTABLE'
    profit_factor_value = float('inf') if str(profit_factor).strip().lower() == 'inf' else safe_float(profit_factor)
    profit_factor_label = 'WEAK' if profit_factor_value < 1.0 else 'BORDERLINE' if profit_factor_value < 1.3 else 'STRONG' if profit_factor_value > 1.6 and profit_factor_stability_score >= 7.0 else 'ACCEPTABLE'
    drawdown_label = 'EXCESSIVE' if safe_float(equity['max_drawdown_pct']) > 20.0 else 'HIGH' if safe_float(equity['max_drawdown_pct']) > 15.0 else 'BORDERLINE' if safe_float(equity['max_drawdown_pct']) > 10.0 else 'CONTROLLED'
    clean_trade_ratio = round(clean_trade_count / max(len(rows), 1), 4) if rows else 0.0
    trade_data_quality_score, trade_data_quality_label = _trade_data_quality(clean_trade_count, valid_trades, len(rows))

    return {
        'strategy': strategy_name,
        'total_trades': len(trade_records),
        'valid_trades': valid_trades,
        'clean_trades': clean_trade_count,
        'clean_trade_ratio': clean_trade_ratio,
        'rejected_trades': rejected_trades,
        'trade_data_quality_score': trade_data_quality_score,
        'trade_data_quality_label': trade_data_quality_label,
        'closed_trades': clean_trade_count,
        'open_trades': max(len(trade_records) - clean_trade_count, 0),
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
        'rolling_expectancy': rolling_expectancy,
        'segment_expectancy_list': segment_expectancy_list,
        'positive_expectancy_segment_ratio': positive_expectancy_segment_ratio,
        'expectancy_stability_score': expectancy_stability_score,
        'expectancy_label': expectancy_label,
        'profit_factor': profit_factor,
        'profit_factor_note': profit_factor_note,
        'rolling_profit_factor': rolling_profit_factor,
        'segment_profit_factor_list': segment_profit_factor_list,
        'positive_pf_segment_ratio': positive_pf_segment_ratio,
        'profit_factor_stability_score': profit_factor_stability_score,
        'profit_factor_label': profit_factor_label,
        'largest_win': largest_win,
        'largest_loss': largest_loss,
        'largest_win_pct_of_total_profit': largest_win_pct_of_total_profit,
        'top_3_wins_pct_of_total_profit': top_3_wins_pct,
        'one_trade_profit_concentration': round(largest_win_pct_of_total_profit / 100.0, 4),
        'top_3_trade_profit_concentration': round(top_3_wins_pct / 100.0, 4),
        'consecutive_wins_max': consecutive_wins_max,
        'consecutive_losses_max': consecutive_losses_max,
        'current_win_streak': current_win_streak,
        'current_loss_streak': current_loss_streak,
        'average_win_streak': avg_win_streak,
        'average_loss_streak': avg_loss_streak,
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
        'segment_count': segment_count,
        'segment_net_profit_list': segment_net_profit_list,
        'average_drawdown_pct': average_drawdown_pct,
        'recovery_factor': recovery_factor,
        'drawdown_stability_score': drawdown_stability_score,
        'drawdown_label': drawdown_label,
        'drawdown_proven': 'YES' if abs(equity['max_drawdown_amount']) > 0 and losses > 0 else 'NO',
        **equity,
        'max_drawdown': abs(equity['max_drawdown_amount']),
    }


def evaluate_paper_readiness(summary: dict[str, Any], config: PaperReadinessConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperReadinessConfig()
    clean_trades = safe_int(summary.get('clean_trades', summary.get('closed_trades', summary.get('total_trades'))))
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    recovery_factor_raw = summary.get('recovery_factor', 0.0)
    recovery_factor = float('inf') if str(recovery_factor_raw).strip().lower() == 'inf' else safe_float(recovery_factor_raw)
    expectancy_stability_score = safe_float(summary.get('expectancy_stability_score'))
    pf_stability_score = safe_float(summary.get('profit_factor_stability_score'))
    segment_count = safe_int(summary.get('segment_count'))
    malformed_ratio = 1.0 - safe_float(summary.get('clean_trade_ratio'))

    reasons: list[str] = []
    blocker_messages: list[str] = []
    status = 'PASS'
    if clean_trades < MIN_CLEAN_TRADES_WARN or segment_count < 4:
        status = 'NEED_MORE_DATA'
        reasons.append(f'NEED_MORE_DATA: only {clean_trades} clean trades')
        blocker_messages.append(f'only {clean_trades} clean trades')
    if malformed_ratio > 0.25:
        status = 'NEED_MORE_DATA'
        reasons.append('NEED_MORE_DATA: too many malformed trades removed')
        blocker_messages.append('too many malformed trades removed')
    if expectancy <= max(cfg.min_expectancy_per_trade, MIN_EXPECTANCY):
        reasons.append('FAIL: expectancy is not positive')
        blocker_messages.append('expectancy is not positive yet')
    if expectancy_stability_score < MIN_EXPECTANCY_STABILITY_SCORE or safe_float(summary.get('positive_expectancy_segment_ratio')) < 0.60:
        reasons.append('FAIL: expectancy is positive globally but unstable across segments')
        blocker_messages.append('expectancy stability is too weak')
    if profit_factor != float('inf') and profit_factor < max(cfg.min_profit_factor, MIN_PROFIT_FACTOR):
        reasons.append('FAIL: profit factor below 1.3')
        blocker_messages.append('profit factor is below 1.3')
    if pf_stability_score < MIN_PF_STABILITY_SCORE or safe_float(summary.get('positive_pf_segment_ratio')) < 0.60:
        reasons.append('FAIL: profit factor is unstable across segments')
        blocker_messages.append('profit factor stability is too weak')
    if max_drawdown_pct > min(cfg.max_drawdown_pct, STRICT_MAX_DRAWDOWN_PCT):
        reasons.append('FAIL: drawdown too high for current edge')
        blocker_messages.append('drawdown is too high')
    if recovery_factor != float('inf') and recovery_factor < cfg.min_recovery_factor:
        reasons.append('FAIL: recovery factor too weak')
        blocker_messages.append('recovery factor is too weak')
    if safe_float(summary.get('one_trade_profit_concentration')) > cfg.max_one_trade_profit_concentration:
        reasons.append('FAIL: results depend too much on one oversized winner')
        blocker_messages.append('results are too concentrated in one trade')
    if safe_float(summary.get('top_3_trade_profit_concentration')) > cfg.max_top3_profit_concentration:
        reasons.append('FAIL: top 3 trades contribute too much profit')
        blocker_messages.append('results are too concentrated in top winners')

    if status != 'NEED_MORE_DATA' and reasons:
        status = 'FAIL'

    warnings: list[str] = []
    if clean_trades < MIN_CLEAN_TRADES_PASS:
        warnings.append(f'Sample is above {MIN_CLEAN_TRADES_WARN} but still below {MIN_CLEAN_TRADES_PASS} clean trades.')
    if malformed_ratio > 0.10:
        warnings.append('Too many malformed trades removed.')
    if safe_float(summary.get('one_trade_profit_concentration')) > 0.30:
        warnings.append(f"Positive expectancy is too dependent on a small subset of trades.")
    if safe_float(summary.get('top_3_trade_profit_concentration')) > 0.60:
        warnings.append(f"Top 3 trades contributed {safe_float(summary.get('top_3_wins_pct_of_total_profit')):.0f}% of total profits.")
    if expectancy_stability_score < 7.0:
        warnings.append('Profitability is unstable across chronological segments.')
    if safe_float(summary.get('drawdown_stability_score')) < 6.0:
        warnings.append('Drawdown control is too weak for live confidence.')
    confidence_label = 'NEED_MORE_DATA' if status == 'NEED_MORE_DATA' else 'FAIL' if status == 'FAIL' else 'STRONG_PASS' if clean_trades >= MIN_CLEAN_TRADES_PASS and expectancy_stability_score >= 7.0 and pf_stability_score >= 7.0 and max_drawdown_pct <= 12.0 else 'PASS' if clean_trades >= MIN_CLEAN_TRADES_WARN else 'BORDERLINE'

    if status == 'PASS':
        blocker_text = ''
        readiness_summary = 'Validation passed with enough clean trades, positive stable expectancy, profit factor above target, and controlled drawdown.'
        next_step = 'Continue paper deployment and keep monitoring before any live decision.'
    elif status == 'NEED_MORE_DATA':
        blocker_text = '; '.join(blocker_messages or [f'only {clean_trades} clean trades'])
        readiness_summary = f'Validation needs more data because {blocker_text.lower()}.'
        next_step = 'Stay in paper trading and collect more clean closed trades before making any pass/fail decision.'
    else:
        blocker_text = '; '.join(blocker_messages or reasons)
        readiness_summary = f'Validation failed because {blocker_text.lower()}.'
        next_step = 'Stay in paper trading. Improve stability, profit factor, or drawdown before any promotion.'

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


def _time_anchor(record: TradeEvaluationRecord) -> datetime:
    return record.exit_time or record.entry_time or record.signal_time or datetime.min


STRICT_FAIL_MIN_TRADES = 100
STRICT_LIVE_READY_TRADES = 150
STRICT_EXPECTANCY_PASS_R = 0.0
STRICT_EXPECTANCY_LIVE_READY_R = 0.30
STRICT_MIN_PROFIT_FACTOR = 1.30
STRICT_STRONG_PROFIT_FACTOR = 1.50
STRICT_MAX_DRAWDOWN_PASS_PCT = 20.0
STRICT_MAX_DRAWDOWN_FAIL_PCT = 25.0
STRICT_LIVE_READY_MAX_DRAWDOWN_PCT = 15.0
STRICT_MIN_WIN_RATE_PCT = 30.0
STRICT_ACCEPTABLE_WIN_RATE_LOW_PCT = 35.0
STRICT_ACCEPTABLE_WIN_RATE_HIGH_PCT = 65.0
STRICT_MIN_RR = 1.50
STRICT_IDEAL_RR = 2.00
STRICT_MAX_LOSS_STREAK = 7
STRICT_STRONG_MAX_LOSS_STREAK = 5
STRICT_MIN_RECOVERY_FACTOR = 1.50


def _timeframe_minutes(value: Any) -> int:
    raw = str(value or '').strip().lower()
    if not raw:
        return 0
    if raw.endswith('min'):
        raw = raw[:-3]
    if raw.endswith('m'):
        return safe_int(raw[:-1], 0)
    if raw.endswith('h'):
        return safe_int(raw[:-1], 0) * 60
    return safe_int(raw, 0)


def _cooldown_window_minutes(raw: dict[str, Any]) -> int:
    explicit_minutes = safe_int(raw.get('duplicate_signal_cooldown_minutes'), 0)
    if explicit_minutes > 0:
        return explicit_minutes
    bars = safe_int(raw.get('duplicate_signal_cooldown_bars'), 0)
    timeframe_minutes = _timeframe_minutes(raw.get('timeframe', raw.get('interval', '')))
    if bars > 0 and timeframe_minutes > 0:
        return bars * timeframe_minutes
    return 0


def _strict_execution_discipline_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    records = standardize_trade_records(rows)
    pairs = [(standardize_trade_record(dict(row)), dict(row)) for row in rows]
    ordered_pairs = sorted(pairs, key=lambda item: _time_anchor(item[0]))

    duplicate_trade_count = sum(1 for record in records if str(record.duplicate_reason).startswith('DUPLICATE_'))
    cooldown_violation_count = sum(
        1
        for record, raw in ordered_pairs
        if 'DUPLICATE_SIGNAL_COOLDOWN' in {
            str(record.duplicate_reason or '').strip().upper(),
            str(record.validation_error or '').strip().upper(),
            str(raw.get('blocked_reason', '') or '').strip().upper(),
            str(raw.get('rejection_reason', '') or '').strip().upper(),
        }
    )
    max_trades_day_violation_count = sum(
        1
        for _, raw in ordered_pairs
        if 'MAX_TRADES_PER_DAY' in {
            str(raw.get('blocked_reason', '') or '').strip().upper(),
            str(raw.get('risk_limit_reason', '') or '').strip().upper(),
            str(raw.get('rejection_reason', '') or '').strip().upper(),
        }
    )

    revenge_entry_count = 0
    previous_closed_loss: tuple[TradeEvaluationRecord, dict[str, Any]] | None = None
    for record, raw in ordered_pairs:
        if not (_record_counts_as_trade(record) and _trade_is_closed(record)):
            continue
        if previous_closed_loss is not None:
            prev_record, prev_raw = previous_closed_loss
            same_setup = (
                str(prev_record.strategy).upper() == str(record.strategy).upper()
                and str(prev_record.symbol).upper() == str(record.symbol).upper()
            )
            entry_time = record.entry_time or record.signal_time
            prev_exit = prev_record.exit_time or prev_record.entry_time or prev_record.signal_time
            cooldown_minutes = _cooldown_window_minutes(raw) or _cooldown_window_minutes(prev_raw) or 30
            if same_setup and entry_time is not None and prev_exit is not None:
                elapsed_minutes = (entry_time - prev_exit).total_seconds() / 60.0
                if elapsed_minutes >= 0 and elapsed_minutes < cooldown_minutes:
                    revenge_entry_count += 1
        previous_closed_loss = (record, raw) if record.pnl < 0 else None

    violation_count = duplicate_trade_count + cooldown_violation_count + max_trades_day_violation_count + revenge_entry_count
    return {
        'duplicate_trade_count': duplicate_trade_count,
        'cooldown_violation_count': cooldown_violation_count,
        'max_trades_day_violation_count': max_trades_day_violation_count,
        'revenge_entry_count': revenge_entry_count,
        'execution_violation_count': violation_count,
        'execution_discipline_ok': violation_count == 0,
        'execution_discipline_status': 'PASS' if violation_count == 0 else 'FAIL',
    }


def _strict_equity_curve_stability(summary: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    expectancy_stability_score = safe_float(summary.get('expectancy_stability_score'))
    pf_stability_score = safe_float(summary.get('profit_factor_stability_score'))
    positive_expectancy_segment_ratio = safe_float(summary.get('positive_expectancy_segment_ratio'))
    one_trade_concentration = safe_float(summary.get('one_trade_profit_concentration'))
    top3_concentration = safe_float(summary.get('top_3_trade_profit_concentration'))
    pnl_std_dev = safe_float(summary.get('pnl_std_dev'))
    expectancy = abs(safe_float(summary.get('expectancy_per_trade')))

    if expectancy_stability_score < 6.0:
        reasons.append('expectancy unstable across segments')
    if pf_stability_score < 6.0:
        reasons.append('profit factor unstable across segments')
    if positive_expectancy_segment_ratio < 0.60:
        reasons.append('too many negative expectancy segments')
    if one_trade_concentration > 0.30:
        reasons.append('single trade dominates profit curve')
    if top3_concentration > 0.60:
        reasons.append('top 3 trades dominate profit curve')
    if pnl_std_dev > max(expectancy * 4.0, 1.0):
        reasons.append('equity volatility too high for the observed expectancy')
    return len(reasons) == 0, reasons


def _strict_metric_rows(summary: dict[str, Any]) -> list[dict[str, str]]:
    expectancy_r = safe_float(summary.get('expectancy_r'))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    win_rate = safe_float(summary.get('win_rate'))
    rr_ratio = safe_float(summary.get('reward_risk_ratio', summary.get('payoff_ratio')))
    trade_count = safe_int(summary.get('clean_trades', summary.get('closed_trades', summary.get('total_trades'))))
    loss_streak = safe_int(summary.get('consecutive_losses_max', summary.get('longest_loss_streak')))
    recovery_factor_raw = summary.get('recovery_factor', 0.0)
    recovery_factor = float('inf') if str(recovery_factor_raw).strip().lower() == 'inf' else safe_float(recovery_factor_raw)
    equity_stable = bool(summary.get('equity_curve_stable', False))
    execution_discipline_ok = bool(summary.get('execution_discipline_ok', False))

    def status_label(passed: bool) -> str:
        return '✅ PASS' if passed else '❌ FAIL'

    rows = [
        {
            'Metric': 'Expectancy',
            'Value': f'{expectancy_r:.2f}R',
            'Status': status_label(expectancy_r > STRICT_EXPECTANCY_PASS_R),
            'Comment': 'Strong edge' if expectancy_r > STRICT_EXPECTANCY_LIVE_READY_R else 'Positive but below live-ready target' if expectancy_r > 0 else 'No positive edge',
        },
        {
            'Metric': 'Profit Factor',
            'Value': 'inf' if profit_factor == float('inf') else f'{profit_factor:.2f}',
            'Status': status_label(profit_factor == float('inf') or profit_factor >= STRICT_MIN_PROFIT_FACTOR),
            'Comment': 'Good efficiency' if profit_factor == float('inf') or profit_factor > STRICT_STRONG_PROFIT_FACTOR else 'Below strict threshold' if profit_factor < STRICT_MIN_PROFIT_FACTOR else 'Tradable but not strong',
        },
        {
            'Metric': 'Max Drawdown',
            'Value': f'{max_drawdown_pct:.2f}%',
            'Status': status_label(max_drawdown_pct <= STRICT_MAX_DRAWDOWN_PASS_PCT),
            'Comment': 'Controlled' if max_drawdown_pct < STRICT_LIVE_READY_MAX_DRAWDOWN_PCT else 'Acceptable but not live-ready' if max_drawdown_pct <= STRICT_MAX_DRAWDOWN_PASS_PCT else 'Risk too high',
        },
        {
            'Metric': 'Win Rate',
            'Value': f'{win_rate:.2f}%',
            'Status': status_label(win_rate >= STRICT_MIN_WIN_RATE_PCT),
            'Comment': 'Within preferred band' if STRICT_ACCEPTABLE_WIN_RATE_LOW_PCT <= win_rate <= STRICT_ACCEPTABLE_WIN_RATE_HIGH_PCT else 'Too low' if win_rate < STRICT_MIN_WIN_RATE_PCT else 'Usable but secondary metric',
        },
        {
            'Metric': 'Risk-Reward Ratio',
            'Value': 'inf' if rr_ratio == float('inf') else f'{rr_ratio:.2f}',
            'Status': status_label(rr_ratio == float('inf') or rr_ratio >= STRICT_MIN_RR),
            'Comment': 'Ideal' if rr_ratio == float('inf') or rr_ratio >= STRICT_IDEAL_RR else 'Meets minimum' if rr_ratio >= STRICT_MIN_RR else 'Reward too small for risk',
        },
        {
            'Metric': 'Trade Count',
            'Value': str(trade_count),
            'Status': status_label(trade_count >= STRICT_FAIL_MIN_TRADES),
            'Comment': 'Sample strong enough for live review' if trade_count >= STRICT_LIVE_READY_TRADES else 'Minimum sample reached' if trade_count >= STRICT_FAIL_MIN_TRADES else 'Insufficient sample size',
        },
        {
            'Metric': 'Equity Curve Stability',
            'Value': 'Stable' if equity_stable else 'Unstable',
            'Status': status_label(equity_stable),
            'Comment': 'Smooth upward profile' if equity_stable else '; '.join(summary.get('equity_curve_instability_reasons', []) or ['Spikes detected']),
        },
        {
            'Metric': 'Loss Streak Control',
            'Value': str(loss_streak),
            'Status': status_label(loss_streak <= STRICT_MAX_LOSS_STREAK),
            'Comment': 'Controlled' if loss_streak <= STRICT_STRONG_MAX_LOSS_STREAK else 'Monitor closely' if loss_streak <= STRICT_MAX_LOSS_STREAK else 'Strategy unstable under loss clustering',
        },
        {
            'Metric': 'Recovery Factor',
            'Value': 'inf' if recovery_factor == float('inf') else f'{recovery_factor:.2f}',
            'Status': status_label(recovery_factor == float('inf') or recovery_factor > STRICT_MIN_RECOVERY_FACTOR),
            'Comment': 'Capital recovers efficiently' if recovery_factor == float('inf') or recovery_factor > STRICT_MIN_RECOVERY_FACTOR else 'Recovery too weak',
        },
        {
            'Metric': 'Execution Discipline',
            'Value': 'Clean' if execution_discipline_ok else 'Violations',
            'Status': status_label(execution_discipline_ok),
            'Comment': 'No duplicate/cooldown/overtrade/revenge violations' if execution_discipline_ok else '; '.join(summary.get('execution_discipline_comments', []) or ['Execution violations detected']),
        },
    ]
    return rows


def _strict_validation_markdown(rows: list[dict[str, str]]) -> str:
    lines = [
        '| Metric | Value | Status | Comment |',
        '|--------|------|--------|---------|',
    ]
    for row in rows:
        lines.append(f"| {row['Metric']} | {row['Value']} | {row['Status']} | {row['Comment']} |")
    return '\n'.join(lines)


def evaluate_paper_readiness(summary: dict[str, Any], config: PaperReadinessConfig | None = None) -> dict[str, Any]:
    _ = config or PaperReadinessConfig()
    trade_count = safe_int(summary.get('clean_trades', summary.get('closed_trades', summary.get('total_trades'))))
    expectancy_r = safe_float(summary.get('expectancy_r'))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    recovery_factor_raw = summary.get('recovery_factor', 0.0)
    recovery_factor = float('inf') if str(recovery_factor_raw).strip().lower() == 'inf' else safe_float(recovery_factor_raw)
    win_rate = safe_float(summary.get('win_rate'))
    rr_ratio = safe_float(summary.get('reward_risk_ratio', summary.get('payoff_ratio')))
    loss_streak = safe_int(summary.get('consecutive_losses_max', summary.get('longest_loss_streak')))
    execution_discipline_ok = bool(summary.get('execution_discipline_ok', False))
    equity_stable = bool(summary.get('equity_curve_stable', False))
    duplicate_trade_count = safe_int(summary.get('duplicate_trade_count'))

    blockers: list[str] = []
    if trade_count < STRICT_FAIL_MIN_TRADES:
        blockers.append('not enough paper trades yet')
    if expectancy_r <= STRICT_EXPECTANCY_PASS_R:
        blockers.append('expectancy is not positive yet')
    if profit_factor != float('inf') and profit_factor < STRICT_MIN_PROFIT_FACTOR:
        blockers.append('profit factor is below 1.3')
    if max_drawdown_pct > STRICT_MAX_DRAWDOWN_FAIL_PCT:
        blockers.append('max drawdown is above 25%')
    if duplicate_trade_count > 0:
        blockers.append('duplicate trades were detected')
    if not equity_stable:
        blockers.append('equity curve is unstable')
    if rr_ratio != float('inf') and rr_ratio < STRICT_MIN_RR:
        blockers.append('risk-reward ratio is below 1.5')
    if win_rate < STRICT_MIN_WIN_RATE_PCT:
        blockers.append('win rate is below 30%')
    if loss_streak > STRICT_MAX_LOSS_STREAK:
        blockers.append('loss streak is too long')
    if recovery_factor != float('inf') and recovery_factor <= STRICT_MIN_RECOVERY_FACTOR:
        blockers.append('recovery factor is too weak')
    if not execution_discipline_ok:
        blockers.append('execution discipline violations are present')

    hard_fail = len(blockers) > 0
    live_ready = (
        expectancy_r > STRICT_EXPECTANCY_LIVE_READY_R
        and (profit_factor == float('inf') or profit_factor > STRICT_STRONG_PROFIT_FACTOR)
        and max_drawdown_pct < STRICT_LIVE_READY_MAX_DRAWDOWN_PCT
        and trade_count >= STRICT_LIVE_READY_TRADES
        and equity_stable
        and execution_discipline_ok
    )

    if live_ready:
        status = 'PASS'
        blocker_text = ''
        readiness_summary = 'Strict validation passed. Statistical edge, drawdown control, and execution discipline are live-ready.'
        next_step = 'System is eligible for controlled go-live with the current hard risk limits unchanged.'
        confidence_label = 'HIGH'
    elif hard_fail:
        status = 'FAIL'
        blocker_text = '; '.join(blockers)
        readiness_summary = f'Validation failed because {blocker_text}.'
        next_step = 'Stay in paper trading. Clear every blocker before any live deployment.'
        confidence_label = 'REJECT'
    else:
        status = 'NEED_MORE_DATA'
        blocker_text = 'strict hard-fail rules passed, but live-ready thresholds are not all satisfied yet'
        readiness_summary = 'Validation is improving, but the system still needs more confirmed edge or more sample depth before go-live.'
        next_step = 'Continue paper trading until expectancy exceeds 0.30R, profit factor exceeds 1.5, drawdown stays below 15%, and the sample reaches 150 trades.'
        confidence_label = 'MEDIUM'

    return {
        'paper_ready': 'YES' if live_ready else 'NO',
        'paper_readiness_status': 'READY_FOR_LIVE' if live_ready else 'FAIL_STRICT_VALIDATION' if hard_fail else 'CONTINUE_PAPER_VALIDATION',
        'paper_readiness_blockers': blocker_text,
        'paper_readiness_summary': readiness_summary,
        'paper_readiness_next_step': next_step,
        'pass_fail_status': status,
        'pass_fail_reasons': blockers,
        'warnings': [] if live_ready else ['Win rate remains secondary. Expectancy, PF, drawdown, and discipline drive the decision.'],
        'confidence_label': confidence_label,
        'go_live_status': 'LIVE_READY' if live_ready else 'REJECT' if hard_fail else 'PAPER_ONLY',
        'deployment_confidence_label': confidence_label,
    }


def metrics_frame(summary: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            'metric': row['Metric'],
            'value': row['Value'],
            'status': row['Status'],
            'comment': row['Comment'],
        }
        for row in summary.get('strict_validation_rows', [])
    ]
    return pd.DataFrame(rows)


def terminal_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        'Validation Summary',
        f"Total trades: {safe_int(summary.get('total_trades'))}",
        f"- Clean Trades: {safe_int(summary.get('clean_trades', summary.get('closed_trades')))}",
        f"- Pre-Execution Validation: {summary.get('pre_execution_validation_status', 'NA')}",
        f"- Post-Execution Validation: {summary.get('post_execution_validation_status', 'NA')}",
        f"- Live Status: {summary.get('go_live_status', 'NA')}",
        'Strict Validation Table',
        *summary.get('strict_validation_markdown', '').splitlines(),
    ]
    blockers = summary.get('pass_fail_reasons', []) or []
    for blocker in blockers:
        lines.append(f'- {blocker}')
    return lines


def build_trade_evaluation_summary(
    rows: list[dict[str, Any]],
    *,
    strategy_name: str = 'TRADE_SYSTEM',
    readiness_config: PaperReadinessConfig | None = None,
) -> dict[str, Any]:
    cfg = readiness_config or PaperReadinessConfig()
    summary = calculate_trade_metrics(rows, strategy_name=strategy_name)

    average_loss = safe_float(summary.get('average_loss'))
    expectancy_per_trade = safe_float(summary.get('expectancy_per_trade'))
    expectancy_r = round(_safe_ratio(expectancy_per_trade, average_loss), 2) if average_loss > 0 else (0.0 if expectancy_per_trade <= 0 else float('inf'))
    summary['expectancy_r'] = expectancy_r

    discipline = _strict_execution_discipline_metrics(rows)
    summary.update(discipline)
    summary['pre_execution_validation_status'] = 'PASS' if (
        safe_int(summary.get('invalid_trade_count')) == 0 and discipline['execution_discipline_ok']
    ) else 'FAIL'

    equity_stable, instability_reasons = _strict_equity_curve_stability(summary)
    summary['equity_curve_stable'] = equity_stable
    summary['equity_curve_instability_reasons'] = instability_reasons

    discipline_comments: list[str] = []
    if discipline['duplicate_trade_count'] > 0:
        discipline_comments.append('duplicate trades detected')
    if discipline['cooldown_violation_count'] > 0:
        discipline_comments.append('cooldown violations detected')
    if discipline['max_trades_day_violation_count'] > 0:
        discipline_comments.append('max trades/day exceeded')
    if discipline['revenge_entry_count'] > 0:
        discipline_comments.append('revenge entries detected')
    summary['execution_discipline_comments'] = discipline_comments

    hard_fail_reasons: list[str] = []
    trade_count = safe_int(summary.get('clean_trades', summary.get('closed_trades', summary.get('total_trades'))))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    if safe_float(summary.get('expectancy_r')) <= STRICT_EXPECTANCY_PASS_R:
        hard_fail_reasons.append('EXPECTANCY<=0R')
    if profit_factor != float('inf') and profit_factor < STRICT_MIN_PROFIT_FACTOR:
        hard_fail_reasons.append('PROFIT_FACTOR<1.3')
    if max_drawdown_pct > STRICT_MAX_DRAWDOWN_FAIL_PCT:
        hard_fail_reasons.append('MAX_DRAWDOWN>25%')
    if trade_count < STRICT_FAIL_MIN_TRADES:
        hard_fail_reasons.append('TRADES<100')
    if safe_int(summary.get('duplicate_trade_count')) > 0:
        hard_fail_reasons.append('DUPLICATE_TRADES_DETECTED')
    if not equity_stable:
        hard_fail_reasons.append('EQUITY_CURVE_UNSTABLE')
    summary['strict_fail_reasons'] = hard_fail_reasons

    strict_rows = _strict_metric_rows(summary)
    summary['strict_validation_rows'] = strict_rows
    summary['strict_validation_markdown'] = _strict_validation_markdown(strict_rows)
    summary['post_execution_validation_status'] = 'FAIL' if hard_fail_reasons else 'PASS'

    summary.update(evaluate_paper_readiness(summary, cfg))
    summary['validation_summary_lines'] = terminal_lines(summary)
    summary['terminal_metrics_lines'] = list(summary['validation_summary_lines'])
    summary['dashboard_metrics_rows'] = metrics_frame(summary).to_dict(orient='records')
    summary['trade_evaluation_records'] = [asdict(record) for record in standardize_trade_records(rows)]
    return summary
