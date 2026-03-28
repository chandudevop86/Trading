from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from statistics import median, pstdev
from typing import Any

import pandas as pd


SMALL_VALUE = 1e-9
MIN_TRADES = 30
MIN_EXPECTANCY = 0.0
MIN_PROFIT_FACTOR = 1.2
MAX_DRAWDOWN_PCT = 12.0
MIN_RECOVERY_FACTOR = 1.5
MAX_LONGEST_DRAWDOWN_STREAK_WARNING = 10
OUTSIZED_WINNER_WARNING_PCT = 0.40
RECENT_TRADE_WINDOW = 5


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


def _max_consecutive_counts(pnl_values: list[float]) -> tuple[int, int]:
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
    return consecutive_wins_max, consecutive_losses_max


def _validation_warnings(summary: dict[str, Any], cfg: PaperReadinessConfig) -> list[str]:
    warnings: list[str] = []
    if safe_int(summary.get('closed_trades')) < max(int(cfg.min_trades), 1) * 2:
        warnings.append('Sample size is still small for strong confidence.')
    if safe_float(summary.get('largest_win_pct_of_total_profit')) > OUTSIZED_WINNER_WARNING_PCT * 100.0:
        warnings.append('Results concentrated in one outsized winner.')
    recent_pnl = summary.get('recent_closed_pnl', []) or []
    if recent_pnl and sum(float(value) for value in recent_pnl) <= 0:
        warnings.append('Recent closed trades are not profitable.')
    if safe_int(summary.get('longest_drawdown_streak')) > MAX_LONGEST_DRAWDOWN_STREAK_WARNING:
        warnings.append('Equity curve shows a long drawdown streak.')
    if safe_float(summary.get('pnl_std_dev')) > max(abs(safe_float(summary.get('expectancy_per_trade'))) * 4.0, 1.0):
        warnings.append('Equity curve looks unstable relative to expectancy.')
    if str(summary.get('profit_factor_note', '')).strip():
        warnings.append(str(summary['profit_factor_note']).strip())
    return warnings


def _confidence_label(summary: dict[str, Any], status: str, cfg: PaperReadinessConfig) -> str:
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    expectancy_pct = safe_float(summary.get('expectancy_pct'))
    profit_factor = summary.get('profit_factor', 0.0)
    profit_factor_value = float('inf') if str(profit_factor).strip().lower() == 'inf' else safe_float(profit_factor)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    recovery_factor = safe_float(summary.get('recovery_factor'))
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
    )
    if total_trades >= cfg.min_trades * 2 and expectancy_pct >= 0.5 and profit_factor_value >= 2.0 and max_drawdown_pct <= cfg.max_drawdown_pct * 0.7 and recovery_factor >= 2.0:
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
    consecutive_wins_max, consecutive_losses_max = _max_consecutive_counts(pnl_values)
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
        {'metric': 'Wins', 'value': safe_int(summary.get('wins'))},
        {'metric': 'Losses', 'value': safe_int(summary.get('losses'))},
        {'metric': 'Win rate %', 'value': round(safe_float(summary.get('win_rate')), 2)},
        {'metric': 'Average win', 'value': round(safe_float(summary.get('average_win', summary.get('avg_win'))), 2)},
        {'metric': 'Average loss', 'value': round(safe_float(summary.get('average_loss', summary.get('avg_loss'))), 2)},
        {'metric': 'Expectancy / trade', 'value': round(safe_float(summary.get('expectancy_per_trade')), 2)},
        {'metric': 'Expectancy %', 'value': round(safe_float(summary.get('expectancy_pct')), 2)},
        {'metric': 'Profit factor', 'value': summary.get('profit_factor', 0.0)},
        {'metric': 'Max drawdown %', 'value': round(safe_float(summary.get('max_drawdown_pct')), 2)},
        {'metric': 'Recovery factor', 'value': summary.get('recovery_factor', 0.0)},
        {'metric': 'Decision', 'value': str(summary.get('pass_fail_status', 'NA'))},
        {'metric': 'Confidence', 'value': str(summary.get('confidence_label', 'NA'))},
    ]
    return pd.DataFrame(rows)


def terminal_lines(summary: dict[str, Any]) -> list[str]:
    decision = str(summary.get('pass_fail_status', 'NA'))
    confidence = str(summary.get('confidence_label', 'NA'))
    lines = [
        'Validation Summary',
        f"Total trades: {safe_int(summary.get('total_trades'))}",
        f"- Trades: {safe_int(summary.get('closed_trades', summary.get('total_trades')))}",
        f"- Expectancy: {safe_float(summary.get('expectancy_per_trade')):.2f}",
        f"- Profit Factor: {summary.get('profit_factor', 0.0)}",
        f"- Max Drawdown: {safe_float(summary.get('max_drawdown_pct')):.2f}%",
        f"- Recovery Factor: {summary.get('recovery_factor', 0.0)}",
        f"- Decision: {decision} ({confidence})",
    ]
    for reason in summary.get('pass_fail_reasons', []) or []:
        lines.append(f"- {reason}")
    for warning in summary.get('warnings', []) or []:
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


def build_trade_evaluation_summary(
    rows: list[dict[str, Any]],
    *,
    strategy_name: str = 'TRADE_SYSTEM',
    readiness_config: PaperReadinessConfig | None = None,
) -> dict[str, Any]:
    """Return one shared metrics/readiness payload for paper, execution, and backtest rows."""
    summary = calculate_trade_metrics(rows, strategy_name=strategy_name)
    summary.update(evaluate_paper_readiness(summary, readiness_config))
    summary['validation_summary_lines'] = terminal_lines(summary)
    summary['terminal_metrics_lines'] = summary['validation_summary_lines']
    summary['dashboard_metrics_rows'] = metrics_frame(summary).to_dict(orient='records')
    summary['trade_evaluation_records'] = [asdict(record) for record in standardize_trade_records(rows)]
    return summary


__all__ = [
    'MIN_EXPECTANCY',
    'MIN_PROFIT_FACTOR',
    'MIN_RECOVERY_FACTOR',
    'MIN_TRADES',
    'MAX_DRAWDOWN_PCT',
    'PaperReadinessConfig',
    'TradeEvaluationRecord',
    'build_trade_evaluation_summary',
    'calculate_trade_metrics',
    'evaluate_paper_readiness',
    'metrics_frame',
    'parse_trade_timestamp',
    'safe_float',
    'safe_int',
    'standardize_trade_record',
    'standardize_trade_records',
    'terminal_lines',
]
