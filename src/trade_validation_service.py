from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd


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
    min_trades: int = 30
    min_expectancy_per_trade: float = 0.0
    min_profit_factor: float = 1.2
    max_drawdown_pct: float = 12.0
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
    gross_profit = sum(winning_pnl)
    gross_loss_abs = abs(sum(losing_pnl))
    profit_factor: float | str = gross_profit / gross_loss_abs if gross_loss_abs > 0 else 'inf' if gross_profit > 0 else 0.0
    total_pnl = round(sum(pnl_values), 2)
    closed_trade_count = len(ordered_closed)
    expectancy = round(total_pnl / closed_trade_count, 2) if closed_trade_count else 0.0

    running_equity = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    for pnl in pnl_values:
        running_equity += pnl
        peak_equity = max(peak_equity, running_equity)
        drawdown = peak_equity - running_equity
        max_drawdown = max(max_drawdown, drawdown)
        if peak_equity > 0:
            max_drawdown_pct = max(max_drawdown_pct, (drawdown / peak_equity) * 100.0)

    longest_win_streak = 0
    longest_loss_streak = 0
    current_win_streak = 0
    current_loss_streak = 0
    for pnl in pnl_values:
        if pnl > 0:
            current_win_streak += 1
            current_loss_streak = 0
        elif pnl < 0:
            current_loss_streak += 1
            current_win_streak = 0
        else:
            current_win_streak = 0
            current_loss_streak = 0
        longest_win_streak = max(longest_win_streak, current_win_streak)
        longest_loss_streak = max(longest_loss_streak, current_loss_streak)

    duplicate_trade_count = sum(1 for record in records if record.duplicate_reason.startswith('DUPLICATE_'))
    execution_error_count = sum(1 for record in records if record.execution_status == 'ERROR' or record.validation_error == 'BROKER_ERROR')
    invalid_trade_count = sum(1 for record in records if bool(record.validation_error and record.validation_error != 'BROKER_ERROR'))

    return {
        'strategy': strategy_name,
        'total_trades': len(trade_records),
        'closed_trades': closed_trade_count,
        'open_trades': max(len(trade_records) - closed_trade_count, 0),
        'wins': wins,
        'losses': losses,
        'win_rate': round((wins / closed_trade_count) * 100.0, 2) if closed_trade_count else 0.0,
        'avg_win': round(sum(winning_pnl) / len(winning_pnl), 2) if winning_pnl else 0.0,
        'avg_loss': round(sum(losing_pnl) / len(losing_pnl), 2) if losing_pnl else 0.0,
        'total_pnl': total_pnl,
        'expectancy_per_trade': expectancy,
        'profit_factor': round(profit_factor, 2) if isinstance(profit_factor, float) else profit_factor,
        'max_drawdown': round(max_drawdown, 2),
        'max_drawdown_pct': round(max_drawdown_pct, 2),
        'longest_win_streak': longest_win_streak,
        'longest_loss_streak': longest_loss_streak,
        'duplicate_trade_count': duplicate_trade_count,
        'execution_error_count': execution_error_count,
        'invalid_trade_count': invalid_trade_count,
        'drawdown_proven': 'YES' if max_drawdown > 0 and losses > 0 else 'NO',
    }


def metrics_frame(summary: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {'metric': 'Total trades', 'value': safe_int(summary.get('total_trades'))},
        {'metric': 'Wins', 'value': safe_int(summary.get('wins'))},
        {'metric': 'Losses', 'value': safe_int(summary.get('losses'))},
        {'metric': 'Win rate', 'value': round(safe_float(summary.get('win_rate')), 2)},
        {'metric': 'Average win', 'value': round(safe_float(summary.get('avg_win')), 2)},
        {'metric': 'Average loss', 'value': round(safe_float(summary.get('avg_loss')), 2)},
        {'metric': 'Expectancy', 'value': round(safe_float(summary.get('expectancy_per_trade')), 2)},
        {'metric': 'Profit factor', 'value': summary.get('profit_factor', 0.0)},
        {'metric': 'Max drawdown', 'value': round(safe_float(summary.get('max_drawdown')), 2)},
        {'metric': 'Max drawdown %', 'value': round(safe_float(summary.get('max_drawdown_pct')), 2)},
        {'metric': 'Longest win streak', 'value': safe_int(summary.get('longest_win_streak'))},
        {'metric': 'Longest loss streak', 'value': safe_int(summary.get('longest_loss_streak'))},
    ]
    return pd.DataFrame(rows)


def terminal_lines(summary: dict[str, Any]) -> list[str]:
    return [
        f"Total trades: {safe_int(summary.get('total_trades'))}",
        f"Wins/Losses: {safe_int(summary.get('wins'))}/{safe_int(summary.get('losses'))}",
        f"Win rate: {safe_float(summary.get('win_rate')):.2f}%",
        f"Average win: {safe_float(summary.get('avg_win')):.2f}",
        f"Average loss: {safe_float(summary.get('avg_loss')):.2f}",
        f"Expectancy: {safe_float(summary.get('expectancy_per_trade')):.2f}",
        f"Profit factor: {summary.get('profit_factor', 0.0)}",
        f"Max drawdown: {safe_float(summary.get('max_drawdown')):.2f} ({safe_float(summary.get('max_drawdown_pct')):.2f}%)",
        f"Execution errors: {safe_int(summary.get('execution_error_count'))}",
        f"Duplicate trades: {safe_int(summary.get('duplicate_trade_count'))}",
    ]


def evaluate_paper_readiness(summary: dict[str, Any], config: PaperReadinessConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperReadinessConfig()
    total_trades = safe_int(summary.get('total_trades'))
    expectancy = safe_float(summary.get('expectancy_per_trade'))
    profit_factor_raw = summary.get('profit_factor', 0.0)
    profit_factor = float('inf') if str(profit_factor_raw).strip().lower() == 'inf' else safe_float(profit_factor_raw)
    max_drawdown_pct = safe_float(summary.get('max_drawdown_pct'))
    execution_error_count = safe_int(summary.get('execution_error_count'))
    duplicate_trade_count = safe_int(summary.get('duplicate_trade_count'))
    invalid_trade_count = safe_int(summary.get('invalid_trade_count'))

    blockers: list[str] = []
    if total_trades < cfg.min_trades:
        blockers.append('not enough paper trades yet')
    if cfg.require_positive_expectancy and expectancy <= cfg.min_expectancy_per_trade:
        blockers.append('expectancy is not positive yet')
    if profit_factor != float('inf') and profit_factor < cfg.min_profit_factor:
        blockers.append('profit factor is below target')
    if max_drawdown_pct > cfg.max_drawdown_pct:
        blockers.append('drawdown is above the current paper limit')
    if execution_error_count > cfg.max_execution_errors:
        blockers.append('execution errors are still present')
    if duplicate_trade_count > cfg.max_duplicate_trades:
        blockers.append('duplicate trades were not fully blocked')
    if invalid_trade_count > cfg.max_invalid_trades:
        blockers.append('invalid trade rows are still present')

    ready = not blockers
    next_step = (
        'Paper trading has passed the current readiness gate. Keep monitoring before any live promotion.'
        if ready
        else 'Stay in paper trading. Clear the listed blockers before any live decision.'
    )
    return {
        'paper_ready': 'YES' if ready else 'NO',
        'paper_readiness_status': 'READY_FOR_PAPER_PHASE' if ready else 'NOT_READY_FOR_PAPER_PHASE',
        'paper_readiness_blockers': '; '.join(blockers),
        'paper_readiness_summary': (
            'Paper trading is ready for the current phase.'
            if ready
            else f"Paper trading is not ready yet because {', '.join(blockers)}."
        ),
        'paper_readiness_next_step': next_step,
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
    summary['terminal_metrics_lines'] = terminal_lines(summary)
    summary['dashboard_metrics_rows'] = metrics_frame(summary).to_dict(orient='records')
    summary['trade_evaluation_records'] = [asdict(record) for record in standardize_trade_records(rows)]
    return summary


__all__ = [
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
