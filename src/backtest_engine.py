from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.trading_core import append_log, prepare_trading_data, write_rows

BACKTEST_TRADES_OUTPUT = Path('data/backtest_trades.csv')
BACKTEST_SUMMARY_OUTPUT = Path('data/backtest_summary.csv')


@dataclass(slots=True)
class BacktestConfig:
    capital: float = 100000.0
    risk_pct: float = 0.01
    rr_ratio: float = 2.0
    trades_output: Path = BACKTEST_TRADES_OUTPUT
    summary_output: Path = BACKTEST_SUMMARY_OUTPUT
    strategy_name: str = 'STRATEGY'
    strategy_config: Any = None
    allow_overlap: bool = False
    close_open_positions_at_end: bool = True


@dataclass(slots=True)
class BacktestResult:
    trades: list[dict[str, object]]
    summary: dict[str, object]


@dataclass(slots=True)
class _TradeLifecycle:
    index: int
    strategy: str
    side: str
    entry_time: pd.Timestamp
    entry_price: float
    stop_loss: float
    target_price: float
    quantity: int
    score: float
    reason: str
    status: str = 'candidate'
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str = ''
    pnl: float = 0.0
    rr_achieved: float = 0.0
    setup_type: str = ''
    rejection_reason: str = ''
    extra: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        payload = {
            'trade_index': self.index,
            'strategy': self.strategy,
            'side': self.side,
            'timestamp': self.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry_time': self.entry_time.strftime('%Y-%m-%d %H:%M:%S'),
            'entry': round(self.entry_price, 4),
            'entry_price': round(self.entry_price, 4),
            'stop_loss': round(self.stop_loss, 4),
            'target': round(self.target_price, 4),
            'target_price': round(self.target_price, 4),
            'quantity': int(self.quantity),
            'score': round(self.score, 2),
            'reason': self.reason,
            'trade_status': self.status,
            'exit_time': '' if self.exit_time is None else self.exit_time.strftime('%Y-%m-%d %H:%M:%S'),
            'exit_price': '' if self.exit_price is None else round(self.exit_price, 4),
            'exit_reason': self.exit_reason,
            'pnl': round(self.pnl, 2),
            'rr_achieved': round(self.rr_achieved, 2),
            'setup_type': self.setup_type,
            'rejection_reason': self.rejection_reason,
        }
        payload.update(self.extra)
        return payload


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _validate_trade_candidate(trade: dict[str, object], index: int) -> tuple[bool, str, dict[str, object]]:
    record = dict(trade)
    side = str(record.get('side', '')).strip().upper()
    if side not in {'BUY', 'SELL'}:
        return False, 'invalid_side', record
    timestamp = pd.to_datetime(record.get('timestamp', record.get('entry_time')), errors='coerce')
    if pd.isna(timestamp):
        return False, 'invalid_timestamp', record
    entry = _safe_float(record.get('entry', record.get('entry_price')))
    stop_loss = _safe_float(record.get('stop_loss'))
    target = _safe_float(record.get('target', record.get('target_price')))
    if entry <= 0:
        return False, 'invalid_entry', record
    if stop_loss <= 0:
        return False, 'invalid_stop_loss', record
    if target <= 0:
        return False, 'invalid_target', record
    if side == 'BUY' and not (stop_loss < entry < target):
        return False, 'buy_levels_not_ordered', record
    if side == 'SELL' and not (target < entry < stop_loss):
        return False, 'sell_levels_not_ordered', record
    quantity = _safe_int(record.get('quantity'), 1)
    if quantity <= 0:
        quantity = 1
    record['side'] = side
    record['timestamp'] = timestamp
    record['entry'] = entry
    record['entry_price'] = entry
    record['stop_loss'] = stop_loss
    record['target'] = target
    record['target_price'] = target
    record['quantity'] = quantity
    record['score'] = _safe_float(record.get('score'))
    record['reason'] = str(record.get('reason', record.get('strategy', 'TRADE')) or 'TRADE')
    record['trade_index'] = index
    return True, '', record


def _build_lifecycle(record: dict[str, object]) -> _TradeLifecycle:
    extra = {
        key: value
        for key, value in record.items()
        if key not in {'trade_index', 'strategy', 'side', 'timestamp', 'entry', 'entry_price', 'stop_loss', 'target', 'target_price', 'quantity', 'score', 'reason'}
    }
    lifecycle = _TradeLifecycle(
        index=int(record['trade_index']),
        strategy=str(record.get('strategy', 'STRATEGY') or 'STRATEGY'),
        side=str(record['side']),
        entry_time=pd.Timestamp(record['timestamp']),
        entry_price=float(record['entry']),
        stop_loss=float(record['stop_loss']),
        target_price=float(record['target']),
        quantity=int(record['quantity']),
        score=float(record.get('score', 0.0) or 0.0),
        reason=str(record.get('reason', 'TRADE') or 'TRADE'),
        setup_type=str(record.get('setup_type', '') or ''),
        rejection_reason=str(record.get('rejection_reason', '') or ''),
        extra=extra,
    )
    lifecycle.status = 'entered'
    return lifecycle


def _dedupe_candidates(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[tuple[str, str, str, float]] = set()
    unique: list[dict[str, object]] = []
    for candidate in candidates:
        key = (
            str(candidate.get('strategy', '')),
            str(candidate.get('side', '')),
            str(pd.Timestamp(candidate.get('timestamp')).strftime('%Y-%m-%d %H:%M:%S')),
            round(_safe_float(candidate.get('entry')), 4),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _simulate_trade_exit(prepared: pd.DataFrame, trade: _TradeLifecycle, *, close_open_positions_at_end: bool) -> _TradeLifecycle:
    future = prepared[prepared['timestamp'] >= trade.entry_time].copy()
    if future.empty:
        trade.status = 'invalid'
        trade.exit_reason = 'NO_FUTURE_DATA'
        return trade

    for row in future.itertuples(index=False):
        candle_time = pd.Timestamp(row.timestamp)
        if trade.side == 'BUY':
            if float(row.low) <= trade.stop_loss:
                trade.exit_price = trade.stop_loss
                trade.exit_time = candle_time
                trade.exit_reason = 'STOP_LOSS'
                trade.status = 'closed'
                break
            if float(row.high) >= trade.target_price:
                trade.exit_price = trade.target_price
                trade.exit_time = candle_time
                trade.exit_reason = 'TARGET'
                trade.status = 'closed'
                break
        else:
            if float(row.high) >= trade.stop_loss:
                trade.exit_price = trade.stop_loss
                trade.exit_time = candle_time
                trade.exit_reason = 'STOP_LOSS'
                trade.status = 'closed'
                break
            if float(row.low) <= trade.target_price:
                trade.exit_price = trade.target_price
                trade.exit_time = candle_time
                trade.exit_reason = 'TARGET'
                trade.status = 'closed'
                break

    if trade.status != 'closed' and close_open_positions_at_end:
        last_row = future.iloc[-1]
        trade.exit_price = float(last_row['close'])
        trade.exit_time = pd.Timestamp(last_row['timestamp'])
        trade.exit_reason = 'END_OF_DATA'
        trade.status = 'closed'

    if trade.exit_price is not None:
        if trade.side == 'BUY':
            trade.pnl = (trade.exit_price - trade.entry_price) * trade.quantity
        else:
            trade.pnl = (trade.entry_price - trade.exit_price) * trade.quantity
        risk_per_unit = abs(trade.entry_price - trade.stop_loss)
        trade.rr_achieved = 0.0 if risk_per_unit <= 0 else abs(trade.exit_price - trade.entry_price) / risk_per_unit
    return trade


def _equity_curve_rows(trades: list[dict[str, object]], capital: float) -> list[dict[str, object]]:
    equity = float(capital)
    peak = equity
    rows: list[dict[str, object]] = []
    for trade in trades:
        equity += _safe_float(trade.get('pnl'))
        peak = max(peak, equity)
        drawdown = equity - peak
        rows.append(
            {
                'timestamp': str(trade.get('exit_time') or trade.get('entry_time') or trade.get('timestamp') or ''),
                'equity': round(equity, 2),
                'drawdown': round(drawdown, 2),
                'strategy': str(trade.get('strategy', '')),
            }
        )
    return rows


def _score_bucket(score: float) -> str:
    if score >= 8:
        return '8+'
    if score >= 6:
        return '6-7.99'
    if score >= 4:
        return '4-5.99'
    if score >= 2:
        return '2-3.99'
    return '<2'


def _summary_from_trades(trades: list[dict[str, object]], cfg: BacktestConfig) -> dict[str, object]:
    total_trades = len(trades)
    wins = sum(1 for trade in trades if _safe_float(trade.get('pnl')) > 0)
    losses = sum(1 for trade in trades if _safe_float(trade.get('pnl')) < 0)
    total_pnl = sum(_safe_float(trade.get('pnl')) for trade in trades)
    avg_pnl = total_pnl / total_trades if total_trades else 0.0
    avg_rr = sum(_safe_float(trade.get('rr_achieved')) for trade in trades) / total_trades if total_trades else 0.0
    winning_trades = [_safe_float(trade.get('pnl')) for trade in trades if _safe_float(trade.get('pnl')) > 0]
    losing_trades = [_safe_float(trade.get('pnl')) for trade in trades if _safe_float(trade.get('pnl')) < 0]
    avg_win = sum(winning_trades) / len(winning_trades) if winning_trades else 0.0
    avg_loss = sum(losing_trades) / len(losing_trades) if losing_trades else 0.0
    gross_profit = sum(winning_trades)
    gross_loss = abs(sum(losing_trades))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf') if gross_profit > 0 else 0.0
    expectancy_per_trade = avg_pnl
    avg_r_winners = sum(_safe_float(trade.get('rr_achieved')) for trade in trades if _safe_float(trade.get('pnl')) > 0) / len(winning_trades) if winning_trades else 0.0
    avg_r_losers = sum(_safe_float(trade.get('rr_achieved')) for trade in trades if _safe_float(trade.get('pnl')) < 0) / len(losing_trades) if losing_trades else 0.0
    expectancy_r = ((wins / total_trades) * avg_r_winners) - ((losses / total_trades) * avg_r_losers) if total_trades else 0.0
    equity_rows = _equity_curve_rows(trades, float(cfg.capital))
    max_drawdown = abs(min((row['drawdown'] for row in equity_rows), default=0.0))

    pnl_by_strategy: dict[str, float] = {}
    score_bucket_analysis: dict[str, dict[str, float]] = {}
    for trade in trades:
        strategy = str(trade.get('strategy', cfg.strategy_name) or cfg.strategy_name)
        pnl_by_strategy[strategy] = pnl_by_strategy.get(strategy, 0.0) + _safe_float(trade.get('pnl'))
        bucket = _score_bucket(_safe_float(trade.get('score')))
        bucket_row = score_bucket_analysis.setdefault(bucket, {'count': 0.0, 'pnl': 0.0, 'wins': 0.0, 'expectancy': 0.0})
        trade_pnl = _safe_float(trade.get('pnl'))
        bucket_row['count'] += 1.0
        bucket_row['pnl'] += trade_pnl
        bucket_row['expectancy'] = bucket_row['pnl'] / bucket_row['count'] if bucket_row['count'] else 0.0
        if trade_pnl > 0:
            bucket_row['wins'] += 1.0

    return {
        'strategy': cfg.strategy_name,
        'total_trades': total_trades,
        'wins': wins,
        'losses': losses,
        'win_rate': round((wins / total_trades) * 100.0, 2) if total_trades else 0.0,
        'total_pnl': round(total_pnl, 2),
        'avg_pnl': round(avg_pnl, 2),
        'avg_win': round(avg_win, 2),
        'avg_loss': round(avg_loss, 2),
        'profit_factor': round(profit_factor, 2) if profit_factor != float('inf') else 'inf',
        'expectancy_per_trade': round(expectancy_per_trade, 2),
        'expectancy_r': round(expectancy_r, 2),
        'positive_expectancy': 'YES' if expectancy_per_trade > 0 else 'NO',
        'max_drawdown': round(max_drawdown, 2),
        'avg_rr': round(avg_rr, 2),
        'pnl_by_strategy': '; '.join(f'{key}:{value:.2f}' for key, value in sorted(pnl_by_strategy.items())),
        'score_bucket_analysis': '; '.join(
            f"{bucket}=count:{int(values['count'])},wins:{int(values['wins'])},pnl:{values['pnl']:.2f},expectancy:{values['expectancy']:.2f}"
            for bucket, values in sorted(score_bucket_analysis.items())
        ),
        'trades_output': str(cfg.trades_output),
        'summary_output': str(cfg.summary_output),
    }


def run_backtest(df: Any, strategy_func: Callable[..., list[dict[str, object]]], config: BacktestConfig | dict[str, object] | None = None) -> dict[str, object]:
    """Run a full historical trade simulation for a standardized strategy."""
    cfg = config if isinstance(config, BacktestConfig) else BacktestConfig(**(config or {}))
    prepared = prepare_trading_data(df)
    strategy_config = getattr(cfg, 'strategy_config', None)

    try:
        raw_trades = strategy_func(
            prepared,
            capital=float(cfg.capital),
            risk_pct=float(cfg.risk_pct),
            rr_ratio=float(cfg.rr_ratio),
            config=strategy_config,
        )
    except Exception as exc:
        append_log(f'backtest_engine strategy execution failed: {exc}')
        write_rows(cfg.trades_output, [])
        summary = {
            'strategy': cfg.strategy_name,
            'total_trades': 0,
            'wins': 0,
            'losses': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_pnl': 0.0,
            'max_drawdown': 0.0,
            'avg_rr': 0.0,
            'pnl_by_strategy': '',
            'score_bucket_analysis': '',
            'trades_output': str(cfg.trades_output),
            'summary_output': str(cfg.summary_output),
            'error': str(exc),
        }
        write_rows(cfg.summary_output, [summary])
        return summary

    validated_candidates: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []
    for index, trade in enumerate(raw_trades or [], start=1):
        valid, reason, normalized = _validate_trade_candidate(trade, index)
        if not valid:
            append_log(f'backtest_engine rejected malformed trade #{index}: {reason}')
            normalized['trade_status'] = 'rejected'
            normalized['rejection_reason'] = reason
            rejected_rows.append(normalized)
            continue
        validated_candidates.append(normalized)

    candidates = _dedupe_candidates(validated_candidates)
    simulated: list[dict[str, object]] = []
    active_exit_cutoff: pd.Timestamp | None = None

    for record in sorted(candidates, key=lambda item: pd.Timestamp(item['timestamp'])):
        lifecycle = _build_lifecycle(record)
        if not cfg.allow_overlap and active_exit_cutoff is not None and lifecycle.entry_time <= active_exit_cutoff:
            skipped = lifecycle.to_dict()
            skipped['trade_status'] = 'rejected'
            skipped['rejection_reason'] = 'OVERLAPPING_TRADE'
            simulated.append(skipped)
            continue
        lifecycle = _simulate_trade_exit(prepared, lifecycle, close_open_positions_at_end=cfg.close_open_positions_at_end)
        lifecycle_row = lifecycle.to_dict()
        simulated.append(lifecycle_row)
        if lifecycle.exit_time is not None and lifecycle.status == 'closed':
            active_exit_cutoff = lifecycle.exit_time

    closed_trades = [row for row in simulated if str(row.get('trade_status', '')) == 'closed']
    all_rows = simulated + rejected_rows
    write_rows(cfg.trades_output, all_rows)
    summary = _summary_from_trades(closed_trades, cfg)
    summary['rejected_candidates'] = len(rejected_rows)
    summary['closed_trades'] = len(closed_trades)
    write_rows(cfg.summary_output, [summary])
    return summary

