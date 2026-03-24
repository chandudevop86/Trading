from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.live_ohlcv import fetch_live_ohlcv, write_csv
from src.strategy_service import StrategyContext, generate_strategy_rows
from src.trading_workflows import build_backtest_workflow, run_live_candidates, run_paper_candidates


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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


def _pnl_summary(
    strategy: str,
    rows: list[dict[str, Any]],
    *,
    starting_equity: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pnl_values = [_safe_float(r.get('pnl')) for r in rows]
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
    summary = {
        'strategy': strategy,
        'trades': total,
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
        'total_pnl': round(sum(pnl_values), 2),
    }
    return summary, equity_curve_rows


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
        return
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Auto backtest all bots and write paper logs with timeframe metadata')
    parser.add_argument('--symbol', default='^NSEI')
    parser.add_argument('--interval', default='5m', choices=['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'])
    parser.add_argument('--period', default='3mo')
    parser.add_argument('--capital', type=float, default=100000.0)
    parser.add_argument('--risk-pct', type=float, default=0.01)
    parser.add_argument('--rr-ratio', type=float, default=2.0)
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
    parser.add_argument('--equity-curve-output', type=Path, default=Path('data/backtest_equity_curves.csv'))
    parser.add_argument('--paper-log-output', type=Path, default=Path('data/paper_trading_logs_all.csv'))
    parser.add_argument('--execution-type', default='PAPER', choices=['PAPER', 'LIVE', 'NONE'])
    parser.add_argument('--live-log-output', type=Path, default=Path('data/live_trading_logs_all.csv'))
    parser.add_argument('--live-broker', default='DHAN', choices=['DHAN', 'NONE'])
    parser.add_argument('--security-map', type=Path, default=Path('data/dhan_security_map.csv'))
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Any]:
    from src.breakout_bot import generate_trades as generate_breakout_trades
    from src.breakout_bot import load_candles
    from src.indicator_bot import IndicatorConfig, generate_indicator_rows

    run_at = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    capital = float(getattr(args, 'capital', 100000.0) or 100000.0)
    risk_pct = float(getattr(args, 'risk_pct', 0.01) or 0.01)
    rr_ratio = float(getattr(args, 'rr_ratio', 2.0) or 2.0)
    trailing_sl_pct = float(getattr(args, 'trailing_sl_pct', 0.0) or 0.0)
    pivot_window = int(getattr(args, 'pivot_window', 2) or 2)
    entry_cutoff = str(getattr(args, 'entry_cutoff', '11:30') or '11:30')
    cost_bps = float(getattr(args, 'cost_bps', 0.0) or 0.0)
    fixed_cost_per_trade = float(getattr(args, 'fixed_cost_per_trade', 0.0) or 0.0)
    max_trades_per_day = int(getattr(args, 'max_trades_per_day', 1) or 1)
    max_daily_loss_value = float(getattr(args, 'max_daily_loss', 0.0) or 0.0)
    max_daily_loss = max_daily_loss_value if max_daily_loss_value > 0 else None
    equity_curve_output = Path(getattr(args, 'equity_curve_output', Path('data/backtest_equity_curves.csv')))

    rows = fetch_live_ohlcv(args.symbol, args.interval, args.period)
    if not rows:
        raise ValueError('No OHLCV rows fetched.')
    write_csv(rows, str(args.data_output))
    timeframe = str(getattr(args, 'interval', '') or '')
    data_start = str(rows[0].get('timestamp', '')) if rows else ''
    data_end = str(rows[-1].get('timestamp', '')) if rows else ''

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
    indicator_rows = generate_indicator_rows(candles, config=indicator_cfg)
    one_trade_rows = generate_strategy_rows(one_trade_context)
    btst_rows = generate_strategy_rows(btst_context)
    summary_and_curves = [
        _pnl_summary('BREAKOUT', breakout_rows, starting_equity=capital),
        _pnl_summary('BREAKOUT_NO_BIAS', breakout_no_bias_rows, starting_equity=capital),
        _pnl_summary('DEMAND_SUPPLY', ds_rows, starting_equity=capital),
        _pnl_summary('ONE_TRADE_DAY', one_trade_rows, starting_equity=capital),
        _pnl_summary('BTST', btst_rows, starting_equity=capital),
    ]
    summary_rows = [summary for summary, _ in summary_and_curves]
    ranked_summary_rows = rank_strategy_summaries(summary_rows)
    equity_curve_rows = [curve_row for _, curve_rows in summary_and_curves for curve_row in curve_rows]
    breakout_bias_evaluation = _build_breakout_bias_evaluation(summary_rows[0], summary_rows[1])
    for summary in summary_rows:
        summary['timeframe'] = timeframe
        summary['data_start'] = data_start
        summary['data_end'] = data_end
        summary['run_at_utc'] = run_at
        summary['equity_curve_output'] = str(equity_curve_output)

    for curve_row in equity_curve_rows:
        curve_row['timeframe'] = timeframe
        curve_row['data_start'] = data_start
        curve_row['data_end'] = data_end
        curve_row['run_at_utc'] = run_at

    _write_rows(args.summary_output, summary_rows)
    _append_rows(args.summary_history_output, summary_rows)
    _write_rows(args.ranking_output, ranked_summary_rows)
    _write_rows(equity_curve_output, equity_curve_rows)

    breakout_workflow = build_backtest_workflow(breakout_rows, 'Breakout (15m)', args.execution_symbol)
    ds_workflow = build_backtest_workflow(ds_rows, 'Demand/Supply', args.execution_symbol)
    ind_workflow = build_backtest_workflow(indicator_rows, 'Indicator (RSI/ADX/MACD+VWAP)', args.execution_symbol)
    one_workflow = build_backtest_workflow(one_trade_rows, 'One Trade/Day (All Indicators)', args.execution_symbol)
    btst_workflow = build_backtest_workflow(btst_rows, 'BTST', args.execution_symbol)
    candidates = (
        breakout_workflow.execution_candidates
        + ds_workflow.execution_candidates
        + ind_workflow.execution_candidates
        + one_workflow.execution_candidates
        + btst_workflow.execution_candidates
    )

    for candidate in candidates:
        candidate['timeframe'] = timeframe
        candidate['data_start'] = data_start
        candidate['data_end'] = data_end
        candidate['backtest_run_at_utc'] = run_at

    execution_type = str(getattr(args, 'execution_type', 'PAPER') or 'PAPER').strip().upper()
    executed_log_path = args.paper_log_output
    paper_rows: list[dict[str, object]] = []

    if execution_type == 'NONE':
        paper_rows = []
    elif execution_type == 'LIVE':
        executed_log_path = getattr(args, 'live_log_output', Path('data/live_trading_logs_all.csv'))
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
            deduplicate=False,
            broker_name=live_broker,
            security_map=security_map,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
        )
        paper_rows = list(getattr(live_result.execution_result, 'rows', []))
    else:
        execution_type = 'PAPER'
        paper_result = run_paper_candidates(
            candidates,
            output_path=args.paper_log_output,
            deduplicate=False,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
        )
        paper_rows = list(getattr(paper_result.execution_result, 'rows', []))
    return {
        'summary_rows': summary_rows,
        'equity_curve_rows': equity_curve_rows,
        'equity_curve_output': str(equity_curve_output),
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
    for row in out['summary_rows']:
        print(
            f"{row['strategy']}: trades={row['trades']} wins={row['wins']} "
            f"losses={row['losses']} gross_pnl={row['gross_total_pnl']} "
            f"costs={row['total_trading_cost']} pnl={row['total_pnl']} win_rate={row['win_rate_pct']}% "
            f"avg_win={row['avg_win']} avg_loss={row['avg_loss']} "
            f"pf={row['profit_factor']} max_dd={row['max_drawdown']} ({row['max_drawdown_pct']}%)"
        )
    print(f"Equity curves: {out['equity_curve_output']}")


if __name__ == '__main__':
    main()



