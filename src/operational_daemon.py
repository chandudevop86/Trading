from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.Trading import fetch_ohlcv_data, run_strategy
from src.aws_storage import sync_path_to_s3_if_enabled
from src.backtest_engine import BacktestConfig, run_backtest, summarize_trade_log
from src.execution.guards import execute_candidates
from src.execution_engine import close_paper_trades, execution_result_summary
from src.execution.pipeline import prepare_candidates_for_execution
from src.legacy_scope import fail_noncanonical_entrypoint
from src.runtime_config import RuntimeConfig
from src.telegram_notifier import send_telegram_message
from src.trade_validation_service import build_trade_evaluation_summary
from src.trading_core import append_log, write_rows


def _append_text_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    with path.open('a', encoding='utf-8') as handle:
        handle.write(f'[{stamp}] {message}\n')


def _paper_readiness_line(executed_rows: list[dict[str, object]], strategy: str) -> str:
    if not executed_rows:
        return 'Paper readiness: no executed trades yet.'
    summary = build_trade_evaluation_summary(executed_rows, strategy_name=strategy)
    readiness_status = str(summary.get('paper_readiness_status', 'NOT_READY_FOR_PAPER_PHASE') or 'NOT_READY_FOR_PAPER_PHASE')
    readiness_summary = str(summary.get('paper_readiness_summary', '') or '').strip()
    return f"Paper readiness: {readiness_status.replace('_', ' ').title()} | {readiness_summary}"


def _build_cycle_telegram_message(config: RuntimeConfig, cycle_summary: dict[str, Any]) -> str:
    daemon = config.daemon
    broker_mode = str(config.broker.mode or 'PAPER').upper()
    execution_messages = cycle_summary.get('execution_messages') or []
    status_line = ' | '.join(message for _, message in execution_messages) if execution_messages else 'No execution events'
    readiness_line = _paper_readiness_line(list(cycle_summary.get('executed_rows') or []), daemon.strategy)
    return '\n'.join(
        [
            'Trading cycle update',
            f'Strategy: {daemon.strategy}',
            f'Symbol: {daemon.symbol}',
            f'Timeframe: {daemon.timeframe}',
            f'Broker: {broker_mode}',
            f'Candle rows: {int(cycle_summary.get("candles", 0) or 0)}',
            f'Strategy rows: {int(cycle_summary.get("trades", 0) or 0)}',
            f'Execution candidates: {int(cycle_summary.get("candidates", 0) or 0)}',
            f'Execution status: {status_line}',
            readiness_line,
        ]
    )


def _send_telegram_notification(config: RuntimeConfig, message: str, *, on_error: bool = False) -> None:
    telegram = config.telegram
    if not telegram.configured:
        return
    if on_error and not telegram.notify_on_error:
        return
    if (not on_error) and not telegram.notify_on_success:
        return
    send_telegram_message(telegram.token, telegram.chat_id, message)


def execute_trading_cycle(config: RuntimeConfig) -> dict[str, Any]:
    daemon = config.daemon
    paths = config.paths
    broker_config = config.broker
    symbol = daemon.symbol.strip()
    timeframe = daemon.timeframe.strip()
    mode = daemon.mode.strip() or 'Balanced'

    candles = fetch_ohlcv_data(symbol, interval=timeframe)
    trades = run_strategy(
        strategy=daemon.strategy,
        candles=candles,
        capital=float(daemon.capital),
        risk_pct=float(daemon.risk_pct),
        rr_ratio=float(daemon.rr_ratio),
        trailing_sl_pct=0.0,
        symbol=symbol,
        strike_step=50,
        moneyness='ATM',
        strike_steps=0,
        fetch_option_metrics=False,
        mtf_ema_period=3,
        mtf_setup_mode='either',
        mtf_retest_strength=True,
        mtf_max_trades_per_day=max(1, broker_config.max_trades_per_day),
        amd_mode=mode,
        amd_swing_window=3,
        amd_min_fvg_size=0.35,
        amd_min_bvg_size=0.25,
        amd_zone_fresh_bars=24,
        amd_retest_tolerance_pct=0.0015,
        amd_max_retest_bars=6,
        amd_min_score_conservative=7.0,
        amd_min_score_balanced=5.0,
        amd_min_score_aggressive=3.0,
    )

    write_rows(paths.ohlcv_csv, candles.to_dict(orient='records'))
    write_rows(paths.trades_csv, trades)

    candidates = prepare_candidates_for_execution(daemon.strategy, symbol, candles, trades)
    if broker_config.mode == 'LIVE':
        execution_result = execute_candidates(
            candidates,
            paths.executed_trades_csv,
            execution_mode='LIVE',
            broker_name='DHAN',
            live_enabled=broker_config.live_enabled,
            max_trades_per_day=broker_config.max_trades_per_day,
            max_daily_loss=broker_config.max_daily_loss or None,
            symbol_allowlist=list(broker_config.symbol_allowlist),
            max_order_quantity=broker_config.max_order_quantity or None,
            max_order_value=broker_config.max_order_value or None,
            order_history_path=paths.order_history_csv,
        )
    else:
        execution_result = execute_candidates(
            candidates,
            paths.executed_trades_csv,
            execution_mode='PAPER',
            max_trades_per_day=broker_config.max_trades_per_day,
            max_daily_loss=broker_config.max_daily_loss or None,
            order_history_path=paths.order_history_csv,
        )
        close_paper_trades(
            paths.executed_trades_csv,
            candles.to_dict(orient='records'),
            max_hold_minutes=60,
        )
        summarize_trade_log(
            paths.executed_trades_csv,
            capital=float(daemon.capital),
            strategy_name='PAPER_EXECUTION',
            summary_output=paths.data_dir / 'paper_trade_summary.csv',
            validation_output=paths.data_dir / 'paper_trade_validation.csv',
        )

    backtest_summary: dict[str, Any] = {}
    if daemon.run_backtest:
        backtest_summary = run_backtest(
            candles,
            lambda df, capital, risk_pct, rr_ratio, config=None: run_strategy(
                strategy=daemon.strategy,
                candles=df,
                capital=capital,
                risk_pct=risk_pct * 100 if risk_pct <= 1 else risk_pct,
                rr_ratio=rr_ratio,
                trailing_sl_pct=0.0,
                symbol=symbol,
                strike_step=50,
                moneyness='ATM',
                strike_steps=0,
                fetch_option_metrics=False,
                mtf_ema_period=3,
                mtf_setup_mode='either',
                mtf_retest_strength=True,
                mtf_max_trades_per_day=max(1, broker_config.max_trades_per_day),
                amd_mode=mode,
            ),
            BacktestConfig(
                capital=float(daemon.capital),
                risk_pct=float(daemon.risk_pct) / 100.0,
                rr_ratio=float(daemon.rr_ratio),
                trades_output=paths.backtest_trades_csv,
                summary_output=paths.backtest_summary_csv,
                strategy_name=daemon.strategy,
            ),
        )

    messages = execution_result_summary(execution_result)
    status_line = ' | '.join(message for _, message in messages) if messages else 'No execution events'
    _append_text_log(paths.app_log, f'Cycle completed strategy={daemon.strategy} broker={broker_config.mode} {status_line}')
    append_log(f'Trading cycle completed strategy={daemon.strategy} broker={broker_config.mode} {status_line}')

    for artifact in [
        paths.ohlcv_csv,
        paths.trades_csv,
        paths.executed_trades_csv,
        paths.order_history_csv,
        paths.backtest_trades_csv,
        paths.backtest_summary_csv,
    ]:
        if artifact.exists():
            try:
                sync_path_to_s3_if_enabled(artifact, key_prefix=artifact.parent.name)
            except Exception as exc:
                _append_text_log(paths.errors_log, f'S3 sync failed for {artifact}: {exc}')

    executed_rows = []
    for rows in (execution_result.executed_rows, execution_result.blocked_rows, execution_result.skipped_rows):
        if rows:
            executed_rows.extend(dict(row) for row in rows)

    cycle_summary = {
        'candles': len(candles),
        'trades': len(trades),
        'candidates': len(candidates),
        'execution_result': execution_result,
        'execution_messages': messages,
        'backtest_summary': backtest_summary,
        'executed_rows': executed_rows,
    }
    try:
        _send_telegram_notification(config, _build_cycle_telegram_message(config, cycle_summary))
    except Exception as exc:
        _append_text_log(paths.errors_log, f'Telegram success notification failed: {exc}')
    return cycle_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the trading execution daemon')
    parser.add_argument('--env-file', default='.env')
    parser.add_argument('--once', action='store_true')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = RuntimeConfig.load(args.env_file)
    paths = config.paths
    for target in [paths.data_dir, paths.logs_dir]:
        target.mkdir(parents=True, exist_ok=True)

    _append_text_log(paths.app_log, f'Daemon starting environment={config.environment} broker_mode={config.broker.mode}')
    while True:
        try:
            execute_trading_cycle(config)
        except Exception as exc:
            _append_text_log(paths.errors_log, f'Daemon cycle failed: {exc}')
            append_log(f'Daemon cycle failed: {exc}')
            try:
                _send_telegram_notification(
                    config,
                    '\n'.join(
                        [
                            'Trading cycle failed',
                            f'Strategy: {config.daemon.strategy}',
                            f'Symbol: {config.daemon.symbol}',
                            f'Timeframe: {config.daemon.timeframe}',
                            f'Broker: {config.broker.mode}',
                            f'Error: {exc}',
                        ]
                    ),
                    on_error=True,
                )
            except Exception as notify_exc:
                _append_text_log(paths.errors_log, f'Telegram error notification failed: {notify_exc}')
        if args.once:
            break
        time.sleep(max(30, int(config.daemon.poll_interval_seconds)))


if __name__ == '__main__':
    fail_noncanonical_entrypoint('src/operational_daemon.py', canonical='src.auto_run')




