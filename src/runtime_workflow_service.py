from __future__ import annotations

import os
from typing import Any, Callable

import pandas as pd

from src.backtest_engine import run_backtest, summarize_trade_log
from src.execution.guards import execute_paper_trades
from src.execution.pipeline import prepare_candidates_for_execution
from src.execution_engine import (
    close_paper_trades,
    execute_live_trades,
    execution_result_summary,
)
from src.runtime_defaults import (
    BACKTEST_RESULTS_OUTPUT,
    BACKTEST_SUMMARY_OUTPUT,
    BACKTEST_TRADES_OUTPUT,
    BACKTEST_VALIDATION_OUTPUT,
    EXECUTED_TRADES_OUTPUT,
    LIVE_LOG_OUTPUT,
    LIVE_OHLCV_OUTPUT,
    OHLCV_OUTPUT,
    OPTIMIZER_OUTPUT,
    ORDER_HISTORY_OUTPUT,
    PAPER_LOG_OUTPUT,
    PAPER_ORDER_HISTORY_OUTPUT,
    PAPER_SUMMARY_OUTPUT,
    SIGNAL_OUTPUT,
    TRADES_OUTPUT,
)
from src.runtime_file_service import mirror_output_file, save_runtime_outputs
from src.runtime_models import TradingActionRequest, period_for_interval
from src.runtime_persistence import load_current_rows, load_latest_batch_rows
from src.runtime_strategy_presets import runtime_strategy_kwargs
from src.strategy_tuning import normalize_strategy_key, strategy_backtest_config
from src.trading_core import prepare_trading_data
from src.validation.engine import validate_trade


def paper_candle_rows(candles: pd.DataFrame) -> list[dict[str, object]]:
    return prepare_trading_data(candles).to_dict(orient="records")


def _validate_execution_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    trades: list[dict[str, object]],
) -> list[dict[str, object]]:
    validated: list[dict[str, object]] = []
    for trade in trades:
        item = dict(trade)
        item.setdefault("symbol", symbol)
        item.setdefault("strategy", strategy)
        validation = validate_trade(item, candles)
        item["validation_status"] = str(validation.get("decision", "FAIL") or "FAIL").upper()
        item["validation_score"] = round(float(validation.get("score", 0.0) or 0.0), 2)
        item["validation_reasons"] = list(validation.get("reasons", []) or [])
        item["reason_codes"] = list(item.get("reason_codes", []) or []) + item["validation_reasons"]
        item["execution_allowed"] = item["validation_status"] == "PASS"
        item["validation_metrics"] = dict(validation.get("metrics", {}) or {})
        validated.append(item)
    return validated



def refresh_paper_trade_summary(candles: pd.DataFrame, capital: float) -> dict[str, object]:
    close_paper_trades(EXECUTED_TRADES_OUTPUT, paper_candle_rows(candles), max_hold_minutes=60)
    return summarize_trade_log(
        EXECUTED_TRADES_OUTPUT,
        capital=float(capital),
        strategy_name="PAPER_EXECUTION",
        summary_output=PAPER_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_OUTPUT,
    )


def latest_actionable_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    if not trades:
        return []
    stamped: list[tuple[pd.Timestamp, dict[str, object]]] = []
    for trade in trades:
        raw = trade.get("signal_time") or trade.get("entry_time") or trade.get("timestamp") or ""
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            continue
        stamped.append((pd.Timestamp(parsed), dict(trade)))
    if not stamped:
        return [dict(trades[-1])]
    latest_stamp = max(stamp for stamp, _ in stamped)
    latest_rows = [row for stamp, row in stamped if stamp == latest_stamp]
    return latest_rows or [dict(trades[-1])]


def latest_optimizer_gate(strategy: str) -> tuple[bool, str]:
    rows = load_current_rows(OPTIMIZER_OUTPUT) or load_latest_batch_rows(OPTIMIZER_OUTPUT)
    if not rows:
        if not OPTIMIZER_OUTPUT.exists() or OPTIMIZER_OUTPUT.stat().st_size == 0:
            return False, 'live deployment locked: optimizer report missing'
        try:
            frame = pd.read_csv(OPTIMIZER_OUTPUT)
        except Exception:
            return False, 'live deployment locked: optimizer report unreadable'
        if frame.empty:
            return False, 'live deployment locked: optimizer report empty'
        rows = frame.to_dict(orient='records')
    frame = pd.DataFrame(rows)
    if frame.empty:
        return False, 'live deployment locked: optimizer report empty'
    strategy_key = normalize_strategy_key(strategy)
    frame['normalized_strategy'] = frame['strategy'].map(lambda value: normalize_strategy_key(str(value)))
    matched = frame[frame['normalized_strategy'] == strategy_key].copy()
    if matched.empty:
        return False, f'live deployment locked: no optimizer row for {strategy}'
    if 'optimizer_rank' in matched.columns:
        matched = matched.sort_values('optimizer_rank')
    elif 'rank_score' in matched.columns:
        matched = matched.sort_values('rank_score', ascending=False)
    row = matched.iloc[0].to_dict()
    deployment_ready = str(row.get('deployment_ready', 'NO')).strip().upper() == 'YES'
    blockers = str(row.get('deployment_blockers', '') or '').strip()
    if deployment_ready:
        return True, 'deployment_ready=YES'
    reason = blockers or 'deployment_ready must be YES'
    return False, f'live deployment locked: {reason}'


def status_message(run_clicked: bool, backtest_clicked: bool) -> str:
    if backtest_clicked:
        return "Backtest completed"
    if run_clicked:
        return "Run completed"
    return "Ready"


def run_live_strategy(
    request: TradingActionRequest,
    *,
    fetch_ohlcv_data_fn: Callable[[str, str, str], pd.DataFrame],
    run_strategy_fn: Callable[..., list[dict[str, object]]],
) -> tuple[pd.DataFrame, list[dict[str, object]], str]:
    period = period_for_interval(request.timeframe)
    candles = fetch_ohlcv_data_fn(request.symbol, request.timeframe, period)
    trades = run_strategy_fn(
        strategy=request.strategy,
        candles=candles,
        capital=float(request.capital),
        risk_pct=float(request.risk_pct),
        rr_ratio=float(request.rr_ratio),
        symbol=request.symbol,
        **runtime_strategy_kwargs(request.mode),
    )
    normalized_trades: list[dict[str, object]] = []
    for trade in trades:
        item = dict(trade)
        item.setdefault("symbol", request.symbol)
        item.setdefault("timeframe", request.timeframe)
        item.setdefault("duplicate_signal_cooldown_bars", item.get("duplicate_signal_cooldown_bars", 0))
        normalized_trades.append(item)
    save_runtime_outputs(
        candles,
        normalized_trades,
        ohlcv_output=OHLCV_OUTPUT,
        live_ohlcv_output=LIVE_OHLCV_OUTPUT,
        trades_output=TRADES_OUTPUT,
        signal_output=SIGNAL_OUTPUT,
        executed_trades_output=EXECUTED_TRADES_OUTPUT,
        paper_log_output=PAPER_LOG_OUTPUT,
        live_log_output=LIVE_LOG_OUTPUT,
    )
    return candles, normalized_trades, period


def run_strategy_backtest(
    candles: pd.DataFrame,
    request: TradingActionRequest,
    *,
    strategy_callable_fn: Callable[[str, str], Callable[[pd.DataFrame, float, float, float, Any], list[dict[str, object]]]],
) -> dict[str, object]:
    config = strategy_backtest_config(
        request.strategy,
        capital=float(request.capital),
        risk_pct=float(request.risk_pct) / 100.0,
        rr_ratio=float(request.rr_ratio),
        trades_output=BACKTEST_TRADES_OUTPUT,
        summary_output=BACKTEST_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_OUTPUT,
    )
    summary = run_backtest(candles, strategy_callable_fn(request.strategy, request.symbol), config)
    mirror_output_file(BACKTEST_SUMMARY_OUTPUT, BACKTEST_RESULTS_OUTPUT)
    return summary


def run_execution(
    request: TradingActionRequest,
    trades: list[dict[str, object]],
    candles: pd.DataFrame,
) -> tuple[object | None, list[tuple[str, str]], str]:
    if not trades:
        return None, [("info", "No actionable trade candidates")], "Paper standby"
    actionable_trades = latest_actionable_trades(trades)
    if not actionable_trades:
        return None, [("info", "No actionable trade candidates")], "Paper standby"
    candidates = prepare_candidates_for_execution(request.strategy, request.symbol, candles, actionable_trades)
    if request.broker_choice == "Dhan Live":
        optimizer_ready, optimizer_reason = latest_optimizer_gate(request.strategy)
        if not optimizer_ready:
            return None, [("warning", f"Live blocked: {optimizer_reason}")], "Live broker blocked by optimizer gate"
        live_enabled = str(os.getenv("LIVE_TRADING_ENABLED", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        result = execute_live_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            broker_name="DHAN",
            live_enabled=live_enabled,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=ORDER_HISTORY_OUTPUT,
        )
        mirror_output_file(EXECUTED_TRADES_OUTPUT, LIVE_LOG_OUTPUT)
        status = "Live broker armed" if live_enabled else "Live broker blocked by config"
    else:
        result = execute_paper_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=PAPER_ORDER_HISTORY_OUTPUT,
        )
        mirror_output_file(EXECUTED_TRADES_OUTPUT, PAPER_LOG_OUTPUT)
        refresh_paper_trade_summary(candles, request.capital)
        status = "Paper broker active"
    return result, execution_result_summary(result), status











