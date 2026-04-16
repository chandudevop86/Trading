from __future__ import annotations

"""Execution coordination for live analysis workflows."""

from typing import Any, Callable


def coordinate_workspace_execution(
    *,
    auto_execute: bool,
    execution_mode: str,
    execution_note: str,
    signal_rows: list[dict[str, Any]],
    execute_inline: bool,
    strategy: str,
    symbol: str,
    candles_df: Any,
    paper_log_path: str,
    live_log_path: str,
    capital: float,
    risk_pct: float,
    max_trades_per_day: int | None,
    max_daily_loss: float | None,
    max_position_value: float | None,
    max_open_positions: int | None,
    max_symbol_exposure_pct: float | None,
    max_portfolio_exposure_pct: float | None,
    max_open_risk_pct: float | None,
    kill_switch_enabled: bool,
    security_map_path: str,
    db_session: Any,
    execute_workspace_candidates_fn: Callable[..., tuple[list[dict[str, Any]], Any]],
    normalize_rows_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    resolve_live_kwargs_fn: Callable[[str], dict[str, object]],
) -> dict[str, Any]:
    execution_summary: dict[str, Any] = {
        "mode": execution_mode,
        "executed_count": 0,
        "blocked_count": 0,
        "error_count": 0,
        "skipped_count": 0,
        "duplicate_count": 0,
    }
    execution_rows: list[dict[str, Any]] = []
    resolved_note = execution_note

    if auto_execute and execution_mode in {"PAPER", "LIVE"} and signal_rows and execute_inline:
        _candidates, result = execute_workspace_candidates_fn(
            strategy,
            symbol,
            candles_df,
            signal_rows,
            execution_mode=execution_mode,
            paper_log_path=str(paper_log_path),
            live_log_path=str(live_log_path),
            capital=capital,
            per_trade_risk_pct=risk_pct,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
            max_position_value=max_position_value,
            max_open_positions=max_open_positions,
            max_symbol_exposure_pct=max_symbol_exposure_pct,
            max_portfolio_exposure_pct=max_portfolio_exposure_pct,
            max_open_risk_pct=max_open_risk_pct,
            kill_switch_enabled=kill_switch_enabled,
            security_map_path=str(security_map_path),
            resolve_live_kwargs=resolve_live_kwargs_fn,
            db_session=db_session,
        )
        execution_summary = {
            "mode": execution_mode,
            "executed_count": result.executed_count,
            "blocked_count": result.blocked_count,
            "error_count": result.error_count,
            "skipped_count": result.skipped_count,
            "duplicate_count": result.duplicate_count,
        }
        execution_rows = normalize_rows_fn(result.rows)
    elif auto_execute and execution_mode in {"PAPER", "LIVE"} and signal_rows and not execute_inline:
        resolved_note = (f"{execution_note} " if execution_note else "") + "Auto execution deferred from the live-analysis critical path."
        execution_summary = {
            "mode": execution_mode,
            "executed_count": 0,
            "blocked_count": 0,
            "error_count": 0,
            "skipped_count": len(signal_rows),
            "duplicate_count": 0,
        }

    return {
        "execution_note": resolved_note,
        "execution_summary": execution_summary,
        "execution_rows": execution_rows,
    }

