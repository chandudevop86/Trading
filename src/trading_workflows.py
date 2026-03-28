from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.execution_engine import build_execution_candidates, execute_live_trades, execute_paper_trades


@dataclass(slots=True)
class WorkflowResult:
    output_rows: list[dict[str, object]]
    execution_candidates: list[dict[str, object]]
    execution_result: Any = None
    execution_type: str = 'NONE'
    log_path: str = ''


def build_backtest_workflow(output_rows: list[dict[str, object]], strategy_label: str, symbol: str) -> WorkflowResult:
    candidates = build_execution_candidates(strategy_label, output_rows, symbol)
    return WorkflowResult(
        output_rows=output_rows,
        execution_candidates=candidates,
        execution_type='BACKTEST',
    )


def run_paper_workflow(
    output_rows: list[dict[str, object]],
    strategy_label: str,
    symbol: str,
    *,
    output_path: Path,
    deduplicate: bool = True,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
) -> WorkflowResult:
    candidates = build_execution_candidates(strategy_label, output_rows, symbol)
    result = execute_paper_trades(
        candidates,
        output_path,
        deduplicate=deduplicate,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
    )
    return WorkflowResult(
        output_rows=output_rows,
        execution_candidates=candidates,
        execution_result=result,
        execution_type='PAPER',
        log_path=str(output_path),
    )


def run_live_workflow(
    output_rows: list[dict[str, object]],
    strategy_label: str,
    symbol: str,
    *,
    output_path: Path,
    deduplicate: bool = True,
    broker_client: object | None = None,
    broker_name: str | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
) -> WorkflowResult:
    candidates = build_execution_candidates(strategy_label, output_rows, symbol)
    result = execute_live_trades(
        candidates,
        output_path,
        deduplicate=deduplicate,
        broker_client=broker_client,
        broker_name=broker_name,
        security_map=security_map,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
    )
    return WorkflowResult(
        output_rows=output_rows,
        execution_candidates=candidates,
        execution_result=result,
        execution_type='LIVE',
        log_path=str(output_path),
    )


def run_paper_candidates(
    candidates: list[dict[str, object]],
    *,
    output_path: Path,
    deduplicate: bool = True,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
) -> WorkflowResult:
    result = execute_paper_trades(
        candidates,
        output_path,
        deduplicate=deduplicate,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
    )
    return WorkflowResult(
        output_rows=[],
        execution_candidates=candidates,
        execution_result=result,
        execution_type='PAPER',
        log_path=str(output_path),
    )


def run_live_candidates(
    candidates: list[dict[str, object]],
    *,
    output_path: Path,
    deduplicate: bool = True,
    broker_client: object | None = None,
    broker_name: str | None = None,
    security_map: dict[str, dict[str, str]] | None = None,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    max_open_trades: int | None = None,
) -> WorkflowResult:
    result = execute_live_trades(
        candidates,
        output_path,
        deduplicate=deduplicate,
        broker_client=broker_client,
        broker_name=broker_name,
        security_map=security_map,
        max_trades_per_day=max_trades_per_day,
        max_daily_loss=max_daily_loss,
        max_open_trades=max_open_trades,
    )
    return WorkflowResult(
        output_rows=[],
        execution_candidates=candidates,
        execution_result=result,
        execution_type='LIVE',
        log_path=str(output_path),
    )
