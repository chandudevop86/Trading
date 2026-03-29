from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.execution.guards import execute_candidates
from src.execution.pipeline import prepare_candidates_for_execution


def prepare_workspace_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return prepare_candidates_for_execution(strategy, symbol, candles, signal_rows)


def execute_workspace_candidates(
    strategy: str,
    symbol: str,
    candles: pd.DataFrame,
    signal_rows: list[dict[str, Any]],
    *,
    execution_mode: str,
    paper_log_path: str,
    live_log_path: str,
    max_trades_per_day: int | None = None,
    max_daily_loss: float | None = None,
    security_map_path: str = 'data/dhan_security_map.csv',
    resolve_live_kwargs: callable | None = None,
):
    candidates = prepare_workspace_candidates(strategy, symbol, candles, signal_rows)
    mode = str(execution_mode or 'NONE').upper()
    if mode == 'LIVE':
        live_kwargs = dict(resolve_live_kwargs(security_map_path) if resolve_live_kwargs is not None else {})
        result = execute_candidates(
            candidates,
            Path(str(live_log_path)),
            deduplicate=True,
            execution_mode='LIVE',
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
            **live_kwargs,
        )
    else:
        result = execute_candidates(
            candidates,
            Path(str(paper_log_path)),
            deduplicate=True,
            execution_mode='PAPER',
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
        )
    return candidates, result


__all__ = [
    'execute_workspace_candidates',
    'prepare_workspace_candidates',
]
