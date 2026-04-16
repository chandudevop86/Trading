from __future__ import annotations

"""Shared types for workspace execution guards."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vinayak.db.repositories.execution_state_repository import ExecutionStateRepository


@dataclass(slots=True)
class WorkspaceGuardResult:
    reasons: list[str] = field(default_factory=list)
    risk_snapshot: dict[str, Any] = field(default_factory=dict)
    candidate: dict[str, Any] | None = None


@dataclass(slots=True)
class WorkspaceGuardContext:
    candidate: dict[str, Any]
    signal_time: datetime
    execution_mode: str
    batch_keys: set[str]
    current_batch_rows: list[dict[str, Any]]
    state_repository: ExecutionStateRepository
    trade_key: str
    capital: float | None = None
    per_trade_risk_pct: float | None = None
    max_trades_per_day: int | None = None
    max_daily_loss: float | None = None
    max_position_value: float | None = None
    max_open_positions: int | None = None
    max_symbol_exposure_pct: float | None = None
    max_portfolio_exposure_pct: float | None = None
    max_open_risk_pct: float | None = None
    kill_switch_enabled: bool = False
    cooldown_minutes: int = 15
    bucket_minutes: int = 5

