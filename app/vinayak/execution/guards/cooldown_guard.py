from __future__ import annotations

"""Cooldown guard evaluation."""

from vinayak.execution.guards.types import WorkspaceGuardContext, WorkspaceGuardResult


def evaluate_cooldown_guard(context: WorkspaceGuardContext) -> WorkspaceGuardResult:
    if context.cooldown_minutes <= 0:
        return WorkspaceGuardResult()
    executed_times = context.state_repository.executed_trade_times(signal_date=context.signal_time, mode=context.execution_mode)
    if not executed_times:
        return WorkspaceGuardResult()
    delta_seconds = (context.signal_time - max(executed_times)).total_seconds()
    if delta_seconds < context.cooldown_minutes * 60:
        return WorkspaceGuardResult(reasons=["COOLDOWN_ACTIVE"])
    return WorkspaceGuardResult()

