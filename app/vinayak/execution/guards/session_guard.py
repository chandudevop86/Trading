from __future__ import annotations

"""Session-window and daily-limit guard evaluation."""

from datetime import time

from vinayak.execution.guards.types import WorkspaceGuardContext, WorkspaceGuardResult


_DEFAULT_SESSION_START = time(9, 15)
_DEFAULT_SESSION_END = time(15, 30)


def evaluate_session_guard(context: WorkspaceGuardContext) -> WorkspaceGuardResult:
    reasons: list[str] = []
    if context.signal_time.time() < _DEFAULT_SESSION_START or context.signal_time.time() > _DEFAULT_SESSION_END:
        reasons.append("OUTSIDE_SESSION")

    executed_today = context.state_repository.executed_trade_count(signal_date=context.signal_time, mode=context.execution_mode)
    if context.max_trades_per_day and context.max_trades_per_day > 0 and executed_today >= int(context.max_trades_per_day):
        reasons.append("MAX_TRADES_PER_DAY")

    realized_pnl = float(context.state_repository.realized_pnl_for_day(signal_date=context.signal_time, mode=context.execution_mode) or 0.0)
    if context.max_daily_loss and float(context.max_daily_loss) > 0 and realized_pnl <= -abs(float(context.max_daily_loss)):
        reasons.append("MAX_DAILY_LOSS")
    return WorkspaceGuardResult(reasons=reasons)

