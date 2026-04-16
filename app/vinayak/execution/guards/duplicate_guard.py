from __future__ import annotations

"""Duplicate-trade guard evaluation."""

from vinayak.execution.guards.types import WorkspaceGuardContext, WorkspaceGuardResult


def evaluate_duplicate_guard(context: WorkspaceGuardContext) -> WorkspaceGuardResult:
    reasons: list[str] = []
    if context.trade_key in context.batch_keys:
        reasons.append("DUPLICATE_TRADE")
    candidate = context.candidate
    if context.state_repository.has_duplicate_signal(
        symbol=str(candidate.get("symbol", "") or ""),
        side=str(candidate.get("side", "") or ""),
        strategy_name=str(candidate.get("strategy_name", "") or ""),
        signal_time=context.signal_time,
        bucket_minutes=context.bucket_minutes,
    ):
        reasons.append("DUPLICATE_TRADE")
    return WorkspaceGuardResult(reasons=list(dict.fromkeys(reasons)))

