from __future__ import annotations

from enum import StrEnum


class ReviewedTradeStatus(StrEnum):
    REVIEWED = 'REVIEWED'
    APPROVED = 'APPROVED'
    REJECTED = 'REJECTED'
    EXECUTE_REQUESTED = 'EXECUTE_REQUESTED'
    EXECUTION_REJECTED = 'EXECUTION_REJECTED'
    EXECUTED = 'EXECUTED'
    EXITED = 'EXITED'
    FAILED = 'FAILED'


class ExecutionLifecycleStatus(StrEnum):
    PENDING = 'PENDING'
    BLOCKED = 'BLOCKED'
    SKIPPED = 'SKIPPED'
    ACCEPTED = 'ACCEPTED'
    FILLED = 'FILLED'
    EXECUTED = 'EXECUTED'
    REJECTED = 'REJECTED'
    FAILED = 'FAILED'
    EXITED = 'EXITED'


class WorkflowActor(StrEnum):
    SYSTEM = 'system'
    REVIEW_SERVICE = 'reviewed_trade_service'
    EXECUTION_SERVICE = 'execution_service'
    EXECUTION_WORKFLOW = 'trade_execution_workflow'


SUCCESSFUL_EXECUTION_STATUSES = frozenset(
    {
        ExecutionLifecycleStatus.ACCEPTED,
        ExecutionLifecycleStatus.FILLED,
        ExecutionLifecycleStatus.EXECUTED,
    }
)
NON_TERMINAL_EXECUTION_STATUSES = frozenset(
    {
        ExecutionLifecycleStatus.PENDING,
        ExecutionLifecycleStatus.ACCEPTED,
    }
)
BLOCKED_EXECUTION_STATUSES = frozenset(
    {
        ExecutionLifecycleStatus.BLOCKED,
        ExecutionLifecycleStatus.SKIPPED,
        ExecutionLifecycleStatus.REJECTED,
    }
)
FAILED_EXECUTION_STATUSES = frozenset({ExecutionLifecycleStatus.FAILED})
TERMINAL_EXECUTION_STATUSES = frozenset(
    SUCCESSFUL_EXECUTION_STATUSES
    | BLOCKED_EXECUTION_STATUSES
    | FAILED_EXECUTION_STATUSES
    | {ExecutionLifecycleStatus.EXITED}
)


def normalize_reviewed_trade_status(value: str | ReviewedTradeStatus) -> ReviewedTradeStatus:
    return ReviewedTradeStatus(str(value).upper().strip())


def normalize_execution_status(value: str | ExecutionLifecycleStatus) -> ExecutionLifecycleStatus:
    return ExecutionLifecycleStatus(str(value).upper().strip())


__all__ = [
    'BLOCKED_EXECUTION_STATUSES',
    'ExecutionLifecycleStatus',
    'FAILED_EXECUTION_STATUSES',
    'NON_TERMINAL_EXECUTION_STATUSES',
    'ReviewedTradeStatus',
    'SUCCESSFUL_EXECUTION_STATUSES',
    'TERMINAL_EXECUTION_STATUSES',
    'WorkflowActor',
    'normalize_execution_status',
    'normalize_reviewed_trade_status',
]
