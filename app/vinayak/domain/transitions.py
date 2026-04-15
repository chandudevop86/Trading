from __future__ import annotations

from vinayak.domain.exceptions import InvalidStatusTransitionError
from vinayak.domain.statuses import ReviewedTradeStatus, normalize_reviewed_trade_status


REVIEWED_TRADE_ALLOWED_TRANSITIONS: dict[ReviewedTradeStatus, frozenset[ReviewedTradeStatus]] = {
    ReviewedTradeStatus.REVIEWED: frozenset(
        {
            ReviewedTradeStatus.APPROVED,
            ReviewedTradeStatus.REJECTED,
            ReviewedTradeStatus.FAILED,
        }
    ),
    ReviewedTradeStatus.APPROVED: frozenset(
        {
            ReviewedTradeStatus.EXECUTE_REQUESTED,
            ReviewedTradeStatus.REJECTED,
            ReviewedTradeStatus.FAILED,
        }
    ),
    ReviewedTradeStatus.REJECTED: frozenset(),
    ReviewedTradeStatus.EXECUTE_REQUESTED: frozenset(
        {
            ReviewedTradeStatus.EXECUTION_REJECTED,
            ReviewedTradeStatus.EXECUTED,
            ReviewedTradeStatus.FAILED,
        }
    ),
    ReviewedTradeStatus.EXECUTION_REJECTED: frozenset(),
    ReviewedTradeStatus.EXECUTED: frozenset({ReviewedTradeStatus.EXITED}),
    ReviewedTradeStatus.EXITED: frozenset(),
    ReviewedTradeStatus.FAILED: frozenset(),
}


def can_transition_reviewed_trade(
    current_status: str | ReviewedTradeStatus,
    new_status: str | ReviewedTradeStatus,
) -> bool:
    current = normalize_reviewed_trade_status(current_status)
    target = normalize_reviewed_trade_status(new_status)
    if current == target:
        return True
    return target in REVIEWED_TRADE_ALLOWED_TRANSITIONS[current]


def validate_reviewed_trade_transition(
    current_status: str | ReviewedTradeStatus,
    new_status: str | ReviewedTradeStatus,
) -> None:
    current = normalize_reviewed_trade_status(current_status)
    target = normalize_reviewed_trade_status(new_status)
    if can_transition_reviewed_trade(current, target):
        return
    raise InvalidStatusTransitionError(
        f'Invalid reviewed trade transition from {current.value} to {target.value}.'
    )


__all__ = [
    'REVIEWED_TRADE_ALLOWED_TRANSITIONS',
    'can_transition_reviewed_trade',
    'validate_reviewed_trade_transition',
]
