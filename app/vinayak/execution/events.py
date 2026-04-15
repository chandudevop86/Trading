from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from vinayak.messaging.events import (
    EVENT_TRADE_EXECUTED,
    EVENT_TRADE_EXECUTION_REJECTED,
    EVENT_TRADE_EXECUTE_REQUESTED,
    EVENT_TRADE_EXITED,
)


TRADE_EXECUTE_REQUESTED = 'TRADE_EXECUTE_REQUESTED'
TRADE_EXECUTION_REJECTED = 'TRADE_EXECUTION_REJECTED'
TRADE_EXECUTED = 'TRADE_EXECUTED'
TRADE_EXITED = 'TRADE_EXITED'

_DEFAULT_SOURCE = 'trade_execution_workflow'


@dataclass(slots=True, frozen=True)
class LifecycleEventPayload:
    reviewed_trade_id: int
    signal_id: int | None
    mode: str | None
    actor: str
    source: str
    occurred_at: datetime
    reason: str | None = None
    execution_id: int | None = None
    broker: str | None = None
    broker_reference: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            'reviewed_trade_id': self.reviewed_trade_id,
            'signal_id': self.signal_id,
            'execution_id': self.execution_id,
            'mode': self.mode,
            'broker': self.broker,
            'broker_reference': self.broker_reference,
            'actor': self.actor,
            'source': self.source,
            'reason': self.reason,
            'occurred_at': self.occurred_at.isoformat(),
            'metadata': dict(self.metadata),
        }


def _event_payload(
    *,
    reviewed_trade_id: int,
    signal_id: int | None,
    mode: str | None,
    actor: str,
    source: str,
    reason: str | None = None,
    execution_id: int | None = None,
    broker: str | None = None,
    broker_reference: str | None = None,
    metadata: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> LifecycleEventPayload:
    return LifecycleEventPayload(
        reviewed_trade_id=reviewed_trade_id,
        signal_id=signal_id,
        execution_id=execution_id,
        mode=mode,
        broker=broker,
        broker_reference=broker_reference,
        actor=actor,
        source=source,
        reason=reason,
        metadata=dict(metadata or {}),
        occurred_at=occurred_at or datetime.now(UTC),
    )


def build_trade_execute_requested_event(
    *,
    reviewed_trade_id: int,
    signal_id: int | None,
    mode: str,
    actor: str,
    source: str = _DEFAULT_SOURCE,
    reason: str | None = None,
    broker: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    payload = _event_payload(
        reviewed_trade_id=reviewed_trade_id,
        signal_id=signal_id,
        mode=mode,
        actor=actor,
        source=source,
        reason=reason,
        broker=broker,
        metadata=metadata,
    )
    return EVENT_TRADE_EXECUTE_REQUESTED, payload.to_payload()


def build_trade_execution_rejected_event(
    *,
    reviewed_trade_id: int,
    signal_id: int | None,
    mode: str,
    actor: str,
    source: str = _DEFAULT_SOURCE,
    reason: str | None = None,
    execution_id: int | None = None,
    broker: str | None = None,
    broker_reference: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    payload = _event_payload(
        reviewed_trade_id=reviewed_trade_id,
        signal_id=signal_id,
        execution_id=execution_id,
        mode=mode,
        actor=actor,
        source=source,
        reason=reason,
        broker=broker,
        broker_reference=broker_reference,
        metadata=metadata,
    )
    return EVENT_TRADE_EXECUTION_REJECTED, payload.to_payload()


def build_trade_executed_event(
    *,
    reviewed_trade_id: int,
    signal_id: int | None,
    mode: str,
    actor: str,
    source: str = _DEFAULT_SOURCE,
    reason: str | None = None,
    execution_id: int | None = None,
    broker: str | None = None,
    broker_reference: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    payload = _event_payload(
        reviewed_trade_id=reviewed_trade_id,
        signal_id=signal_id,
        execution_id=execution_id,
        mode=mode,
        actor=actor,
        source=source,
        reason=reason,
        broker=broker,
        broker_reference=broker_reference,
        metadata=metadata,
    )
    return EVENT_TRADE_EXECUTED, payload.to_payload()


def build_trade_exited_event(
    *,
    reviewed_trade_id: int,
    signal_id: int | None,
    mode: str | None,
    actor: str,
    source: str = _DEFAULT_SOURCE,
    reason: str | None = None,
    execution_id: int | None = None,
    broker: str | None = None,
    broker_reference: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    payload = _event_payload(
        reviewed_trade_id=reviewed_trade_id,
        signal_id=signal_id,
        execution_id=execution_id,
        mode=mode,
        actor=actor,
        source=source,
        reason=reason,
        broker=broker,
        broker_reference=broker_reference,
        metadata=metadata,
    )
    return EVENT_TRADE_EXITED, payload.to_payload()


__all__ = [
    'LifecycleEventPayload',
    'TRADE_EXECUTED',
    'TRADE_EXECUTION_REJECTED',
    'TRADE_EXECUTE_REQUESTED',
    'TRADE_EXITED',
    'build_trade_executed_event',
    'build_trade_execute_requested_event',
    'build_trade_execution_rejected_event',
    'build_trade_exited_event',
]
