from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.outbox_event import OutboxEventRecord
from vinayak.db.repositories.outbox_repository import OutboxRepository
from vinayak.messaging.bus import build_message_bus
from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric


@dataclass(slots=True)
class OutboxDispatchResult:
    published_count: int = 0
    failed_count: int = 0


class OutboxService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = OutboxRepository(session)

    def enqueue(self, *, event_name: str, payload: dict[str, Any], source: str) -> OutboxEventRecord:
        record = self.repository.enqueue_event(event_name=event_name, payload=payload, source=source)
        increment_metric('outbox_enqueue_total', 1)
        log_event(
            component='outbox',
            event_name='outbox_enqueue',
            severity='INFO',
            message='Outbox event queued',
            context_json={'event_name': event_name, 'source': source},
        )
        return record

    def list_events(self, *, status: str | None = None) -> list[OutboxEventRecord]:
        return self.repository.list_events(status=status)

    def get_event(self, event_id: int) -> OutboxEventRecord | None:
        return self.repository.get_event(event_id)

    def retry_event(self, event_id: int) -> OutboxEventRecord:
        record = self.repository.get_event(event_id)
        if record is None:
            raise ValueError(f'Outbox event {event_id} was not found.')
        if record.status == 'PUBLISHED':
            raise ValueError(f'Outbox event {event_id} is already published.')
        updated = self.repository.requeue_event(record)
        self.session.commit()
        self.session.refresh(updated)
        increment_metric('outbox_retry_total', 1)
        log_event(
            component='outbox',
            event_name='outbox_retry_queued',
            severity='INFO',
            message='Outbox event queued for retry',
            context_json={'event_id': updated.id, 'event_name': updated.event_name, 'attempt_count': updated.attempt_count},
        )
        return updated


def dispatch_pending_outbox_events(session: Session, *, limit: int = 50) -> OutboxDispatchResult:
    import time

    repository = OutboxRepository(session)
    bus = build_message_bus()
    result = OutboxDispatchResult()
    started = time.perf_counter()
    ready_events = repository.list_ready_events(limit=limit)
    set_metric('outbox_ready_events', len(ready_events))

    for record in ready_events:
        increment_metric('outbox_dispatch_attempt_total', 1)
        try:
            payload = json.loads(record.payload)
            published = bus.publish(record.event_name, payload, source=record.source)
            if not published:
                raise RuntimeError('Message bus publish returned false')
            repository.mark_published(record)
            result.published_count += 1
            increment_metric('outbox_dispatch_published_total', 1)
            log_event(
                component='outbox',
                event_name='outbox_dispatch_published',
                severity='INFO',
                message='Outbox event published',
                context_json={'event_id': record.id, 'event_name': record.event_name, 'source': record.source, 'attempt_count': record.attempt_count},
            )
        except Exception as exc:
            repository.mark_failed(record, str(exc))
            result.failed_count += 1
            increment_metric('outbox_dispatch_failed_total', 1)
            log_exception(
                component='outbox',
                event_name='outbox_dispatch_failed',
                exc=exc,
                message='Outbox event dispatch failed',
                context_json={'event_id': record.id, 'event_name': record.event_name, 'source': record.source, 'attempt_count': record.attempt_count},
            )

    session.commit()
    duration = round(time.perf_counter() - started, 4)
    set_metric('outbox_last_dispatch_duration_seconds', duration)
    record_stage(
        'notify',
        status='SUCCESS' if result.failed_count == 0 else 'WARN',
        duration_seconds=duration,
        message='Outbox dispatch cycle completed',
        trace_id='outbox_dispatch',
    )
    return result
