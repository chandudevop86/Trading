from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.outbox_event import OutboxEventRecord
from vinayak.db.repositories.outbox_repository import OutboxRepository
from vinayak.messaging.bus import build_message_bus


@dataclass(slots=True)
class OutboxDispatchResult:
    published_count: int = 0
    failed_count: int = 0


class OutboxService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = OutboxRepository(session)

    def enqueue(self, *, event_name: str, payload: dict[str, Any], source: str) -> None:
        self.repository.enqueue_event(event_name=event_name, payload=payload, source=source)

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
        return updated


def dispatch_pending_outbox_events(session: Session, *, limit: int = 50) -> OutboxDispatchResult:
    repository = OutboxRepository(session)
    bus = build_message_bus()
    result = OutboxDispatchResult()
    for record in repository.list_ready_events(limit=limit):
        try:
            payload = json.loads(record.payload)
            published = bus.publish(record.event_name, payload, source=record.source)
            if not published:
                raise RuntimeError('Message bus publish returned false')
            repository.mark_published(record)
            result.published_count += 1
        except Exception as exc:
            repository.mark_failed(record, str(exc))
            result.failed_count += 1
    session.commit()
    return result
