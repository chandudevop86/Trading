from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from vinayak.db.models.outbox_event import OutboxEventRecord


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def enqueue_event(
        self,
        *,
        event_name: str,
        payload: dict[str, Any],
        source: str,
        available_at: datetime | None = None,
    ) -> OutboxEventRecord:
        record = OutboxEventRecord(
            event_name=event_name,
            payload=json.dumps(payload, default=str),
            source=source,
            status='PENDING',
            attempt_count=0,
            available_at=available_at or datetime.now(UTC),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_event(self, event_id: int) -> OutboxEventRecord | None:
        return self.session.get(OutboxEventRecord, event_id)

    def list_ready_events(self, *, limit: int = 50) -> list[OutboxEventRecord]:
        now = datetime.now(UTC)
        return list(
            self.session.query(OutboxEventRecord)
            .filter(
                OutboxEventRecord.available_at <= now,
                or_(OutboxEventRecord.status == 'PENDING', OutboxEventRecord.status == 'FAILED'),
            )
            .order_by(OutboxEventRecord.id.asc())
            .limit(limit)
            .all()
        )

    def mark_published(self, record: OutboxEventRecord) -> OutboxEventRecord:
        record.status = 'PUBLISHED'
        record.published_at = datetime.now(UTC)
        record.last_error = None
        self.session.add(record)
        self.session.flush()
        return record

    def mark_failed(self, record: OutboxEventRecord, error: str, *, retry_delay_seconds: int = 30) -> OutboxEventRecord:
        record.status = 'FAILED'
        record.attempt_count = int(record.attempt_count or 0) + 1
        record.last_error = error
        record.available_at = datetime.now(UTC) + timedelta(seconds=max(1, retry_delay_seconds))
        self.session.add(record)
        self.session.flush()
        return record

    def requeue_event(self, record: OutboxEventRecord) -> OutboxEventRecord:
        record.status = 'PENDING'
        record.available_at = datetime.now(UTC)
        record.last_error = None
        self.session.add(record)
        self.session.flush()
        return record

    def list_events(self, *, status: str | None = None) -> list[OutboxEventRecord]:
        query = self.session.query(OutboxEventRecord)
        if status:
            query = query.filter(OutboxEventRecord.status == status)
        return list(query.order_by(OutboxEventRecord.id.desc()).all())
