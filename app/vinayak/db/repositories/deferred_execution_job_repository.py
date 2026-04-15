from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.deferred_execution_job import DeferredExecutionJobRecord


class DeferredExecutionJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_job(
        self,
        *,
        job_id: str,
        source_job_id: str | None,
        symbol: str,
        strategy: str,
        execution_mode: str,
        signal_count: int,
        request_payload: dict[str, Any],
    ) -> DeferredExecutionJobRecord:
        record = DeferredExecutionJobRecord(
            id=job_id,
            source_job_id=source_job_id,
            symbol=symbol,
            strategy=strategy,
            execution_mode=execution_mode,
            request_payload=json.dumps(request_payload, default=str),
            status='PENDING',
            attempt_count=0,
            signal_count=max(int(signal_count), 0),
            requested_at=datetime.now(UTC),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_job(self, job_id: str) -> DeferredExecutionJobRecord | None:
        return self.session.get(DeferredExecutionJobRecord, str(job_id or '').strip())

    def list_jobs(self, *, limit: int = 50, status: str | None = None) -> list[DeferredExecutionJobRecord]:
        query = self.session.query(DeferredExecutionJobRecord)
        if status:
            query = query.filter(DeferredExecutionJobRecord.status == str(status).strip().upper())
        return list(
            query.order_by(DeferredExecutionJobRecord.requested_at.desc(), DeferredExecutionJobRecord.id.desc())
            .limit(max(int(limit), 1))
            .all()
        )

    def attach_outbox_event(self, record: DeferredExecutionJobRecord, *, outbox_event_id: int) -> DeferredExecutionJobRecord:
        record.outbox_event_id = int(outbox_event_id)
        self.session.add(record)
        self.session.flush()
        return record

    def mark_running(self, record: DeferredExecutionJobRecord) -> DeferredExecutionJobRecord:
        record.status = 'RUNNING'
        record.started_at = datetime.now(UTC)
        record.attempt_count = int(record.attempt_count or 0) + 1
        record.error = None
        self.session.add(record)
        self.session.flush()
        return record

    def mark_succeeded(self, record: DeferredExecutionJobRecord, result_payload: dict[str, Any]) -> DeferredExecutionJobRecord:
        record.status = 'SUCCEEDED'
        record.finished_at = datetime.now(UTC)
        record.error = None
        record.result_payload = json.dumps(result_payload, default=str)
        self.session.add(record)
        self.session.flush()
        return record

    def mark_failed(self, record: DeferredExecutionJobRecord, error: str) -> DeferredExecutionJobRecord:
        record.status = 'FAILED'
        record.finished_at = datetime.now(UTC)
        record.error = str(error or 'Deferred execution job failed')
        self.session.add(record)
        self.session.flush()
        return record

    def retry_job(self, record: DeferredExecutionJobRecord) -> DeferredExecutionJobRecord:
        record.status = 'PENDING'
        record.started_at = None
        record.finished_at = None
        record.error = None
        self.session.add(record)
        self.session.flush()
        return record

    def parse_request_payload(self, record: DeferredExecutionJobRecord) -> dict[str, Any]:
        try:
            payload = json.loads(record.request_payload or '{}')
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
