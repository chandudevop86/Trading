from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from vinayak.db.models.live_analysis_job import LiveAnalysisJobRecord


STALE_RUNNING_JOB_TIMEOUT_SECONDS = 900


class LiveAnalysisJobRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def _dialect_name(self) -> str:
        bind = getattr(self.session, 'bind', None)
        dialect = getattr(bind, 'dialect', None)
        return str(getattr(dialect, 'name', '') or '').strip().lower()

    def create_job(
        self,
        *,
        job_id: str,
        dedup_key: str,
        symbol: str,
        interval: str,
        period: str,
        strategy: str,
        request_payload: dict[str, Any],
    ) -> LiveAnalysisJobRecord:
        record = LiveAnalysisJobRecord(
            id=job_id,
            dedup_key=dedup_key,
            symbol=symbol,
            interval=interval,
            period=period,
            strategy=strategy,
            request_payload=json.dumps(request_payload, default=str),
            status='PENDING',
            attempt_count=0,
            requested_at=datetime.now(UTC),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_job(self, job_id: str) -> LiveAnalysisJobRecord | None:
        return self.session.get(LiveAnalysisJobRecord, str(job_id or '').strip())

    def list_jobs(self, *, limit: int = 50, status: str | None = None) -> list[LiveAnalysisJobRecord]:
        query = self.session.query(LiveAnalysisJobRecord)
        if status:
            query = query.filter(LiveAnalysisJobRecord.status == str(status).strip().upper())
        return list(
            query.order_by(LiveAnalysisJobRecord.requested_at.desc(), LiveAnalysisJobRecord.id.desc())
            .limit(max(int(limit), 1))
            .all()
        )

    def find_active_job_by_key(self, dedup_key: str) -> LiveAnalysisJobRecord | None:
        return (
            self.session.query(LiveAnalysisJobRecord)
            .filter(
                LiveAnalysisJobRecord.dedup_key == dedup_key,
                LiveAnalysisJobRecord.status.in_(('PENDING', 'RUNNING')),
            )
            .order_by(LiveAnalysisJobRecord.requested_at.desc())
            .first()
        )

    def claim_next_pending_job(self) -> LiveAnalysisJobRecord | None:
        if self._dialect_name() == 'postgresql':
            return self._claim_next_pending_job_postgresql()
        return self._claim_next_pending_job_fallback()

    def requeue_stale_running_jobs(self, *, stale_after_seconds: int = STALE_RUNNING_JOB_TIMEOUT_SECONDS) -> int:
        cutoff = datetime.now(UTC) - timedelta(seconds=max(int(stale_after_seconds), 1))
        updated = self.session.execute(
            update(LiveAnalysisJobRecord)
            .where(
                LiveAnalysisJobRecord.status == 'RUNNING',
                LiveAnalysisJobRecord.started_at.is_not(None),
                LiveAnalysisJobRecord.started_at <= cutoff,
            )
            .values(
                status='PENDING',
                started_at=None,
                error='Recovered stale live-analysis job after worker timeout.',
            )
        )
        count = int(updated.rowcount or 0)
        if count:
            self.session.flush()
        return count

    def queue_metrics(self) -> dict[str, float]:
        now = datetime.now(UTC)
        pending_records = list(
            self.session.execute(
                select(LiveAnalysisJobRecord.requested_at)
                .where(LiveAnalysisJobRecord.status == 'PENDING')
            ).scalars()
        )
        running_records = list(
            self.session.execute(
                select(LiveAnalysisJobRecord.started_at)
                .where(LiveAnalysisJobRecord.status == 'RUNNING')
            ).scalars()
        )
        oldest_pending_age_seconds = 0.0
        if pending_records:
            oldest_pending = min(pending_records)
            oldest_pending_age_seconds = max(0.0, (now - oldest_pending).total_seconds())
        return {
            'pending_count': float(len(pending_records)),
            'running_count': float(sum(1 for value in running_records if value is not None)),
            'oldest_pending_age_seconds': round(oldest_pending_age_seconds, 2),
        }

    def _claim_next_pending_job_postgresql(self) -> LiveAnalysisJobRecord | None:
        record = (
            self.session.execute(
                select(LiveAnalysisJobRecord)
                .where(LiveAnalysisJobRecord.status == 'PENDING')
                .order_by(LiveAnalysisJobRecord.requested_at.asc(), LiveAnalysisJobRecord.id.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            ).scalars().first()
        )
        if record is None:
            return None
        record.status = 'RUNNING'
        record.started_at = datetime.now(UTC)
        record.attempt_count = int(record.attempt_count or 0) + 1
        record.error = None
        self.session.add(record)
        self.session.flush()
        return record

    def _claim_next_pending_job_fallback(self) -> LiveAnalysisJobRecord | None:
        pending_ids = list(
            self.session.execute(
                select(LiveAnalysisJobRecord.id)
                .where(LiveAnalysisJobRecord.status == 'PENDING')
                .order_by(LiveAnalysisJobRecord.requested_at.asc(), LiveAnalysisJobRecord.id.asc())
                .limit(10)
            ).scalars()
        )
        if not pending_ids:
            return None

        claimed_at = datetime.now(UTC)
        for record_id in pending_ids:
            updated = self.session.execute(
                update(LiveAnalysisJobRecord)
                .where(
                    LiveAnalysisJobRecord.id == record_id,
                    LiveAnalysisJobRecord.status == 'PENDING',
                )
                .values(
                    status='RUNNING',
                    started_at=claimed_at,
                    attempt_count=LiveAnalysisJobRecord.attempt_count + 1,
                    error=None,
                )
            )
            if int(updated.rowcount or 0) != 1:
                continue
            self.session.flush()
            return self.get_job(str(record_id))
        return None

    def mark_succeeded(self, record: LiveAnalysisJobRecord, result_payload: dict[str, Any]) -> LiveAnalysisJobRecord:
        record.status = 'SUCCEEDED'
        record.finished_at = datetime.now(UTC)
        record.error = None
        record.result_payload = json.dumps(result_payload, default=str)
        self.session.add(record)
        self.session.flush()
        return record

    def mark_failed(self, record: LiveAnalysisJobRecord, error: str) -> LiveAnalysisJobRecord:
        record.status = 'FAILED'
        record.finished_at = datetime.now(UTC)
        record.error = str(error or 'Live analysis job failed')
        record.result_payload = None
        self.session.add(record)
        self.session.flush()
        return record

    def retry_job(self, record: LiveAnalysisJobRecord) -> LiveAnalysisJobRecord:
        record.status = 'PENDING'
        record.started_at = None
        record.finished_at = None
        record.error = None
        record.result_payload = None
        self.session.add(record)
        self.session.flush()
        return record

    def cancel_job(self, record: LiveAnalysisJobRecord, *, reason: str = 'Cancelled by operator.') -> LiveAnalysisJobRecord:
        record.status = 'CANCELLED'
        record.finished_at = datetime.now(UTC)
        record.error = str(reason or 'Cancelled by operator.')
        self.session.add(record)
        self.session.flush()
        return record

    def parse_request_payload(self, record: LiveAnalysisJobRecord) -> dict[str, Any]:
        try:
            payload = json.loads(record.request_payload or '{}')
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def parse_result_payload(self, record: LiveAnalysisJobRecord) -> dict[str, Any] | None:
        if not record.result_payload:
            return None
        try:
            payload = json.loads(record.result_payload)
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None
