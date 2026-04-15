from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from vinayak.api.schemas.strategy import LiveAnalysisRequest
from vinayak.api.services import live_analysis_jobs as jobs_module
from vinayak.api.services.live_analysis_jobs import LiveAnalysisJobService


@dataclass
class FakeRecord:
    id: str
    dedup_key: str
    symbol: str
    interval: str
    period: str
    strategy: str
    request_payload: str
    status: str
    attempt_count: int
    error: str | None
    result_payload: str | None
    requested_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class FakeSession:
    def __init__(self, store: dict[str, FakeRecord]) -> None:
        self.store = store
        self.commits = 0
        self.requeue_calls = 0

    def commit(self) -> None:
        self.commits += 1

    def refresh(self, record: FakeRecord) -> None:
        return None

    def close(self) -> None:
        return None


class FakeRepository:
    def __init__(self, session: FakeSession) -> None:
        self.session = session
        self.requeued = 0

    def create_job(self, **kwargs):
        record = FakeRecord(
            id=kwargs['job_id'],
            dedup_key=kwargs['dedup_key'],
            symbol=kwargs['symbol'],
            interval=kwargs['interval'],
            period=kwargs['period'],
            strategy=kwargs['strategy'],
            request_payload='{"symbol":"^NSEI","interval":"5m","period":"1d","strategy":"Breakout"}',
            status='PENDING',
            attempt_count=0,
            error=None,
            result_payload=None,
            requested_at=datetime.now(UTC),
        )
        self.session.store[record.id] = record
        return record

    def get_job(self, job_id: str):
        return self.session.store.get(job_id)

    def find_active_job_by_key(self, dedup_key: str):
        for record in self.session.store.values():
            if record.dedup_key == dedup_key and record.status in {'PENDING', 'RUNNING'}:
                return record
        return None

    def claim_next_pending_job(self):
        for record in self.session.store.values():
            if record.status == 'PENDING':
                record.status = 'RUNNING'
                record.started_at = datetime.now(UTC)
                record.attempt_count += 1
                return record
        return None

    def requeue_stale_running_jobs(self, *, stale_after_seconds: int = 900):
        self.requeued += 1
        self.session.requeue_calls += 1
        return 0

    def queue_metrics(self):
        pending = sum(1 for record in self.session.store.values() if record.status == 'PENDING')
        running = sum(1 for record in self.session.store.values() if record.status == 'RUNNING')
        return {
            'pending_count': float(pending),
            'running_count': float(running),
            'oldest_pending_age_seconds': 0.0,
        }

    def mark_succeeded(self, record: FakeRecord, result_payload):
        record.status = 'SUCCEEDED'
        record.finished_at = datetime.now(UTC)
        record.result_payload = '{"generated_at":"2026-04-15T10:00:00Z","signal_count":2,"candle_count":3}'
        record.error = None
        return record

    def mark_failed(self, record: FakeRecord, error: str):
        record.status = 'FAILED'
        record.finished_at = datetime.now(UTC)
        record.error = error
        record.result_payload = None
        return record

    def parse_request_payload(self, record: FakeRecord):
        return {
            'symbol': '^NSEI',
            'interval': '5m',
            'period': '1d',
            'strategy': 'Breakout',
            'capital': 100000,
            'risk_pct': 1,
            'rr_ratio': 2,
        }

    def parse_result_payload(self, record: FakeRecord):
        if record.result_payload is None:
            return None
        return {
            'generated_at': '2026-04-15T10:00:00Z',
            'signal_count': 2,
            'candle_count': 3,
        }


def test_live_analysis_job_service_enqueues_pending_job(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', FakeRepository)

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    accepted = service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    assert accepted['status'] == 'PENDING'
    assert accepted['deduplicated'] is False
    assert accepted['job_id'] in store


def test_live_analysis_job_service_deduplicates_active_job(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', FakeRepository)

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    request = LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout')
    first = service.submit(request)
    second = service.submit(request)

    assert first['job_id'] == second['job_id']
    assert second['status'] == 'PENDING'
    assert second['deduplicated'] is True


def test_live_analysis_job_service_processes_next_pending_job(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', FakeRepository)
    monkeypatch.setattr(
        jobs_module,
        'run_live_trading_analysis',
        lambda **kwargs: {
            'generated_at': '2026-04-15T10:00:00Z',
            'signal_count': 2,
            'candle_count': 3,
        },
    )

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    accepted = service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    processed = service.process_next_pending_job()
    stored = service.get(accepted['job_id'])

    assert processed is True
    assert stored is not None
    assert stored['status'] == 'SUCCEEDED'
    assert stored['signal_count'] == 2


def test_live_analysis_job_service_requeues_stale_jobs_before_claim(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', FakeRepository)
    monkeypatch.setattr(
        jobs_module,
        'run_live_trading_analysis',
        lambda **kwargs: {
            'generated_at': '2026-04-15T10:00:00Z',
            'signal_count': 1,
            'candle_count': 1,
        },
    )

    session = FakeSession(store)
    service = LiveAnalysisJobService(session_factory=lambda: session)
    accepted = service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    processed = service.process_next_pending_job()
    stored = service.get(accepted['job_id'])

    assert processed is True
    assert stored is not None
    assert stored['status'] == 'SUCCEEDED'
    assert session.requeue_calls >= 1


def test_live_analysis_job_service_lists_and_allows_retry_cancel(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', FakeRepository)

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    first = service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))
    record = store[first['job_id']]
    record.status = 'FAILED'
    record.error = 'boom'

    listed = service.list_jobs(limit=10, status='FAILED')
    retried = service.retry_job(first['job_id'])
    record.status = 'RUNNING'
    cancelled = service.cancel_job(first['job_id'])

    assert len(listed) == 1
    assert listed[0]['status'] == 'FAILED'
    assert retried['status'] == 'PENDING'
    assert cancelled['status'] == 'CANCELLED'
