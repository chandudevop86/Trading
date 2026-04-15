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
        self.queue_metrics_calls = 0

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
        self.queue_metrics_calls += 1
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


class FakeDeferredExecutionJobRecord:
    def __init__(self, job_id: str) -> None:
        self.id = job_id
        self.outbox_event_id = None


class FakeDeferredExecutionJobRepository:
    def __init__(self, session) -> None:
        self.session = session
        self.created_jobs: list[FakeDeferredExecutionJobRecord] = []

    def create_job(self, **kwargs):
        record = FakeDeferredExecutionJobRecord(kwargs['job_id'])
        self.created_jobs.append(record)
        return record

    def attach_outbox_event(self, record, *, outbox_event_id: int):
        record.outbox_event_id = outbox_event_id
        return record


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
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        jobs_module,
        'run_live_trading_analysis',
        lambda **kwargs: captured.update(kwargs) or {
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
    assert captured['force_market_refresh'] is False
    assert captured['persist_reports'] is False
    assert captured['publish_completion_event'] is False
    assert captured['deliver_telegram_inline'] is False
    assert captured['publish_alert_notifications'] is False
    assert captured['execute_inline'] is False


def test_live_analysis_job_service_can_force_market_refresh(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    captured: dict[str, object] = {}

    class ForceRefreshRepository(FakeRepository):
        def parse_request_payload(self, record: FakeRecord):
            payload = super().parse_request_payload(record)
            payload['force_market_refresh'] = True
            return payload

    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', ForceRefreshRepository)
    monkeypatch.setattr(
        jobs_module,
        'run_live_trading_analysis',
        lambda **kwargs: captured.update(kwargs) or {
            'generated_at': '2026-04-15T10:00:00Z',
            'signal_count': 1,
            'candle_count': 1,
        },
    )

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    processed = service.process_next_pending_job()

    assert processed is True
    assert captured['force_market_refresh'] is True


def test_live_analysis_job_service_enqueues_deferred_execution_event(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    captured: dict[str, object] = {}
    queued_events: list[tuple[str, dict[str, object], str]] = []

    class DeferredExecutionRepository(FakeRepository):
        def parse_request_payload(self, record: FakeRecord):
            payload = super().parse_request_payload(record)
            payload.update({
                'auto_execute': True,
                'execution_type': 'PAPER',
                'paper_log_path': 'paper.csv',
                'live_log_path': 'live.csv',
            })
            return payload

    class FakeOutboxService:
        def __init__(self, session) -> None:
            self.session = session

        def enqueue(self, *, event_name: str, payload: dict[str, object], source: str):
            queued_events.append((event_name, payload, source))
            return type('OutboxEvent', (), {'id': 41})()

    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', DeferredExecutionRepository)
    monkeypatch.setattr(jobs_module, 'DeferredExecutionJobRepository', FakeDeferredExecutionJobRepository)
    monkeypatch.setattr(jobs_module, 'OutboxService', FakeOutboxService)
    monkeypatch.setattr(
        jobs_module,
        'run_live_trading_analysis',
        lambda **kwargs: captured.update(kwargs) or {
            'generated_at': '2026-04-15T10:00:00Z',
            'signal_count': 1,
            'candle_count': 2,
            'symbol': '^NSEI',
            'strategy': 'Breakout',
            'signals': [{'symbol': '^NSEI', 'side': 'BUY', 'entry_price': 101.0}],
            'candles': [{'timestamp': '2026-04-15 09:15:00', 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000}],
            'execution_summary': {'mode': 'PAPER'},
        },
    )

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    processed = service.process_next_pending_job()

    assert processed is True
    assert captured['execute_inline'] is False
    assert len(queued_events) == 1
    event_name, payload, source = queued_events[0]
    assert event_name == jobs_module.EVENT_DEFERRED_EXECUTION_REQUESTED
    assert payload['deferred_execution_job_id']
    assert payload['execution_mode'] == 'PAPER'
    assert payload['paper_log_path'] == 'paper.csv'
    assert source == 'live_analysis_jobs'


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


def test_live_analysis_job_service_throttles_non_forced_queue_metric_refresh(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    created_repositories: list[FakeRepository] = []

    class TrackingRepository(FakeRepository):
        def __init__(self, session: FakeSession) -> None:
            super().__init__(session)
            created_repositories.append(self)

    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', TrackingRepository)
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
    service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))

    first_repo = created_repositories[-1]
    assert first_repo.queue_metrics_calls == 1

    service.process_next_pending_job()
    second_repo = created_repositories[-1]
    assert second_repo.queue_metrics_calls == 1


def test_live_analysis_job_service_forces_queue_metric_refresh_for_operator_actions(monkeypatch) -> None:
    store: dict[str, FakeRecord] = {}
    created_repositories: list[FakeRepository] = []

    class TrackingRepository(FakeRepository):
        def __init__(self, session: FakeSession) -> None:
            super().__init__(session)
            created_repositories.append(self)

    monkeypatch.setattr(jobs_module, 'LiveAnalysisJobRepository', TrackingRepository)

    service = LiveAnalysisJobService(session_factory=lambda: FakeSession(store))
    first = service.submit(LiveAnalysisRequest(symbol='^NSEI', interval='5m', period='1d', strategy='Breakout'))
    record = store[first['job_id']]
    record.status = 'FAILED'
    record.error = 'boom'

    service.retry_job(first['job_id'])
    retried_repo = created_repositories[-1]
    assert retried_repo.queue_metrics_calls == 1

    record.status = 'RUNNING'
    service.cancel_job(first['job_id'])
    cancelled_repo = created_repositories[-1]
    assert cancelled_repo.queue_metrics_calls == 1
