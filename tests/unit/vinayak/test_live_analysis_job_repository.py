from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from vinayak.db.repositories.live_analysis_job_repository import LiveAnalysisJobRepository


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return iter(self._values)


class _SingleScalarResult:
    def __init__(self, value):
        self._value = value

    def scalars(self):
        return self

    def first(self):
        return self._value


class _ExecuteResult:
    def __init__(self, rowcount: int):
        self.rowcount = rowcount


class _FakeSession:
    def __init__(self) -> None:
        self.calls = 0
        self.flushed = False

    def execute(self, statement):
        self.calls += 1
        if self.calls == 1:
            return _ScalarResult(['job-a', 'job-b'])
        if self.calls == 2:
            return _ExecuteResult(0)
        if self.calls == 3:
            return _ExecuteResult(1)
        raise AssertionError(f'unexpected execute call #{self.calls}')

    def flush(self) -> None:
        self.flushed = True


class _RecoverySession:
    def __init__(self) -> None:
        self.rowcounts = [2]
        self.flushed = False

    def execute(self, statement):
        return _ExecuteResult(self.rowcounts.pop(0))

    def flush(self) -> None:
        self.flushed = True


@dataclass
class _Dialect:
    name: str


@dataclass
class _Bind:
    dialect: _Dialect


@dataclass
class _Record:
    id: str
    status: str
    attempt_count: int
    requested_at: datetime
    started_at: datetime | None = None
    error: str | None = None


class _FakePostgresSession:
    def __init__(self, record: _Record | None) -> None:
        self.bind = _Bind(dialect=_Dialect(name='postgresql'))
        self.record = record
        self.flushed = False
        self.added = None

    def execute(self, statement):
        return _SingleScalarResult(self.record)

    def add(self, record) -> None:
        self.added = record

    def flush(self) -> None:
        self.flushed = True


def test_claim_next_pending_job_skips_already_claimed_rows(monkeypatch) -> None:
    session = _FakeSession()
    repository = LiveAnalysisJobRepository(session)  # type: ignore[arg-type]

    monkeypatch.setattr(repository, 'get_job', lambda job_id: {'id': job_id})

    claimed = repository.claim_next_pending_job()

    assert claimed == {'id': 'job-b'}
    assert session.flushed is True


def test_claim_next_pending_job_uses_skip_locked_path_for_postgres() -> None:
    record = _Record(
        id='job-pg',
        status='PENDING',
        attempt_count=0,
        requested_at=datetime.now(UTC),
    )
    session = _FakePostgresSession(record)
    repository = LiveAnalysisJobRepository(session)  # type: ignore[arg-type]

    claimed = repository.claim_next_pending_job()

    assert claimed is record
    assert record.status == 'RUNNING'
    assert record.attempt_count == 1
    assert record.started_at is not None
    assert record.error is None
    assert session.added is record
    assert session.flushed is True


def test_requeue_stale_running_jobs_marks_jobs_pending_again() -> None:
    session = _RecoverySession()
    repository = LiveAnalysisJobRepository(session)  # type: ignore[arg-type]

    recovered = repository.requeue_stale_running_jobs(stale_after_seconds=60)

    assert recovered == 2
    assert session.flushed is True
