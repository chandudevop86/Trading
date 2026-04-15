from __future__ import annotations

from vinayak.workers import live_analysis_worker as worker_module


def test_drain_available_jobs_stops_when_queue_is_empty(monkeypatch) -> None:
    calls = {'count': 0}

    def _fake_process() -> bool:
        calls['count'] += 1
        return calls['count'] <= 2

    monkeypatch.setattr(worker_module, 'process_next_live_analysis_job', _fake_process)

    processed = worker_module.drain_available_jobs(max_jobs=5)

    assert processed == 2
    assert calls['count'] == 3


def test_drain_available_jobs_honors_batch_cap(monkeypatch) -> None:
    calls = {'count': 0}

    def _fake_process() -> bool:
        calls['count'] += 1
        return True

    monkeypatch.setattr(worker_module, 'process_next_live_analysis_job', _fake_process)

    processed = worker_module.drain_available_jobs(max_jobs=3)

    assert processed == 3
    assert calls['count'] == 3


def test_next_idle_sleep_resets_after_work() -> None:
    assert worker_module.next_idle_sleep(4.0, processed_jobs=1) == worker_module.MIN_IDLE_SLEEP_SECONDS


def test_next_idle_sleep_backs_off_when_idle() -> None:
    first = worker_module.next_idle_sleep(0.0, processed_jobs=0)
    second = worker_module.next_idle_sleep(first, processed_jobs=0)
    capped = worker_module.next_idle_sleep(worker_module.MAX_IDLE_SLEEP_SECONDS, processed_jobs=0)

    assert first == worker_module.MIN_IDLE_SLEEP_SECONDS
    assert second == min(worker_module.MIN_IDLE_SLEEP_SECONDS * 2.0, worker_module.MAX_IDLE_SLEEP_SECONDS)
    assert capped == worker_module.MAX_IDLE_SLEEP_SECONDS
