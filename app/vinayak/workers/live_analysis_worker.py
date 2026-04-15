from __future__ import annotations

import time

from vinayak.api.services.live_analysis_jobs import process_next_live_analysis_job
from vinayak.core.config import should_auto_initialize_database
from vinayak.db.session import initialize_database


MIN_IDLE_SLEEP_SECONDS = 0.5
MAX_IDLE_SLEEP_SECONDS = 8.0
MAX_JOBS_PER_CYCLE = 5


def drain_available_jobs(*, max_jobs: int = MAX_JOBS_PER_CYCLE) -> int:
    processed_jobs = 0
    for _ in range(max(1, int(max_jobs))):
        if not process_next_live_analysis_job():
            break
        processed_jobs += 1
    return processed_jobs


def next_idle_sleep(previous_sleep_seconds: float, *, processed_jobs: int) -> float:
    if processed_jobs > 0:
        return MIN_IDLE_SLEEP_SECONDS
    if previous_sleep_seconds <= 0:
        return MIN_IDLE_SLEEP_SECONDS
    return min(previous_sleep_seconds * 2.0, MAX_IDLE_SLEEP_SECONDS)


def main() -> None:
    if should_auto_initialize_database():
        initialize_database()
    idle_sleep_seconds = MIN_IDLE_SLEEP_SECONDS
    while True:
        processed_jobs = drain_available_jobs()
        idle_sleep_seconds = next_idle_sleep(idle_sleep_seconds, processed_jobs=processed_jobs)
        if processed_jobs == 0:
            time.sleep(idle_sleep_seconds)


if __name__ == '__main__':
    main()
