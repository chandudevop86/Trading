from __future__ import annotations

import time

from vinayak.api.services.live_analysis_jobs import process_next_live_analysis_job
from vinayak.core.config import should_auto_initialize_database
from vinayak.db.session import initialize_database


POLL_INTERVAL_SECONDS = 2


def main() -> None:
    if should_auto_initialize_database():
        initialize_database()
    while True:
        processed = process_next_live_analysis_job()
        if not processed:
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
