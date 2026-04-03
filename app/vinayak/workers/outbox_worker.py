from __future__ import annotations

import time

from vinayak.db.session import build_session_factory, initialize_database
from vinayak.messaging.outbox import dispatch_pending_outbox_events


POLL_INTERVAL_SECONDS = 5


def main() -> None:
    initialize_database()
    session_factory = build_session_factory()
    while True:
        session = session_factory()
        try:
            dispatch_pending_outbox_events(session)
        finally:
            session.close()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
