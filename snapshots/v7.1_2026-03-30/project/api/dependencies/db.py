from __future__ import annotations

from sqlalchemy.orm import Session

from vinayak.db.session import build_session_factory, initialize_database


def get_db() -> Session:
    initialize_database()
    session_factory = build_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
