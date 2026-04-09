from __future__ import annotations

from sqlalchemy.orm import Session

from vinayak.db.session import build_session_factory


def get_db() -> Session:
    session_factory = build_session_factory()
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
