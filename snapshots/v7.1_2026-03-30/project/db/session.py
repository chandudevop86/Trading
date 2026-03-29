from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from vinayak.core.config import get_settings


class Base(DeclarativeBase):
    pass


def get_database_url() -> str:
    return get_settings().sql.url


def get_database_provider() -> str:
    return get_settings().sql.provider


@lru_cache(maxsize=8)
def get_engine(database_url: str | None = None) -> Engine:
    url = database_url or get_database_url()
    connect_args = {'check_same_thread': False} if url.startswith('sqlite') else {}
    return create_engine(url, future=True, connect_args=connect_args, pool_pre_ping=not url.startswith('sqlite'))


@lru_cache(maxsize=8)
def build_session_factory(database_url: str | None = None) -> sessionmaker:
    engine = get_engine(database_url)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def reset_database_state(database_url: str | None = None) -> None:
    try:
        engine = get_engine(database_url)
        engine.dispose()
    except Exception:
        pass
    build_session_factory.cache_clear()
    get_engine.cache_clear()


def initialize_database(database_url: str | None = None) -> None:
    from vinayak.db.models.execution import ExecutionRecord  # noqa: F401
    from vinayak.db.models.execution_audit_log import ExecutionAuditLogRecord  # noqa: F401
    from vinayak.db.models.outbox_event import OutboxEventRecord  # noqa: F401
    from vinayak.db.models.reviewed_trade import ReviewedTradeRecord  # noqa: F401
    from vinayak.db.models.signal import SignalRecord  # noqa: F401

    Base.metadata.create_all(bind=get_engine(database_url))
