from __future__ import annotations

import os

from fastapi import APIRouter
from sqlalchemy import text

from vinayak.db.session import build_session_factory, get_database_url
from vinayak.execution.broker.dhan_client import DhanClient


router = APIRouter(prefix='/health', tags=['health'])


def _database_check() -> tuple[str, str]:
    database_url = get_database_url()
    session_factory = build_session_factory(database_url)
    session = session_factory()
    try:
        session.execute(text('SELECT 1'))
        return 'ok', database_url
    except Exception:
        return 'error', database_url
    finally:
        session.close()


def _broker_check() -> dict[str, str]:
    client = DhanClient(
        os.getenv('DHAN_CLIENT_ID'),
        os.getenv('DHAN_ACCESS_TOKEN'),
    )
    return {
        'status': 'ready' if client.is_ready() else 'missing_credentials',
        'broker': 'dhan',
    }


@router.get('')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/live')
def health_live() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/ready')
def health_ready() -> dict[str, object]:
    database_status, database_url = _database_check()
    broker = _broker_check()
    ready = database_status == 'ok'
    return {
        'status': 'ready' if ready else 'degraded',
        'checks': {
            'database': {
                'status': database_status,
                'engine': 'sqlite' if database_url.startswith('sqlite') else 'postgresql',
            },
            'broker': broker,
        },
    }
