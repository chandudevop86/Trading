from __future__ import annotations

import os

from fastapi import APIRouter
from sqlalchemy import text

from vinayak.cache.redis_client import RedisCache
from vinayak.catalog.service import ProductCatalogService
from vinayak.core.config import get_settings
from vinayak.db.session import build_session_factory, get_database_provider, get_database_url
from vinayak.execution.broker.dhan_client import DhanClient
from vinayak.messaging.bus import build_message_bus


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


def _redis_check() -> dict[str, str]:
    cache = RedisCache.from_env()
    if not cache.is_configured():
        return {'status': 'disabled', 'engine': 'redis'}
    try:
        client = cache._get_client()
        if client is None:
            return {'status': 'disabled', 'engine': 'redis'}
        client.ping()
        return {'status': 'ok', 'engine': 'redis'}
    except Exception as exc:
        return {'status': 'error', 'engine': 'redis', 'detail': str(exc)}


@router.get('')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/live')
def health_live() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/ready')
def health_ready() -> dict[str, object]:
    settings = get_settings()
    database_status, database_url = _database_check()
    broker = _broker_check()
    redis_check = _redis_check()
    mongo_check = ProductCatalogService().readiness()
    bus_check = build_message_bus().readiness()
    ready = database_status == 'ok'
    return {
        'status': 'ready' if ready else 'degraded',
        'checks': {
            'database': {
                'status': database_status,
                'engine': get_database_provider(),
                'url': database_url,
            },
            'document_store': mongo_check,
            'cache': redis_check,
            'message_bus': bus_check,
            'broker': broker,
        },
        'config': {
            'environment': settings.env,
            'message_bus_backend': settings.message_bus.backend,
        },
    }
