from __future__ import annotations

import os

from fastapi import APIRouter
from sqlalchemy import text

from vinayak.auth.service import UserAuthService
from vinayak.cache.redis_client import RedisCache
from vinayak.catalog.service import ProductCatalogService
from vinayak.core.config import get_settings
from vinayak.db.session import build_session_factory, get_database_provider, get_database_url
from vinayak.execution.broker.dhan_client import DhanClient
from vinayak.messaging.bus import build_message_bus


_REDIS_CACHE = RedisCache.from_env()

router = APIRouter(prefix='/health', tags=['health'])


def _admin_auth_check() -> dict[str, str]:
    try:
        UserAuthService.admin_username()
        UserAuthService.admin_password()
        UserAuthService.auth_secret()
        return {'status': 'ok'}
    except RuntimeError:
        return {'status': 'error'}


def _sanitize_database_url(database_url: str) -> str:
    raw = str(database_url or '').strip()
    if not raw:
        return '<redacted>'
    if '://' not in raw:
        return '<redacted>'
    scheme, remainder = raw.split('://', 1)
    if '@' not in remainder:
        return raw
    _, location = remainder.rsplit('@', 1)
    return f'{scheme}://{location}'


def _database_check() -> tuple[str, str]:
    database_url = get_database_url()
    session_factory = build_session_factory(database_url)
    session = session_factory()
    try:
        session.execute(text('SELECT 1'))
        return 'ok', _sanitize_database_url(database_url)
    except Exception:
        return 'error', _sanitize_database_url(database_url)
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
    if not _REDIS_CACHE.is_configured():
        return {'status': 'disabled', 'engine': 'redis'}
    try:
        client = _REDIS_CACHE._get_client()
        if client is None:
            return {'status': 'disabled', 'engine': 'redis'}
        client.ping()
        return {'status': 'ok', 'engine': 'redis'}
    except Exception:
        return {'status': 'error', 'engine': 'redis'}

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
    admin_auth = _admin_auth_check()
    ready = database_status == 'ok' and admin_auth['status'] == 'ok'
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
            'message_bus': {'status': str(bus_check.get('status', 'unknown') or 'unknown'), 'engine': str(bus_check.get('engine', '') or '')},
            'broker': broker,
            'admin_auth': admin_auth,
        },
        'config': {
            'environment': settings.env,
            'message_bus_backend': settings.message_bus.backend,
        },
    }

