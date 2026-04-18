import os
from pathlib import Path

from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.api.routes.health import _reset_health_ready_cache, _sanitize_database_url
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import reset_database_state


client = TestClient(app)


def test_health_route_returns_ok() -> None:
    response = client.get('/health')
    assert response.status_code == 200
    assert response.json() == {'status': 'ok'}


def test_health_live_route_returns_ok() -> None:
    response = client.get('/health/live')
    assert response.status_code == 200
    assert response.json() == {'status': 'ok'}


def test_database_url_sanitizer_removes_credentials() -> None:
    sanitized = _sanitize_database_url('postgresql+psycopg2://vinayak:super-secret@db.example:5432/vinayak')

    assert sanitized == 'postgresql+psycopg2://db.example:5432/vinayak'
    assert 'super-secret' not in sanitized
    assert 'vinayak@' not in sanitized


def test_health_ready_route_reports_platform_dependencies(tmp_path: Path) -> None:
    database_path = tmp_path / 'vinayak-health.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{database_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    os.environ.pop('DHAN_CLIENT_ID', None)
    os.environ.pop('DHAN_ACCESS_TOKEN', None)
    os.environ.pop('REDIS_URL', None)
    os.environ.pop('MONGODB_URL', None)
    reset_settings_cache()
    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    _reset_health_ready_cache()

    response = client.get('/health/ready')

    assert response.status_code == 200
    payload = response.json()
    assert payload['status'] == 'ready'
    assert payload['checks']['database']['status'] == 'ok'
    assert payload['checks']['database']['engine'] == 'sqlite'
    assert payload['checks']['broker']['status'] == 'missing_credentials'
    assert payload['checks']['broker']['broker'] == 'dhan'
    assert payload['checks']['cache']['engine'] == 'redis'
    assert payload['checks']['document_store']['engine'] == 'mongodb'
    assert payload['checks']['message_bus']['engine'] == 'noop'

    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    reset_settings_cache()
    _reset_health_ready_cache()


def test_health_ready_route_detects_missing_admin_auth_config_without_detail(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / 'vinayak-health-missing-admin.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{database_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    monkeypatch.delenv('VINAYAK_ADMIN_USERNAME', raising=False)
    monkeypatch.delenv('VINAYAK_ADMIN_PASSWORD', raising=False)
    monkeypatch.delenv('VINAYAK_ADMIN_SECRET', raising=False)
    reset_settings_cache()
    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    _reset_health_ready_cache()

    response = client.get('/health/ready')

    assert response.status_code == 200
    payload = response.json()
    assert payload['status'] == 'degraded'
    assert payload['checks']['admin_auth'] == {'status': 'error'}

    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    reset_settings_cache()
    _reset_health_ready_cache()


def test_health_ready_route_rejects_placeholder_admin_auth_config_without_detail(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / 'vinayak-health-placeholder-admin.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{database_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    monkeypatch.setenv('VINAYAK_ADMIN_PASSWORD', 'change-me-in-production')
    monkeypatch.setenv('VINAYAK_ADMIN_SECRET', 'change-me-in-production')
    reset_settings_cache()
    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    _reset_health_ready_cache()

    response = client.get('/health/ready')

    assert response.status_code == 200
    payload = response.json()
    assert payload['status'] == 'degraded'
    assert payload['checks']['admin_auth'] == {'status': 'error'}

    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    reset_settings_cache()
    _reset_health_ready_cache()


def test_health_ready_route_uses_short_ttl_cache(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / 'vinayak-health-cache.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{database_path.as_posix()}"
    os.environ['MESSAGE_BUS_ENABLED'] = 'false'
    monkeypatch.setenv('VINAYAK_HEALTH_READY_CACHE_TTL_SECONDS', '60')
    reset_settings_cache()
    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    _reset_health_ready_cache()

    calls = {'count': 0}

    def _cached_payload() -> dict[str, str]:
        calls['count'] += 1
        return {'status': 'ok', 'engine': 'mongodb'}

    monkeypatch.setattr('vinayak.api.routes.health.ProductCatalogService.readiness', lambda self: _cached_payload())

    first = client.get('/health/ready')
    second = client.get('/health/ready')

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls['count'] == 1

    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    os.environ.pop('MESSAGE_BUS_ENABLED', None)
    monkeypatch.delenv('VINAYAK_HEALTH_READY_CACHE_TTL_SECONDS', raising=False)
    reset_settings_cache()
    _reset_health_ready_cache()
