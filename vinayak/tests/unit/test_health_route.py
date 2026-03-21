import os
from pathlib import Path

from fastapi.testclient import TestClient

from vinayak.api.main import app
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


def test_health_ready_route_reports_database_and_broker_state(tmp_path: Path) -> None:
    database_path = tmp_path / 'vinayak-health.db'
    os.environ['VINAYAK_DATABASE_URL'] = f"sqlite:///{database_path.as_posix()}"
    os.environ.pop('DHAN_CLIENT_ID', None)
    os.environ.pop('DHAN_ACCESS_TOKEN', None)
    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])

    response = client.get('/health/ready')

    assert response.status_code == 200
    payload = response.json()
    assert payload['status'] == 'ready'
    assert payload['checks']['database']['status'] == 'ok'
    assert payload['checks']['database']['engine'] == 'sqlite'
    assert payload['checks']['broker']['status'] == 'missing_credentials'
    assert payload['checks']['broker']['broker'] == 'dhan'

    reset_database_state(os.environ['VINAYAK_DATABASE_URL'])
    os.environ.pop('VINAYAK_DATABASE_URL', None)
