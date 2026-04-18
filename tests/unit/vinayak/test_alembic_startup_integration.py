from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import inspect

from vinayak.api.main import app
from vinayak.api.routes.health import _reset_health_ready_cache
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import get_engine, reset_database_state


def test_alembic_upgrade_and_app_startup_on_clean_database(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / 'vinayak_clean_migrated.db'
    db_url = f"sqlite:///{db_path.as_posix()}"
    monkeypatch.setenv('VINAYAK_DATABASE_URL', db_url)
    monkeypatch.setenv('MESSAGE_BUS_ENABLED', 'false')
    reset_settings_cache()
    reset_database_state(db_url)
    _reset_health_ready_cache()

    alembic_config = Config(str(Path('D:/Trading/app/vinayak/alembic.ini')))
    alembic_config.set_main_option('sqlalchemy.url', db_url)
    command.upgrade(alembic_config, 'head')

    inspector = inspect(get_engine(db_url))
    tables = set(inspector.get_table_names())
    assert 'signals' in tables
    assert 'reviewed_trades' in tables
    assert 'executions' in tables
    assert 'outbox_events' in tables
    assert 'users' in tables
    assert 'alembic_version' in tables

    with TestClient(app) as client:
        live = client.get('/health/live')
        ready = client.get('/health/ready')

    assert live.status_code == 200
    assert live.json() == {'status': 'ok'}
    assert ready.status_code == 200
    payload = ready.json()
    assert payload['checks']['database']['status'] == 'ok'
    assert payload['checks']['database']['engine'] == 'sqlite'

    reset_database_state(db_url)
    _reset_health_ready_cache()
