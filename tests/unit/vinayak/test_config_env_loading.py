from __future__ import annotations

from pathlib import Path

import pytest

from vinayak.api.dependencies.admin_auth import auto_login_enabled
from vinayak.core.config import SettingsValidationError
from vinayak.core.config import get_settings
from vinayak.core.config import reset_settings_cache
from vinayak.core.config import validate_settings


def test_get_settings_loads_repo_or_app_env_file(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / 'repo'
    app_dir = repo_root / 'app' / 'vinayak'
    core_dir = app_dir / 'core'
    core_dir.mkdir(parents=True)
    env_file = app_dir / '.env'
    env_file.write_text(
        'VINAYAK_DATABASE_URL=sqlite:///loaded-from-env.db\n',
        encoding='utf-8',
    )

    monkeypatch.delenv('VINAYAK_DATABASE_URL', raising=False)
    reset_settings_cache()

    import vinayak.core.config as config_module

    original_file = config_module.__file__
    try:
        config_module.__file__ = str(core_dir / 'config.py')
        settings = get_settings()
        assert settings.sql.url == 'sqlite:///loaded-from-env.db'
    finally:
        config_module.__file__ = original_file
        reset_settings_cache()


def test_validate_settings_rejects_insecure_production_configuration(monkeypatch) -> None:
    monkeypatch.setenv('APP_ENV', 'production')
    monkeypatch.setenv('VINAYAK_DATABASE_URL', 'sqlite:///prod.db')
    monkeypatch.setenv('VINAYAK_AUTO_LOGIN', 'true')
    monkeypatch.setenv('VINAYAK_SECURE_COOKIES', 'false')
    monkeypatch.setenv('VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS', 'true')
    monkeypatch.setenv('MESSAGE_BUS_ENABLED', 'true')
    monkeypatch.delenv('MESSAGE_BUS_URL', raising=False)
    reset_settings_cache()

    with pytest.raises(SettingsValidationError) as exc:
        validate_settings(startup=True)

    message = str(exc.value)
    assert 'VINAYAK_AUTO_LOGIN must be disabled in production.' in message
    assert 'VINAYAK_SECURE_COOKIES must be enabled in production.' in message
    assert 'VINAYAK_DATABASE_URL must not use sqlite in production.' in message
    assert 'VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS must be disabled in production.' in message


def test_auto_login_is_forced_off_in_production(monkeypatch) -> None:
    monkeypatch.setenv('APP_ENV', 'production')
    monkeypatch.setenv('VINAYAK_AUTO_LOGIN', 'true')
    reset_settings_cache()

    assert auto_login_enabled() is False


def test_validate_settings_allows_safe_test_configuration(monkeypatch) -> None:
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('VINAYAK_DATABASE_URL', 'sqlite:///test.db')
    monkeypatch.setenv('VINAYAK_AUTO_LOGIN', 'false')
    monkeypatch.setenv('VINAYAK_SECURE_COOKIES', 'false')
    reset_settings_cache()

    settings = validate_settings(startup=True)

    assert settings.runtime.is_development_like is True
    assert settings.auth.secure_cookies is False
