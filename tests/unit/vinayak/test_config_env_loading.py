from __future__ import annotations

from pathlib import Path

from vinayak.core.config import get_settings, reset_settings_cache


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
