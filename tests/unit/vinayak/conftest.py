import pytest

from vinayak.core.config import reset_settings_cache


@pytest.fixture(autouse=True)
def _vinayak_test_auth_env(monkeypatch):
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('VINAYAK_ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('VINAYAK_ADMIN_PASSWORD', 'vinayak-test-password')
    monkeypatch.setenv('VINAYAK_ADMIN_SECRET', 'vinayak-test-secret')
    monkeypatch.setenv('VINAYAK_SECURE_COOKIES', 'false')
    monkeypatch.setenv('VINAYAK_AUTO_LOGIN', 'false')
    monkeypatch.setenv('VINAYAK_SYNC_ADMIN_FROM_ENV', 'false')
    reset_settings_cache()
    yield
    reset_settings_cache()
