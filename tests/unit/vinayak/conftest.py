import os

import pytest


@pytest.fixture(autouse=True)
def _vinayak_test_auth_env(monkeypatch):
    monkeypatch.setenv('APP_ENV', 'test')
    monkeypatch.setenv('VINAYAK_ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('VINAYAK_ADMIN_PASSWORD', 'vinayak-test-password')
    monkeypatch.setenv('VINAYAK_ADMIN_SECRET', 'vinayak-test-secret')
    monkeypatch.setenv('VINAYAK_SECURE_COOKIES', 'false')
