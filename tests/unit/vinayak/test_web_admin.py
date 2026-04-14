import os
from pathlib import Path

from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import reset_database_state


def _configure_db(tmp_path: Path) -> None:
    db_path = tmp_path / 'vinayak_web_auth.db'
    os.environ['VINAYAK_DATABASE_URL'] = f'sqlite:///{db_path.as_posix()}'
    reset_settings_cache()
    reset_database_state()


def _cleanup_db() -> None:
    os.environ.pop('VINAYAK_DATABASE_URL', None)
    reset_settings_cache()
    reset_database_state()


def test_admin_console_requires_login_and_then_renders_dashboard(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        login_page = fresh.get('/admin')
        assert login_page.status_code == 200
        assert 'Vinayak Login' in login_page.text

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak-test-password',
        })
        assert response.status_code == 200
        html = response.text
        assert 'Admin Dashboard' in html
        assert '/admin/validation' in html
        assert '/admin/execution' in html
        assert '/admin/logs' in html
        assert '/admin/settings' in html
        assert '/workspace' in html
    finally:
        _cleanup_db()


def test_workspace_requires_login_and_then_renders(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        login_page = fresh.get('/workspace')
        assert login_page.status_code == 200
        assert 'Vinayak Login' in login_page.text

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak-test-password',
        })
        assert response.status_code == 200

        workspace = fresh.get('/workspace')
        assert workspace.status_code == 200
        assert 'Vinayak Workspace' in workspace.text
        assert '/dashboard/live-analysis' in workspace.text
    finally:
        _cleanup_db()


def test_admin_can_create_user_and_user_can_login(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        admin_login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak-test-password',
        })
        assert admin_login.status_code == 200

        created = fresh.post('/admin/users/create', data={
            'username': 'trader1',
            'password': 'trader123',
            'role': 'USER',
        })
        assert created.status_code == 200
        assert 'Current Users' in created.text
        assert 'trader1' in created.text

        fresh.post('/logout')

        blocked = fresh.get('/app')
        assert blocked.status_code == 200
        assert 'Vinayak Login' in blocked.text

        user_login = fresh.post('/login', data={
            'username': 'trader1',
            'password': 'trader123',
        })
        assert user_login.status_code == 200
        assert 'User View' in user_login.text

        live_signal = fresh.get('/app/live-signal')
        assert live_signal.status_code == 200
        assert 'Required Output' in live_signal.text
    finally:
        _cleanup_db()


def test_admin_login_rejects_invalid_credentials(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'wrong-password',
        })
        assert response.status_code == 200
        assert 'Invalid admin username or password.' in response.text
    finally:
        _cleanup_db()


def test_user_pages_require_login(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        home = fresh.get('/app')
        assert home.status_code == 200
        assert 'Vinayak Login' in home.text

        live_signal = fresh.get('/app/live-signal')
        assert live_signal.status_code == 200
        assert 'Vinayak Login' in live_signal.text
    finally:
        _cleanup_db()


def test_non_admin_cannot_open_admin_dashboard(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})
        fresh.post('/admin/users/create', data={'username': 'viewer1', 'password': 'viewer123', 'role': 'USER'})
        fresh.post('/logout')
        fresh.post('/login', data={'username': 'viewer1', 'password': 'viewer123'})

        response = fresh.get('/admin/dashboard')
        assert response.status_code == 403
    finally:
        _cleanup_db()


def test_admin_api_routes_use_same_cookie_session(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)

        unauth = fresh.get('/dashboard/summary')
        assert unauth.status_code == 401

        login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak-test-password',
        })
        assert login.status_code == 200

        authed = fresh.get('/dashboard/summary')
        assert authed.status_code == 200
    finally:
        _cleanup_db()


def test_admin_password_rotation_from_env_does_not_overwrite_existing_admin(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    os.environ['VINAYAK_ADMIN_PASSWORD'] = 'firstpass123'
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        first_login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'firstpass123',
        })
        assert first_login.status_code == 200

        fresh.post('/admin/logout')
        os.environ['VINAYAK_ADMIN_PASSWORD'] = 'secondpass456'

        old_login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'firstpass123',
        })
        assert old_login.status_code == 200
        assert 'Admin Dashboard' in old_login.text

        new_login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'secondpass456',
        })
        assert new_login.status_code == 200
        assert 'Invalid admin username or password.' in new_login.text
    finally:
        os.environ.pop('VINAYAK_ADMIN_PASSWORD', None)
        _cleanup_db()


def test_admin_env_rotation_does_not_invalidate_existing_admin_session(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    os.environ['VINAYAK_ADMIN_PASSWORD'] = 'firstpass123'
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'firstpass123',
        })
        assert login.status_code == 200
        assert fresh.get('/dashboard/summary').status_code == 200

        os.environ['VINAYAK_ADMIN_PASSWORD'] = 'secondpass456'

        stale_session = fresh.get('/dashboard/summary')
        assert stale_session.status_code == 200

        relogin = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'secondpass456',
        })
        assert relogin.status_code == 200
        assert 'Invalid admin username or password.' in relogin.text
    finally:
        os.environ.pop('VINAYAK_ADMIN_PASSWORD', None)
        _cleanup_db()


def test_admin_validation_page_shows_empty_state_when_no_analysis_exists(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})
        response = fresh.get('/admin/validation')
        assert response.status_code == 200
        assert 'No analysis run yet' in response.text
        assert 'Why Not Ready' in response.text
        assert 'NO_ANALYSIS_RUN_YET' in response.text
    finally:
        _cleanup_db()


def test_admin_login_requires_explicit_admin_env_configuration(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        monkeypatch.delenv('VINAYAK_ADMIN_USERNAME', raising=False)
        monkeypatch.delenv('VINAYAK_ADMIN_PASSWORD', raising=False)
        monkeypatch.delenv('VINAYAK_ADMIN_SECRET', raising=False)
        fresh = TestClient(app, raise_server_exceptions=False)

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak-test-password',
        })

        assert response.status_code == 200
        assert 'Admin authentication is not configured correctly.' in response.text
    finally:
        _cleanup_db()


def test_admin_login_rejects_placeholder_admin_env_configuration(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        monkeypatch.setenv('VINAYAK_ADMIN_PASSWORD', 'change-me-in-production')
        monkeypatch.setenv('VINAYAK_ADMIN_SECRET', 'change-me-in-production')
        fresh = TestClient(app, raise_server_exceptions=False)

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'change-me-in-production',
        })

        assert response.status_code == 200
        assert 'Admin authentication is not configured correctly.' in response.text
    finally:
        _cleanup_db()
