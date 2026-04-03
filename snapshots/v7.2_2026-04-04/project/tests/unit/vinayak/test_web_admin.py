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
        fresh = TestClient(app)
        login_page = fresh.get('/admin')
        assert login_page.status_code == 200
        assert 'Vinayak Login' in login_page.text

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak123',
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
        fresh = TestClient(app)
        login_page = fresh.get('/workspace')
        assert login_page.status_code == 200
        assert 'Vinayak Login' in login_page.text

        response = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak123',
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
        fresh = TestClient(app)
        admin_login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak123',
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
        fresh = TestClient(app)
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
        fresh = TestClient(app)
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
        fresh = TestClient(app)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
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
        fresh = TestClient(app)

        unauth = fresh.get('/dashboard/summary')
        assert unauth.status_code == 401

        login = fresh.post('/admin/login', data={
            'username': 'admin',
            'password': 'vinayak123',
        })
        assert login.status_code == 200

        authed = fresh.get('/dashboard/summary')
        assert authed.status_code == 200
    finally:
        _cleanup_db()


def test_admin_validation_page_shows_empty_state_when_no_analysis_exists(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak123'})
        response = fresh.get('/admin/validation')
        assert response.status_code == 200
        assert 'No analysis run yet' in response.text
        assert 'Why Not Ready' in response.text
        assert 'NO_ANALYSIS_RUN_YET' in response.text
    finally:
        _cleanup_db()
