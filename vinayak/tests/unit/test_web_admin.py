from fastapi.testclient import TestClient

from vinayak.api.main import app


def test_admin_console_requires_login_and_then_renders_dashboard() -> None:
    fresh = TestClient(app)
    login_page = fresh.get('/admin')
    assert login_page.status_code == 200
    assert 'Vinayak Admin Login' in login_page.text

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


def test_workspace_requires_login_and_then_renders() -> None:
    fresh = TestClient(app)
    login_page = fresh.get('/workspace')
    assert login_page.status_code == 200
    assert 'Vinayak Admin Login' in login_page.text

    response = fresh.post('/admin/login', data={
        'username': 'admin',
        'password': 'vinayak123',
    })
    assert response.status_code == 200

    workspace = fresh.get('/workspace')
    assert workspace.status_code == 200
    assert 'Vinayak Workspace' in workspace.text
    assert '/dashboard/live-analysis' in workspace.text
    assert 'Run Live Analysis' in workspace.text
    assert 'Fetch Option Metrics' in workspace.text
    assert 'Send Telegram' in workspace.text
    assert 'Auto Execute' in workspace.text
    assert 'Security Map Path' in workspace.text
    assert 'Paper Log Path' in workspace.text
    assert 'Live Log Path' in workspace.text
    assert 'v4 Strategy Desk' in workspace.text


def test_user_pages_render_without_admin_login() -> None:
    fresh = TestClient(app)

    home = fresh.get('/app')
    assert home.status_code == 200
    assert 'User View' in home.text
    assert 'Final trading output only.' in home.text

    live_signal = fresh.get('/app/live-signal')
    assert live_signal.status_code == 200
    assert 'Required Output' in live_signal.text
    assert 'status' in live_signal.text

    history = fresh.get('/app/trade-history')
    assert history.status_code == 200
    assert 'Trade History' in history.text


def test_admin_login_rejects_invalid_credentials() -> None:
    fresh = TestClient(app)
    response = fresh.post('/admin/login', data={
        'username': 'admin',
        'password': 'wrong-password',
    })
    assert response.status_code == 200
    assert 'Invalid admin username or password.' in response.text


def test_admin_api_routes_use_same_cookie_session() -> None:
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
