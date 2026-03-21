from fastapi.testclient import TestClient

from vinayak.api.main import app


client = TestClient(app)


def test_admin_console_requires_login_and_then_renders() -> None:
    login_page = client.get('/admin')
    assert login_page.status_code == 200
    assert 'Vinayak Admin Login' in login_page.text

    response = client.post('/admin/login', data={
        'username': 'admin',
        'password': 'vinayak123',
    })
    assert response.status_code == 200
    html = response.text
    assert 'Vinayak Admin Console' in html
    assert '/dashboard/summary' in html
    assert '/reviewed-trades' in html
    assert '/executions/audit-logs' in html
    assert '/executions' in html
    assert 'Load Execution Audit' in html
    assert 'Approve' in html
    assert 'Reject' in html
    assert 'Execute' in html
    assert 'Recent Executions' in html
    assert 'Open Audit' in html
    assert 'reviewedTradeStatusFilter' in html
    assert 'executionModeFilter' in html
    assert 'executionStatusFilter' in html
    assert 'auditBrokerFilter' in html
    assert 'auditStatusFilter' in html
    assert 'executionModeSelect' in html
    assert 'Logout' in html


def test_admin_login_rejects_invalid_credentials() -> None:
    response = client.post('/admin/login', data={
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
