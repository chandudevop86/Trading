import os
from pathlib import Path

from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.core.config import reset_settings_cache
from vinayak.db.session import reset_database_state
from vinayak.web.app import main as web_main


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


def test_admin_console_login_form_posts_to_admin_login(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        login_page = fresh.get('/admin')
        assert login_page.status_code == 200
        assert 'action="/admin/login"' in login_page.text
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
        assert 'Admin authentication is not configured.' in response.text
        assert 'VINAYAK_ADMIN_USERNAME' in response.text
        assert 'VINAYAK_ADMIN_PASSWORD' in response.text
        assert 'VINAYAK_ADMIN_SECRET' in response.text
    finally:
        _cleanup_db()


def test_admin_jobs_page_renders_for_admin(tmp_path: Path) -> None:
    _configure_db(tmp_path)
    try:
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})
        response = fresh.get('/admin/jobs')
        assert response.status_code == 200
        assert 'Live Analysis Jobs' in response.text
        assert '/dashboard/live-analysis/jobs' in response.text
    finally:
        _cleanup_db()


def test_admin_execution_page_shows_deferred_execution_outcomes(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        class FakeRoleViewService:
            def __init__(self, db) -> None:
                self.db = db

            def build_execution_page(self):
                return {
                    'history': [],
                    'paper_summary': {'mode': 'PAPER', 'executed_count': 0, 'blocked_count': 0, 'duplicate_count': 0},
                    'latest_signal': {'symbol': '^NSEI', 'status': 'BUY'},
                    'deferred_execution_metrics': {
                        'enqueued_total': 3,
                        'attempt_total': 2,
                        'success_total': 1,
                        'failed_total': 1,
                        'last_status': 'FAIL',
                    },
                    'deferred_execution_jobs': [
                        {
                            'id': 'deferred-job-11',
                            'status': 'FAILED',
                            'symbol': '^NSEI',
                            'strategy': 'Breakout',
                            'execution_mode': 'PAPER',
                            'signal_count': 2,
                            'attempt_count': 2,
                            'outbox_status': 'FAILED',
                            'last_error': 'worker boom',
                        }
                    ],
                }

        monkeypatch.setattr(web_main, 'RoleViewService', FakeRoleViewService)
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})
        response = fresh.get('/admin/execution')
        assert response.status_code == 200
        assert 'Deferred Execution Outcomes' in response.text
        assert 'Recent Deferred Execution Jobs' in response.text
        assert 'Deferred Failed' in response.text
        assert 'worker boom' in response.text
        assert 'Breakout' in response.text
        assert '/admin/execution/jobs/deferred-job-11/retry' in response.text
    finally:
        _cleanup_db()


def test_admin_execution_retry_route_redirects_with_flash(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        class FakeDeferredExecutionJobRepository:
            def __init__(self, db) -> None:
                self.db = db

            def get_job(self, job_id: str):
                assert job_id == 'deferred-job-11'
                return type('DeferredJob', (), {'outbox_event_id': 11})()

        class FakeOutboxService:
            def __init__(self, db) -> None:
                self.db = db

            def get_event(self, event_id: int):
                assert event_id == 11
                return type('Event', (), {'event_name': 'analysis.execution.deferred'})()

            def retry_event(self, event_id: int):
                assert event_id == 11
                return object()

        monkeypatch.setattr(web_main, 'DeferredExecutionJobRepository', FakeDeferredExecutionJobRepository)
        monkeypatch.setattr(web_main, 'OutboxService', FakeOutboxService)
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})

        response = fresh.post('/admin/execution/jobs/deferred-job-11/retry', follow_redirects=False)

        assert response.status_code == 303
        assert 'created=Deferred%20execution%20job%20deferred-job-11%20queued%20for%20retry' in response.headers['location']
    finally:
        _cleanup_db()


def test_admin_jobs_page_shows_selected_job_details_and_actions(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        class FakeJobService:
            def list_jobs(self, limit: int = 25, status: str | None = None):
                assert status == 'FAILED'
                return [{
                    'job_id': 'job-1',
                    'status': 'FAILED',
                    'symbol': '^NSEI',
                    'interval': '5m',
                    'period': '1d',
                    'strategy': 'Breakout',
                    'requested_at': '2026-04-15T09:00:00Z',
                    'started_at': '2026-04-15T09:00:01Z',
                    'finished_at': '2026-04-15T09:00:05Z',
                    'error': 'boom',
                    'result': {'signal_count': 0, 'candle_count': 0},
                    'execution_type': 'NONE',
                }]

            def get(self, job_id: str):
                assert job_id == 'job-1'
                return self.list_jobs()[0]

        monkeypatch.setattr(web_main, 'get_live_analysis_job_service', lambda: FakeJobService())
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})
        response = fresh.get('/admin/jobs?job_id=job-1&status=FAILED&refresh_seconds=15')
        assert response.status_code == 200
        assert 'Selected Job' in response.text
        assert 'Retry Job' in response.text
        assert 'Auto Refresh' in response.text
        assert 'Status' in response.text
        assert 'Request Payload' in response.text
        assert 'Result Payload' in response.text
        assert 'Error Detail' in response.text
        assert 'execution_type' in response.text
        assert 'http-equiv="refresh"' in response.text
        assert 'job-1' in response.text
    finally:
        _cleanup_db()


def test_admin_job_retry_and_cancel_routes_redirect_with_flash(tmp_path: Path, monkeypatch) -> None:
    _configure_db(tmp_path)
    try:
        class FakeJobService:
            def retry_job(self, job_id: str):
                assert job_id == 'job-1'
                return {'job_id': job_id, 'status': 'PENDING'}

            def cancel_job(self, job_id: str):
                assert job_id == 'job-2'
                return {'job_id': job_id, 'status': 'CANCELLED'}

        monkeypatch.setattr(web_main, 'get_live_analysis_job_service', lambda: FakeJobService())
        fresh = TestClient(app, raise_server_exceptions=False)
        fresh.post('/admin/login', data={'username': 'admin', 'password': 'vinayak-test-password'})

        retried = fresh.post('/admin/jobs/job-1/retry', follow_redirects=False)
        cancelled = fresh.post('/admin/jobs/job-2/cancel', follow_redirects=False)

        assert retried.status_code == 303
        assert 'created=Job%20job-1%20queued%20for%20retry' in retried.headers['location']
        assert cancelled.status_code == 303
        assert 'created=Job%20job-2%20cancelled' in cancelled.headers['location']
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
        assert 'Admin authentication is not configured.' in response.text
        assert 'VINAYAK_ADMIN_USERNAME' in response.text
        assert 'VINAYAK_ADMIN_PASSWORD' in response.text
        assert 'VINAYAK_ADMIN_SECRET' in response.text
    finally:
        _cleanup_db()
