from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse,JSONResponse
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import (
    COOKIE_NAME,
    LEGACY_COOKIE_NAME,
    get_current_user,
    require_admin_session,
)
from vinayak.api.dependencies.db import get_db
from vinayak.auth.service import ADMIN_ROLE, UserAuthService
from vinayak.db.repositories.deferred_execution_job_repository import DeferredExecutionJobRepository
from vinayak.messaging.outbox import OutboxService
from vinayak.observability.observability_dashboard_spec import build_observability_dashboard_html
from vinayak.web.app.role_pages import (
    render_admin_dashboard_page,
    render_admin_execution_page,
    render_admin_jobs_page,
    render_admin_logs_page,
    render_admin_settings_page,
    render_admin_validation_page,
    render_trade_history_page,
    render_user_home_page,
    render_user_signal_page,
)
from vinayak.web.app.workspace_html import WORKSPACE_DOWNLOADS_HTML, WORKSPACE_HTML, WORKSPACE_REPORTS_HTML
from vinayak.web.services.role_view_service import RoleViewService
from vinayak.api.services.live_analysis_jobs import get_live_analysis_job_service



router = APIRouter()


@router.get('/admin/dashboard')
def admin_dashboard_page(
    request: Request,
    db: Session = Depends(get_db),
    format: str = Query(default="html"),
):
    if not _admin_or_login(request):
        if format == "json":
            return JSONResponse(
                status_code=401,
                content={"detail": "Authentication required."}
            )
        return _render_login('Admin sign in to access the dashboard.', form_action='/admin/login')

    service = RoleViewService(db)
    payload = service.build_admin_dashboard()

    if format == "json":
        return JSONResponse(content=payload)

    return HTMLResponse(render_admin_dashboard_page(payload))




HOME_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vinayak Trading Platform</title>
  <style>
    :root {
      --bg: #07111d;
      --panel: #102338;
      --panel-2: #0c1b2d;
      --text: #eef4ff;
      --muted: #91a7c5;
      --line: #28435f;
      --accent: #ff9f3f;
      --accent-2: #ffcb76;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Segoe UI, Arial, sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(255, 159, 63, 0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(69, 165, 255, 0.14), transparent 30%),
        linear-gradient(180deg, #0d2035 0%, var(--bg) 62%);
    }
    .wrap { max-width: 1180px; margin: 0 auto; padding: 32px 24px 56px; }
    .nav { display:flex; justify-content:space-between; align-items:center; gap:16px; margin-bottom:42px; }
    .brand { font-size: 24px; font-weight: 800; letter-spacing: 0.02em; }
    .nav-links, .actions { display:flex; gap:12px; flex-wrap:wrap; }
    .hero { display:grid; grid-template-columns:1.2fr .9fr; gap:22px; align-items:stretch; margin-bottom:22px; }
    .card { background: linear-gradient(180deg, var(--panel), var(--panel-2)); border:1px solid var(--line); border-radius:22px; padding:28px; box-shadow:0 24px 60px rgba(0,0,0,0.22); }
    .eyebrow { display:inline-flex; padding:8px 12px; border-radius:999px; border:1px solid rgba(255,255,255,0.1); background: rgba(255,255,255,0.04); color: var(--accent-2); font-size:12px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; }
    h1 { margin:16px 0 14px; font-size:clamp(36px, 6vw, 58px); line-height:1.04; }
    .lead, .feature p, .footer-note { color: var(--muted); line-height: 1.6; }
    .button { display:inline-flex; align-items:center; justify-content:center; min-height:48px; padding:12px 18px; border-radius:14px; border:1px solid var(--line); text-decoration:none; font-weight:800; }
    .button.primary { color:#111; background: linear-gradient(135deg, #ffb45f, var(--accent)); }
    .button.secondary { color: var(--text); background: rgba(255,255,255,0.03); }
    .status-grid, .feature-grid { display:grid; gap:12px; }
    .status-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
    .feature-grid { grid-template-columns:repeat(3,minmax(0,1fr)); gap:18px; margin-top:18px; }
    .status-box { padding:16px; border-radius:16px; border:1px solid var(--line); background: rgba(255,255,255,0.03); }
    .status-label { color: var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }
    .status-value { margin-top:10px; font-size:24px; font-weight:800; }
    code { color: var(--accent-2); background: rgba(255,255,255,0.04); padding:2px 6px; border-radius:6px; }
    @media (max-width: 980px) { .hero, .feature-grid, .status-grid { grid-template-columns: 1fr; } .nav { align-items:start; flex-direction:column; } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="nav">
      <div class="brand">Vinayak</div>
      <div class="nav-links">
        <a class="button secondary" href="/health">Health</a>
        <a class="button secondary" href="/login">User Login</a>
        <a class="button secondary" href="/workspace">Workspace</a>
        <a class="button primary" href="/admin">Admin</a>
      </div>
    </div>

    <div class="hero">
      <section class="card">
        <div class="eyebrow">Role-Based Trading Platform</div>
        <h1>Admin accounts manage the system. User accounts get final trading output only.</h1>
        <p class="lead">Vinayak now supports database-backed users and roles: Admin pages for validation, execution, logs, settings, and user creation, plus User pages that only show final BUY, SELL, or NO TRADE output.</p>
        <div class="actions">
          <a class="button primary" href="/login">Open User Login</a>
          <a class="button secondary" href="/admin">Open Admin View</a>
          <a class="button secondary" href="/workspace/observability">Observability</a>
        </div>
        <p class="footer-note">Existing operator workspace remains available under <code>/workspace</code>. API endpoints remain under <code>/dashboard</code>, <code>/reviewed-trades</code>, and <code>/executions</code>.</p>
      </section>

      <aside class="card">
        <div class="status-grid">
          <div class="status-box"><div class="status-label">User Pages</div><div class="status-value">/app</div></div>
          <div class="status-box"><div class="status-label">Admin Pages</div><div class="status-value">/admin</div></div>
          <div class="status-box"><div class="status-label">Observability</div><div class="status-value">/workspace/observability</div></div>
          <div class="status-box"><div class="status-label">Operator Desk</div><div class="status-value">/workspace</div></div>
        </div>
      </aside>
    </div>

    <div class="feature-grid">
      <section class="card feature"><h2>User Output</h2><p>Only required trade fields are shown: symbol, status, entry, stop, target, RR, confidence, last updated, and message.</p></section>
      <section class="card feature"><h2>Admin Control</h2><p>Validation internals, rejection reasons, execution health, logs, settings, and user creation stay visible only to the admin role.</p></section>
      <section class="card feature"><h2>Service-Layer Driven</h2><p>The web pages read from existing Vinayak services and persisted outputs instead of calling strategy logic directly from the UI.</p></section>
    </div>
  </div>
</body>
</html>
"""

LOGIN_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vinayak Login</title>
  <style>
    :root { --bg:#081421; --panel:#102338; --panel-2:#0d1d2f; --text:#eef4ff; --muted:#8ea6c7; --accent:#ff9f3f; --line:#28435f; --bad:#ff6b6b; }
    * { box-sizing: border-box; }
    body { margin:0; min-height:100vh; display:grid; place-items:center; font-family:Segoe UI, Arial, sans-serif; background: radial-gradient(circle at top, #133052 0%, var(--bg) 55%); color:var(--text); padding:24px; }
    .panel { width:min(460px, 100%); background:linear-gradient(180deg, var(--panel), var(--panel-2)); border:1px solid var(--line); border-radius:20px; padding:28px; box-shadow:0 24px 60px rgba(0,0,0,0.28); }
    h1 { margin:0 0 10px; font-size:30px; }
    p { margin:0 0 20px; color:var(--muted); line-height:1.5; }
    label { display:block; font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }
    input { width:100%; padding:12px 14px; margin-bottom:16px; border-radius:12px; border:1px solid var(--line); background:rgba(255,255,255,0.03); color:var(--text); }
    button { width:100%; border:1px solid var(--line); background:linear-gradient(135deg, #ffb25f, var(--accent)); color:#111; border-radius:12px; padding:12px 16px; font-weight:700; cursor:pointer; }
    .error { margin-bottom:16px; padding:12px 14px; border-radius:12px; border:1px solid rgba(255,107,107,0.4); color:#ffd6d6; background:rgba(255,107,107,0.08); }
  </style>
</head>
<body>
  <div class="panel">
    <h1>Vinayak Login</h1>
    <p>Sign in with an admin or user account. Admins go to the operations console. Users go to the signal view.</p>
    __ERROR_BLOCK__
    <form method="post" action="__FORM_ACTION__">
      <label for="username">Username</label>
      <input id="username" name="username" type="text" autocomplete="username" required />
      <label for="password">Password</label>
      <input id="password" name="password" type="password" autocomplete="current-password" required />
      <button type="submit">Sign In</button>
    </form>
  </div>
</body>
</html>
"""


def _render_login(error_message: str | None = None, *, form_action: str = '/login') -> HTMLResponse:
    error_block = f'<div class="error">{error_message}</div>' if error_message else ''
    return HTMLResponse(
        LOGIN_HTML
        .replace('__ERROR_BLOCK__', error_block)
        .replace('__FORM_ACTION__', form_action)
    )


def _auth_config_error_message(*, admin_only: bool) -> str:
    prefix = 'Admin authentication' if admin_only else 'Authentication'
    return (
        f'{prefix} is not configured. '
        'Set VINAYAK_ADMIN_USERNAME, VINAYAK_ADMIN_PASSWORD, and VINAYAK_ADMIN_SECRET '
        'to real non-placeholder values and restart the app.'
    )

def _secure_cookies_enabled() -> bool:
    value = str(__import__('os').getenv('VINAYAK_SECURE_COOKIES', 'true') or 'true').strip().lower()
    return value not in {'0', 'false', 'no'}


def _set_session_cookie(response: RedirectResponse, token: str) -> None:
    response.set_cookie(COOKIE_NAME, token, httponly=True, samesite='lax', secure=_secure_cookies_enabled())


def _redirect_for_role(role: str) -> str:
    return '/admin/dashboard' if str(role).upper() == ADMIN_ROLE else '/app'


def _admin_or_login(request: Request) -> bool:
    user = get_current_user(request)
    return user is not None and str(user.role).upper() == ADMIN_ROLE


def _user_or_login(request: Request) -> bool:
    return get_current_user(request) is not None


@router.get('/', response_class=HTMLResponse)
def home_page() -> HTMLResponse:
    return HTMLResponse(HOME_HTML)


@router.get('/login', response_class=HTMLResponse)
def login_page(request: Request) -> HTMLResponse:
    user = get_current_user(request)
    if user is not None:
        return RedirectResponse(url=_redirect_for_role(user.role), status_code=303)
    return _render_login()


@router.post('/login', response_model=None)
def login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    auth = UserAuthService(db)
    try:
        user = auth.authenticate(username, password)
    except RuntimeError:
        return _render_login(_auth_config_error_message(admin_only=False))
    if user is None:
        return _render_login('Invalid username or password.')
    response = RedirectResponse(url=_redirect_for_role(user.role), status_code=303)
    try:
        _set_session_cookie(response, auth.create_session_token(user))
    except RuntimeError:
        return _render_login(_auth_config_error_message(admin_only=False))
    return response


@router.post('/logout', response_model=None)
def logout():
    response = RedirectResponse(url='/login', status_code=303)
    response.delete_cookie(COOKIE_NAME)
    response.delete_cookie(LEGACY_COOKIE_NAME)
    return response


@router.get('/app', response_class=HTMLResponse)
def user_home_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    if not _user_or_login(request):
        return _render_login('Sign in to access the user view.')
    service = RoleViewService(db)
    return HTMLResponse(render_user_home_page(service.build_user_home()))


@router.get('/app/live-signal', response_class=HTMLResponse)
def user_live_signal_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    if not _user_or_login(request):
        return _render_login('Sign in to access the live signal view.')
    service = RoleViewService(db)
    return HTMLResponse(render_user_signal_page(service.build_user_signal()))


@router.get('/app/trade-history', response_class=HTMLResponse)
def user_trade_history_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    if not _user_or_login(request):
        return _render_login('Sign in to access trade history.')
    service = RoleViewService(db)
    return HTMLResponse(render_trade_history_page(service.build_user_trade_history()))


@router.get('/workspace', response_class=HTMLResponse)
def live_workspace(request: Request) -> HTMLResponse:
    if not _admin_or_login(request):
        return _render_login('Admin login required for the workspace.', form_action='/admin/login')
    return HTMLResponse(WORKSPACE_HTML)


@router.get('/workspace/observability', response_class=HTMLResponse)
def observability_page(request: Request) -> HTMLResponse:
    if not _admin_or_login(request):
        return _render_login('Admin login required for observability.', form_action='/admin/login')
    return HTMLResponse(build_observability_dashboard_html())


@router.get('/workspace/reports', response_class=HTMLResponse)
def live_workspace_reports(request: Request) -> HTMLResponse:
    if not _admin_or_login(request):
        return _render_login('Admin login required for reports.', form_action='/admin/login')
    return HTMLResponse(WORKSPACE_REPORTS_HTML)


@router.get('/workspace/downloads', response_class=HTMLResponse)
def live_workspace_downloads(request: Request) -> HTMLResponse:
    if not _admin_or_login(request):
        return _render_login('Admin login required for downloads.', form_action='/admin/login')
    return HTMLResponse(WORKSPACE_DOWNLOADS_HTML)


@router.get('/admin', response_class=HTMLResponse)
def admin_console(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    if not _admin_or_login(request):
        return _render_login('Admin sign in to access the operations console.', form_action='/admin/login')
    service = RoleViewService(db)
    return HTMLResponse(render_admin_dashboard_page(service.build_admin_dashboard()))


@router.get('/admin/dashboard', response_class=HTMLResponse)
def admin_dashboard_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    require_admin_session(request)
    service = RoleViewService(db)
    return HTMLResponse(render_admin_dashboard_page(service.build_admin_dashboard()))


@router.get('/admin/validation', response_class=HTMLResponse)
def admin_validation_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    require_admin_session(request)
    service = RoleViewService(db)
    return HTMLResponse(render_admin_validation_page(service.build_validation_page()))


@router.get('/admin/execution', response_class=HTMLResponse)
def admin_execution_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    require_admin_session(request)
    service = RoleViewService(db)
    payload = service.build_execution_page()
    created = request.query_params.get('created')
    error = request.query_params.get('error')
    if created:
        payload['flash_message'] = created
        payload['flash_tone'] = 'good'
    if error:
        payload['flash_message'] = error
        payload['flash_tone'] = 'bad'
    return HTMLResponse(render_admin_execution_page(payload))


@router.post('/admin/execution/jobs/{job_id}/retry', response_model=None)
def admin_retry_deferred_execution_event(request: Request, job_id: str, db: Session = Depends(get_db)):
    require_admin_session(request)
    deferred_job = DeferredExecutionJobRepository(db).get_job(job_id)
    if deferred_job is None:
        return RedirectResponse(url=f'/admin/execution?error=Deferred%20execution%20job%20{job_id}%20was%20not%20found', status_code=303)
    if not deferred_job.outbox_event_id:
        return RedirectResponse(url=f'/admin/execution?error=Deferred%20execution%20job%20{job_id}%20has%20no%20linked%20outbox%20event', status_code=303)
    service = OutboxService(db)
    event_id = int(deferred_job.outbox_event_id)
    record = service.get_event(event_id)
    if record is None or str(record.event_name or '') != 'analysis.execution.deferred':
        return RedirectResponse(url=f'/admin/execution?error=Deferred%20execution%20event%20{event_id}%20was%20not%20found', status_code=303)
    try:
        service.retry_event(event_id)
    except ValueError as exc:
        return RedirectResponse(url=f'/admin/execution?error={str(exc)}', status_code=303)
    return RedirectResponse(url=f'/admin/execution?created=Deferred%20execution%20job%20{job_id}%20queued%20for%20retry', status_code=303)


@router.get('/admin/jobs', response_class=HTMLResponse)
def admin_jobs_page(
    request: Request,
    job_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    refresh_seconds: int = Query(default=0, ge=0, le=300),
    created: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> HTMLResponse:
    require_admin_session(request)
    service = get_live_analysis_job_service()
    jobs = service.list_jobs(limit=25, status=status)
    selected_job = None
    if job_id:
        selected_job = service.get(job_id)
    if selected_job is None and jobs:
        selected_job = jobs[0]
    payload = {
        'total': len(jobs),
        'jobs': jobs,
        'selected_job': selected_job,
        'status_filter': str(status or '').upper(),
        'refresh_seconds': int(refresh_seconds),
        'pending_count': sum(1 for item in jobs if str(item.get('status', '')).upper() == 'PENDING'),
        'running_count': sum(1 for item in jobs if str(item.get('status', '')).upper() == 'RUNNING'),
        'failed_count': sum(1 for item in jobs if str(item.get('status', '')).upper() == 'FAILED'),
    }
    if created:
        payload['flash_message'] = created
        payload['flash_tone'] = 'good'
    if error:
        payload['flash_message'] = error
        payload['flash_tone'] = 'bad'
    return HTMLResponse(render_admin_jobs_page(payload))


@router.post('/admin/jobs/{job_id}/retry', response_model=None)
def admin_retry_job(request: Request, job_id: str):
    require_admin_session(request)
    try:
        get_live_analysis_job_service().retry_job(job_id)
    except ValueError as exc:
        return RedirectResponse(url=f'/admin/jobs?job_id={job_id}&error={str(exc)}', status_code=303)
    return RedirectResponse(url=f'/admin/jobs?job_id={job_id}&created=Job%20{job_id}%20queued%20for%20retry', status_code=303)


@router.post('/admin/jobs/{job_id}/cancel', response_model=None)
def admin_cancel_job(request: Request, job_id: str):
    require_admin_session(request)
    try:
        get_live_analysis_job_service().cancel_job(job_id)
    except ValueError as exc:
        return RedirectResponse(url=f'/admin/jobs?job_id={job_id}&error={str(exc)}', status_code=303)
    return RedirectResponse(url=f'/admin/jobs?job_id={job_id}&created=Job%20{job_id}%20cancelled', status_code=303)


@router.get('/admin/logs', response_class=HTMLResponse)
def admin_logs_page(request: Request, db: Session = Depends(get_db)) -> HTMLResponse:
    require_admin_session(request)
    service = RoleViewService(db)
    return HTMLResponse(render_admin_logs_page(service.build_logs_page()))


@router.get('/admin/settings', response_class=HTMLResponse)
def admin_settings_page(
    request: Request,
    db: Session = Depends(get_db),
    created: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> HTMLResponse:
    require_admin_session(request)
    service = RoleViewService(db)
    payload = service.build_settings_page()
    if created:
        payload['flash_message'] = created
        payload['flash_tone'] = 'good'
    if error:
        payload['flash_message'] = error
        payload['flash_tone'] = 'bad'
    return HTMLResponse(render_admin_settings_page(payload))


@router.post('/admin/users/create', response_model=None)
def admin_create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form('USER'),
    db: Session = Depends(get_db),
):
    require_admin_session(request)
    auth = UserAuthService(db)
    try:
        user = auth.create_user(username=username, password=password, role=role)
    except ValueError as exc:
        return RedirectResponse(url=f'/admin/settings?error={str(exc)}', status_code=303)
    return RedirectResponse(url=f'/admin/settings?created=User%20{user.username}%20created', status_code=303)


@router.post('/admin/login', response_model=None)
def admin_login(username: str = Form(...), password: str = Form(...), db: Session = Depends(get_db)):
    auth = UserAuthService(db)
    try:
        auth.ensure_default_admin()
    except RuntimeError:
        return _render_login(_auth_config_error_message(admin_only=True), form_action='/admin/login')
    user = auth.authenticate(username, password)
    if user is None or str(user.role).upper() != ADMIN_ROLE:
        return _render_login('Invalid admin username or password.', form_action='/admin/login')
    response = RedirectResponse(url='/admin/dashboard', status_code=303)
    _set_session_cookie(response, auth.create_session_token(user))
    return response


@router.post('/admin/logout', response_model=None)
def admin_logout():
    response = RedirectResponse(url='/admin', status_code=303)
    response.delete_cookie(COOKIE_NAME)
    response.delete_cookie(LEGACY_COOKIE_NAME)
    return response



