import hashlib

from fastapi import HTTPException, Request

from vinayak.auth.service import ADMIN_ROLE, AuthenticatedUser, UserAuthService
from vinayak.core.config import get_settings
from vinayak.db.session import build_session_factory
from vinayak.db.session import initialize_database


settings = get_settings()
COOKIE_NAME = settings.auth.session_cookie_name
LEGACY_COOKIE_NAME = settings.auth.legacy_session_cookie_name


def auto_login_enabled() -> bool:
    settings = get_settings()
    if settings.runtime.is_production:
        return False
    return settings.auth.auto_login_enabled


def admin_username() -> str:
    return UserAuthService.admin_username()


def admin_password() -> str:
    return UserAuthService.admin_password()


def admin_secret() -> str:
    return UserAuthService.auth_secret()


def session_token() -> str:
    seed = f'{admin_username()}:{admin_password()}:{admin_secret()}'
    return hashlib.sha256(seed.encode('utf-8')).hexdigest()


def _load_user_from_session_token(raw_token: str | None) -> AuthenticatedUser | None:
    initialize_database()
    session_factory = build_session_factory()
    session = session_factory()
    try:
        service = UserAuthService(session)
        return service.get_authenticated_user(raw_token)
    finally:
        session.close()


def _load_default_admin_user() -> AuthenticatedUser | None:
    initialize_database()
    session_factory = build_session_factory()
    session = session_factory()
    try:
        service = UserAuthService(session)
        record = service.ensure_default_admin()
        return AuthenticatedUser(
            id=record.id,
            username=record.username,
            role=record.role,
            is_active=record.is_active,
        )
    finally:
        session.close()


def get_current_user(request: Request) -> AuthenticatedUser | None:
    raw_token = request.cookies.get(COOKIE_NAME) or request.cookies.get(LEGACY_COOKIE_NAME)
    user = _load_user_from_session_token(raw_token)
    if user is not None:
        return user
    if auto_login_enabled():
        return _load_default_admin_user()
    return None


def is_authenticated(request: Request) -> bool:
    return get_current_user(request) is not None


def require_user_session(request: Request) -> AuthenticatedUser:
    user = get_current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail='Authentication required.')
    return user


def require_admin_session(request: Request) -> AuthenticatedUser:
    user = require_user_session(request)
    if str(user.role).upper() != ADMIN_ROLE:
        raise HTTPException(status_code=403, detail='Admin authentication required.')
    return user
