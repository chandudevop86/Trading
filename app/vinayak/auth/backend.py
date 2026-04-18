from __future__ import annotations

"""Backend helpers for web login and logout flows."""

import os

from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import COOKIE_NAME, LEGACY_COOKIE_NAME
from vinayak.auth.service import ADMIN_ROLE, AuthenticatedUser, UserAuthService


class WebAuthBackend:
    """Encapsulates login/logout behavior for the web surface."""

    def __init__(self, session: Session) -> None:
        self.auth = UserAuthService(session)

    def login_user(self, username: str, password: str) -> AuthenticatedUser | None:
        return self.auth.authenticate(username, password)

    def login_admin(self, username: str, password: str) -> AuthenticatedUser | None:
        self.auth.ensure_default_admin()
        user = self.auth.authenticate(username, password)
        if user is None or str(user.role).upper() != ADMIN_ROLE:
            return None
        return user

    def build_login_response(self, user: AuthenticatedUser, *, redirect_to: str) -> RedirectResponse:
        response = RedirectResponse(url=redirect_to, status_code=303)
        response.set_cookie(
            COOKIE_NAME,
            self.auth.create_session_token(user),
            httponly=True,
            samesite='lax',
            secure=self.secure_cookies_enabled(),
        )
        return response

    @staticmethod
    def build_logout_response(*, redirect_to: str) -> RedirectResponse:
        response = RedirectResponse(url=redirect_to, status_code=303)
        response.delete_cookie(COOKIE_NAME)
        response.delete_cookie(LEGACY_COOKIE_NAME)
        return response

    @staticmethod
    def secure_cookies_enabled() -> bool:
        value = str(os.getenv('VINAYAK_SECURE_COOKIES', 'true') or 'true').strip().lower()
        return value not in {'0', 'false', 'no'}


__all__ = ["WebAuthBackend"]
