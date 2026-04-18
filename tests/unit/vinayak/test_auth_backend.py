from __future__ import annotations

from dataclasses import dataclass

from vinayak.api.dependencies.admin_auth import COOKIE_NAME, LEGACY_COOKIE_NAME
from vinayak.auth.backend import WebAuthBackend
from vinayak.auth.service import ADMIN_ROLE, AuthenticatedUser


@dataclass
class _StubAuthService:
    user: AuthenticatedUser | None = None
    admin_user: AuthenticatedUser | None = None
    token: str = "session-token"

    def authenticate(self, username: str, password: str) -> AuthenticatedUser | None:
        if self.user is not None and username == self.user.username:
            return self.user
        if self.admin_user is not None and username == self.admin_user.username:
            return self.admin_user
        return None

    def ensure_default_admin(self) -> None:
        return None

    def create_session_token(self, user: AuthenticatedUser) -> str:
        return self.token


def test_web_auth_backend_builds_login_response_with_session_cookie() -> None:
    backend = WebAuthBackend.__new__(WebAuthBackend)
    backend.auth = _StubAuthService()
    user = AuthenticatedUser(id=1, username="admin", role=ADMIN_ROLE, is_active=True)

    response = backend.build_login_response(user, redirect_to="/admin/dashboard")

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/dashboard"
    assert COOKIE_NAME in response.headers.get("set-cookie", "")


def test_web_auth_backend_builds_logout_response_and_clears_cookies() -> None:
    response = WebAuthBackend.build_logout_response(redirect_to="/login")

    set_cookie = "\n".join(response.raw_headers[i][1].decode("latin-1") for i in range(len(response.raw_headers)) if response.raw_headers[i][0] == b"set-cookie")
    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert COOKIE_NAME in set_cookie
    assert LEGACY_COOKIE_NAME in set_cookie


def test_web_auth_backend_login_admin_requires_admin_role() -> None:
    backend = WebAuthBackend.__new__(WebAuthBackend)
    backend.auth = _StubAuthService(
        admin_user=AuthenticatedUser(id=1, username="admin", role=ADMIN_ROLE, is_active=True),
    )

    user = backend.login_admin("admin", "secret")

    assert user is not None
    assert user.role == ADMIN_ROLE
