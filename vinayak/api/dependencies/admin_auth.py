import hashlib
import os

from fastapi import HTTPException, Request


COOKIE_NAME = "vinayak_admin_session"


def admin_username() -> str:
    return os.getenv("VINAYAK_ADMIN_USERNAME", "admin")


def admin_password() -> str:
    return os.getenv("VINAYAK_ADMIN_PASSWORD", "vinayak123")


def admin_secret() -> str:
    return os.getenv("VINAYAK_ADMIN_SECRET", "vinayak-admin-secret")


def session_token() -> str:
    seed = f"{admin_username()}:{admin_password()}:{admin_secret()}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def is_authenticated(request: Request) -> bool:
    return request.cookies.get(COOKIE_NAME) == session_token()


def require_admin_session(request: Request) -> None:
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Admin authentication required.")
