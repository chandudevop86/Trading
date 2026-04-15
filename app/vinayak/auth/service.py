from __future__ import annotations

import base64
import hashlib
import hmac
import os
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.user import UserRecord
from vinayak.db.repositories.user_repository import UserRepository


ADMIN_ROLE = 'ADMIN'
USER_ROLE = 'USER'
PASSWORD_ITERATIONS = 120000
DISALLOWED_ADMIN_ENV_VALUES = {
    'VINAYAK_ADMIN_PASSWORD': {
        'Vinayak@123',
        'change-me',
        'change-me-for-development',
        'change-me-in-uat',
        'change-me-in-production',
    },
    'VINAYAK_ADMIN_SECRET': {
        'vinayak-admin-secret',
        'change-me',
        'change-me-for-development',
        'change-me-in-uat',
        'change-me-in-production',
    },
}


@dataclass(slots=True)
class AuthenticatedUser:
    id: int
    username: str
    role: str
    is_active: bool

    def to_cookie_token(self, secret: str, session_salt: str = '') -> str:
        payload = f'{self.id}:{self.username}:{self.role}:{session_salt}'.encode('utf-8')
        signature = hmac.new(secret.encode('utf-8'), payload, hashlib.sha256).hexdigest()
        return f'{self.id}:{signature}'


class UserAuthService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.users = UserRepository(session)

    @staticmethod
    def admin_username() -> str:
        return UserAuthService._required_admin_env('VINAYAK_ADMIN_USERNAME')

    @staticmethod
    def admin_password() -> str:
        return UserAuthService._required_admin_env('VINAYAK_ADMIN_PASSWORD')

    @staticmethod
    def auth_secret() -> str:
        return UserAuthService._required_admin_env('VINAYAK_ADMIN_SECRET')

    @staticmethod
    def _required_admin_env(name: str) -> str:
        value = str(os.getenv(name, '') or '').strip()
        if not value:
            raise RuntimeError(f'{name} must be configured for Vinayak admin authentication.')
        if UserAuthService._is_disallowed_admin_env_value(name, value):
            raise RuntimeError(f'{name} must be changed from the bundled example value before use.')
        return value

    @staticmethod
    def _is_disallowed_admin_env_value(name: str, value: str) -> bool:
        normalized = str(value or '').strip().lower()
        if not normalized:
            return False
        if 'change-me' in normalized or 'replace-me' in normalized:
            return True
        return normalized in DISALLOWED_ADMIN_ENV_VALUES.get(name, set())

    @staticmethod
    def hash_password(password: str) -> str:
        raw_password = str(password or '')
        salt = os.urandom(16)
        derived = hashlib.pbkdf2_hmac('sha256', raw_password.encode('utf-8'), salt, PASSWORD_ITERATIONS)
        return f'pbkdf2_sha256${PASSWORD_ITERATIONS}${base64.urlsafe_b64encode(salt).decode()}${base64.urlsafe_b64encode(derived).decode()}'

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        try:
            algorithm, iterations_raw, salt_raw, digest_raw = str(password_hash or '').split('$', 3)
            if algorithm != 'pbkdf2_sha256':
                return False
            iterations = int(iterations_raw)
            salt = base64.urlsafe_b64decode(salt_raw.encode())
            expected = base64.urlsafe_b64decode(digest_raw.encode())
            actual = hashlib.pbkdf2_hmac('sha256', str(password or '').encode('utf-8'), salt, iterations)
            return hmac.compare_digest(actual, expected)
        except Exception:
            return False

    def create_session_token(self, user: AuthenticatedUser) -> str:
        record = self.users.get_by_id(user.id)
        if record is None or not bool(record.is_active):
            raise ValueError('Active user record is required to build a session token.')
        return AuthenticatedUser(id=record.id, username=record.username, role=record.role, is_active=record.is_active).to_cookie_token(
            self.auth_secret(),
            session_salt=record.password_hash,
        )

    def ensure_default_admin(self) -> UserRecord:
        username = self.admin_username()
        expected_password = self.admin_password()
        existing = self.users.get_by_username(username)
        if existing is not None:
            if self._should_sync_default_admin_credentials() and (
                not self.verify_password(expected_password, existing.password_hash)
                or str(existing.role).upper() != ADMIN_ROLE
                or not bool(existing.is_active)
            ):
                existing.password_hash = self.hash_password(expected_password)
                existing.role = ADMIN_ROLE
                existing.is_active = True
                self.session.add(existing)
                self.session.commit()
                self.session.refresh(existing)
            return existing
        record = self.users.create_user(
            username=username,
            password_hash=self.hash_password(expected_password),
            role=ADMIN_ROLE,
            is_active=True,
        )
        self.session.commit()
        self.session.refresh(record)
        return record

    @staticmethod
    def _should_sync_default_admin_credentials() -> bool:
        env = str(os.getenv('APP_ENV', 'dev') or 'dev').strip().lower()
        if env in {'dev', 'development', 'test'}:
            return True
        flag = str(os.getenv('VINAYAK_SYNC_ADMIN_FROM_ENV', '') or '').strip().lower()
        return flag in {'1', 'true', 'yes', 'on'}

    def authenticate(self, username: str, password: str) -> AuthenticatedUser | None:
        record = self.users.get_by_username(username)
        if record is None or not bool(record.is_active):
            return None
        if not self.verify_password(password, record.password_hash):
            return None
        return AuthenticatedUser(id=record.id, username=record.username, role=record.role, is_active=record.is_active)

    def create_user(self, *, username: str, password: str, role: str = USER_ROLE, is_active: bool = True) -> UserRecord:
        cleaned_username = str(username or '').strip()
        cleaned_role = str(role or USER_ROLE).strip().upper()
        if not cleaned_username:
            raise ValueError('Username is required.')
        if len(str(password or '')) < 6:
            raise ValueError('Password must be at least 6 characters.')
        if cleaned_role not in {ADMIN_ROLE, USER_ROLE}:
            raise ValueError('Role must be ADMIN or USER.')
        if self.users.get_by_username(cleaned_username) is not None:
            raise ValueError('Username already exists.')
        record = self.users.create_user(
            username=cleaned_username,
            password_hash=self.hash_password(password),
            role=cleaned_role,
            is_active=is_active,
        )
        self.session.commit()
        self.session.refresh(record)
        return record

    def list_users(self) -> list[dict[str, Any]]:
        return [
            {
                'id': item.id,
                'username': item.username,
                'role': item.role,
                'is_active': bool(item.is_active),
                'created_at': item.created_at.isoformat() if item.created_at else '',
            }
            for item in self.users.list_users()
        ]

    def get_authenticated_user(self, token: str | None) -> AuthenticatedUser | None:
        raw = str(token or '').strip()
        if not raw or ':' not in raw:
            return None
        user_id_raw, signature = raw.split(':', 1)
        try:
            user_id = int(user_id_raw)
        except ValueError:
            return None
        record = self.users.get_by_id(user_id)
        if record is None or not bool(record.is_active):
            return None
        expected = AuthenticatedUser(id=record.id, username=record.username, role=record.role, is_active=record.is_active).to_cookie_token(
            self.auth_secret(),
            session_salt=record.password_hash,
        )
        if not hmac.compare_digest(raw, expected):
            return None
        return AuthenticatedUser(id=record.id, username=record.username, role=record.role, is_active=record.is_active)
