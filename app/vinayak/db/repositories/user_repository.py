from __future__ import annotations

from sqlalchemy.orm import Session

from vinayak.db.models.user import UserRecord


class UserRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_username(self, username: str) -> UserRecord | None:
        return self.session.query(UserRecord).filter(UserRecord.username == str(username or '').strip()).one_or_none()

    def get_by_id(self, user_id: int) -> UserRecord | None:
        return self.session.get(UserRecord, int(user_id))

    def list_users(self) -> list[UserRecord]:
        return list(self.session.query(UserRecord).order_by(UserRecord.role.asc(), UserRecord.username.asc()).all())

    def create_user(self, *, username: str, password_hash: str, role: str, is_active: bool = True) -> UserRecord:
        record = UserRecord(
            username=str(username or '').strip(),
            password_hash=password_hash,
            role=str(role or 'USER').strip().upper(),
            is_active=bool(is_active),
        )
        self.session.add(record)
        self.session.flush()
        return record
