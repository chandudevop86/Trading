from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from vinayak.db.session import Base


class ExecutionAuditLogRecord(Base):
    __tablename__ = 'execution_audit_logs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[int] = mapped_column(Integer, ForeignKey('executions.id'), nullable=False)
    broker: Mapped[str] = mapped_column(String(40), nullable=False)
    request_payload: Mapped[str] = mapped_column(Text, nullable=False)
    response_payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
