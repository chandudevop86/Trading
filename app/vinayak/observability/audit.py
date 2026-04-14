from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from vinayak.domain.models import AuditEvent, AuditEventType


def build_audit_event(
    *,
    event_type: AuditEventType,
    correlation_id: UUID,
    payload: dict[str, Any],
) -> AuditEvent:
    return AuditEvent(
        event_type=event_type,
        correlation_id=correlation_id,
        occurred_at=datetime.now(UTC),
        payload=payload,
    )
