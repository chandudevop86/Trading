from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class ExecutionAdapterResult:
    broker: str
    status: str
    executed_price: float | None
    executed_at: datetime | None
    broker_reference: str | None = None
    notes: str | None = None
    audit_request_payload: dict[str, object] | None = None
    audit_response_payload: dict[str, object] | None = None
