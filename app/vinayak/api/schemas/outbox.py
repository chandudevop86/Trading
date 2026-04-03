from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class OutboxEventResponse(BaseModel):
    id: int
    event_name: str
    payload: dict[str, Any]
    source: str
    status: str
    attempt_count: int
    available_at: datetime
    published_at: datetime | None
    last_error: str | None
    created_at: datetime


class OutboxEventListResponse(BaseModel):
    total: int
    events: list[OutboxEventResponse]


class OutboxRetryResponse(BaseModel):
    event: OutboxEventResponse
    message: str


class OutboxDispatchResponse(BaseModel):
    published_count: int
    failed_count: int
