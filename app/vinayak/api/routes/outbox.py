from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.outbox import OutboxDispatchResponse, OutboxEventListResponse, OutboxEventResponse, OutboxRetryResponse
from vinayak.messaging.outbox import OutboxService, dispatch_pending_outbox_events


router = APIRouter(prefix='/outbox', tags=['outbox'], dependencies=[Depends(require_admin_session)])


def _to_response(record) -> OutboxEventResponse:
    payload = json.loads(record.payload)
    return OutboxEventResponse(
        id=record.id,
        event_name=record.event_name,
        payload=payload if isinstance(payload, dict) else {'raw': payload},
        source=record.source,
        status=record.status,
        attempt_count=record.attempt_count,
        available_at=record.available_at,
        published_at=record.published_at,
        last_error=record.last_error,
        created_at=record.created_at,
    )


@router.get('', response_model=OutboxEventListResponse)
def list_outbox_events(
    status: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> OutboxEventListResponse:
    service = OutboxService(db)
    records = service.list_events(status=status)
    events = [_to_response(record) for record in records]
    return OutboxEventListResponse(total=len(events), events=events)


@router.get('/{event_id}', response_model=OutboxEventResponse)
def get_outbox_event(event_id: int, db: Session = Depends(get_db)) -> OutboxEventResponse:
    service = OutboxService(db)
    record = service.get_event(event_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f'Outbox event {event_id} was not found.')
    return _to_response(record)


@router.post('/dispatch', response_model=OutboxDispatchResponse)
def dispatch_outbox(limit: int = Query(default=50, ge=1, le=500), db: Session = Depends(get_db)) -> OutboxDispatchResponse:
    result = dispatch_pending_outbox_events(db, limit=limit)
    return OutboxDispatchResponse(published_count=result.published_count, failed_count=result.failed_count)


@router.post('/{event_id}/retry', response_model=OutboxRetryResponse)
def retry_outbox_event(event_id: int, db: Session = Depends(get_db)) -> OutboxRetryResponse:
    service = OutboxService(db)
    try:
        record = service.retry_event(event_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if 'was not found' in message else 422
        raise HTTPException(status_code=status_code, detail=message) from exc
    return OutboxRetryResponse(event=_to_response(record), message=f'Outbox event {event_id} queued for retry.')
