from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import ExecutionAuditLogListResponse, ExecutionAuditLogResponse, ExecutionListResponse, ExecutionResponse
from vinayak.api.schemas.strategy import ExecutionCreateRequest
from vinayak.db.repositories.execution_audit_log_repository import ExecutionAuditLogRepository
from vinayak.execution.service import ExecutionCreateCommand, ExecutionService


router = APIRouter(prefix='/executions', tags=['executions'], dependencies=[Depends(require_admin_session)])


@router.get('', response_model=ExecutionListResponse)
def list_executions(db: Session = Depends(get_db)) -> ExecutionListResponse:
    service = ExecutionService(db)
    records = service.list_executions()
    executions = [
        ExecutionResponse(
            id=record.id,
            signal_id=record.signal_id,
            reviewed_trade_id=record.reviewed_trade_id,
            mode=record.mode,
            broker=record.broker,
            status=record.status,
            executed_price=record.executed_price,
            executed_at=record.executed_at,
            broker_reference=record.broker_reference,
            notes=record.notes,
        )
        for record in records
    ]
    return ExecutionListResponse(total=len(executions), executions=executions)


@router.get('/audit-logs', response_model=ExecutionAuditLogListResponse)
def list_execution_audit_logs(db: Session = Depends(get_db)) -> ExecutionAuditLogListResponse:
    repository = ExecutionAuditLogRepository(db)
    records = repository.list_audit_logs()
    audit_logs = [
        ExecutionAuditLogResponse(
            id=record.id,
            execution_id=record.execution_id,
            broker=record.broker,
            request_payload=record.request_payload,
            response_payload=record.response_payload,
            status=record.status,
            created_at=record.created_at,
        )
        for record in records
    ]
    return ExecutionAuditLogListResponse(total=len(audit_logs), audit_logs=audit_logs)


@router.get('/{execution_id}/audit', response_model=ExecutionAuditLogListResponse)
def list_execution_audit_logs_for_execution(execution_id: int, db: Session = Depends(get_db)) -> ExecutionAuditLogListResponse:
    repository = ExecutionAuditLogRepository(db)
    records = repository.list_audit_logs_for_execution(execution_id)
    if not records:
        raise HTTPException(status_code=404, detail=f'No audit logs were found for execution {execution_id}.')
    audit_logs = [
        ExecutionAuditLogResponse(
            id=record.id,
            execution_id=record.execution_id,
            broker=record.broker,
            request_payload=record.request_payload,
            response_payload=record.response_payload,
            status=record.status,
            created_at=record.created_at,
        )
        for record in records
    ]
    return ExecutionAuditLogListResponse(total=len(audit_logs), audit_logs=audit_logs)


@router.post('', response_model=ExecutionResponse)
def create_execution(request: ExecutionCreateRequest, db: Session = Depends(get_db)) -> ExecutionResponse:
    service = ExecutionService(db)
    try:
        record = service.create_execution(
            ExecutionCreateCommand(
                signal_id=request.signal_id,
                reviewed_trade_id=request.reviewed_trade_id,
                mode=request.mode,
                broker=request.broker,
                status=request.status,
                executed_price=request.executed_price,
            )
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if 'was not found' in detail else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc

    return ExecutionResponse(
        id=record.id,
        signal_id=record.signal_id,
        reviewed_trade_id=record.reviewed_trade_id,
        mode=record.mode,
        broker=record.broker,
        status=record.status,
        executed_price=record.executed_price,
        executed_at=record.executed_at,
        broker_reference=record.broker_reference,
        notes=record.notes,
    )
