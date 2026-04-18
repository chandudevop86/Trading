from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import ExecutionAuditLogListResponse, ExecutionAuditLogResponse, ExecutionListResponse, ExecutionResponse
from vinayak.api.schemas.strategy import ExecutionCreateRequest
from vinayak.db.repositories.execution_audit_log_repository import ExecutionAuditLogRepository
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.runtime import build_execution_facade


router = APIRouter(prefix='/executions', tags=['executions'], dependencies=[Depends(require_admin_session)])


def _execution_facade(db: Session):
    return build_execution_facade(db)


@router.get('', response_model=ExecutionListResponse)
def list_executions(db: Session = Depends(get_db)) -> ExecutionListResponse:
    records = _execution_facade(db).list_executions()
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
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            event_name=record.event_name,
            old_status=record.old_status,
            new_status=record.new_status,
            actor=record.actor,
            reason=record.reason,
            metadata_json=record.metadata_json,
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
            entity_type=record.entity_type,
            entity_id=record.entity_id,
            event_name=record.event_name,
            old_status=record.old_status,
            new_status=record.new_status,
            actor=record.actor,
            reason=record.reason,
            metadata_json=record.metadata_json,
            created_at=record.created_at,
        )
        for record in records
    ]
    return ExecutionAuditLogListResponse(total=len(audit_logs), audit_logs=audit_logs)


@router.post('', response_model=ExecutionResponse)
def create_execution(request: ExecutionCreateRequest, db: Session = Depends(get_db)) -> ExecutionResponse:
    try:
        record = _execution_facade(db).create_execution(
            ExecutionCreateCommand(
                signal_id=request.signal_id,
                reviewed_trade_id=request.reviewed_trade_id,
                trade_id=request.trade_id,
                strategy_name=request.strategy_name,
                symbol=request.symbol,
                side=request.side,
                entry_price=request.entry_price,
                stop_loss=request.stop_loss,
                target_price=request.target_price,
                quantity=request.quantity,
                validation_status=request.validation_status,
                reviewed_trade_status=request.reviewed_trade_status,
                mode=request.mode,
                broker=request.broker,
                status=request.status,
                executed_price=request.executed_price,
                metadata={
                    'execution_allowed': request.execution_allowed,
                    'system_status': request.system_status,
                    'go_live_status': request.go_live_status,
                    'duplicate_reason': request.duplicate_reason,
                    'setup_already_used': request.setup_already_used,
                    'trades_taken_today': request.trades_taken_today,
                    'max_trades_per_day': request.max_trades_per_day,
                    'realized_pnl_today': request.realized_pnl_today,
                    'max_daily_loss': request.max_daily_loss,
                    'kill_switch_enabled': request.kill_switch_enabled,
                    'active_trade_exists': request.active_trade_exists,
                    'cooldown_active': request.cooldown_active,
                },
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




