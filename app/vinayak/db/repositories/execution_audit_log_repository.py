from __future__ import annotations

import json

from sqlalchemy.orm import Session

from vinayak.db.models.execution_audit_log import ExecutionAuditLogRecord


class ExecutionAuditLogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_audit_log(
        self,
        execution_id: int,
        broker: str,
        request_payload: dict[str, object],
        response_payload: dict[str, object] | None,
        status: str,
        *,
        entity_type: str | None = None,
        entity_id: int | None = None,
        event_name: str | None = None,
        old_status: str | None = None,
        new_status: str | None = None,
        actor: str | None = None,
        reason: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> ExecutionAuditLogRecord:
        record = ExecutionAuditLogRecord(
            execution_id=execution_id,
            broker=broker,
            request_payload=json.dumps(request_payload, default=str),
            response_payload=json.dumps(response_payload, default=str) if response_payload is not None else None,
            status=status,
            entity_type=entity_type,
            entity_id=entity_id,
            event_name=event_name,
            old_status=old_status,
            new_status=new_status,
            actor=actor,
            reason=reason,
            metadata_json=json.dumps(metadata, default=str) if metadata is not None else None,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def create_lifecycle_audit(
        self,
        *,
        entity_type: str,
        entity_id: int,
        old_status: str | None,
        new_status: str | None,
        actor: str,
        reason: str | None,
        metadata: dict[str, object] | None = None,
        execution_id: int | None = None,
        broker: str = 'INTERNAL',
        event_name: str = 'lifecycle.transition',
    ) -> ExecutionAuditLogRecord:
        payload = {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'old_status': old_status,
            'new_status': new_status,
            'actor': actor,
            'reason': reason,
        }
        return self.create_audit_log(
            execution_id=execution_id,
            broker=broker,
            request_payload=payload,
            response_payload=metadata,
            status=new_status or old_status or 'UNKNOWN',
            entity_type=entity_type,
            entity_id=entity_id,
            event_name=event_name,
            old_status=old_status,
            new_status=new_status,
            actor=actor,
            reason=reason,
            metadata=metadata,
        )

    def list_audit_logs(self) -> list[ExecutionAuditLogRecord]:
        return list(self.session.query(ExecutionAuditLogRecord).order_by(ExecutionAuditLogRecord.id.desc()).all())

    def list_audit_logs_for_execution(self, execution_id: int) -> list[ExecutionAuditLogRecord]:
        return list(
            self.session.query(ExecutionAuditLogRecord)
            .filter(ExecutionAuditLogRecord.execution_id == execution_id)
            .order_by(ExecutionAuditLogRecord.id.desc())
            .all()
        )
