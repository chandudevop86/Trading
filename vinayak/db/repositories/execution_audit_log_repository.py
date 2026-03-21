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
    ) -> ExecutionAuditLogRecord:
        record = ExecutionAuditLogRecord(
            execution_id=execution_id,
            broker=broker,
            request_payload=json.dumps(request_payload, default=str),
            response_payload=json.dumps(response_payload, default=str) if response_payload is not None else None,
            status=status,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def list_audit_logs(self) -> list[ExecutionAuditLogRecord]:
        return list(self.session.query(ExecutionAuditLogRecord).order_by(ExecutionAuditLogRecord.id.desc()).all())

    def list_audit_logs_for_execution(self, execution_id: int) -> list[ExecutionAuditLogRecord]:
        return list(
            self.session.query(ExecutionAuditLogRecord)
            .filter(ExecutionAuditLogRecord.execution_id == execution_id)
            .order_by(ExecutionAuditLogRecord.id.desc())
            .all()
        )
