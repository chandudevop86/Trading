from __future__ import annotations

import os
from collections import Counter

from sqlalchemy.orm import Session

from vinayak.db.repositories.execution_audit_log_repository import ExecutionAuditLogRepository
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.execution.broker.dhan_client import DhanClient


class DashboardSummaryService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.reviewed_trade_repository = ReviewedTradeRepository(session)
        self.execution_repository = ExecutionRepository(session)
        self.execution_audit_log_repository = ExecutionAuditLogRepository(session)
        self.dhan_client = DhanClient(
            client_id=os.getenv('DHAN_CLIENT_ID'),
            access_token=os.getenv('DHAN_ACCESS_TOKEN'),
        )

    def build_summary(self) -> dict[str, object]:
        reviewed_trades = self.reviewed_trade_repository.list_reviewed_trades()
        executions = self.execution_repository.list_executions()
        audit_logs = self.execution_audit_log_repository.list_audit_logs()

        reviewed_trade_counts = Counter(record.status.upper() for record in reviewed_trades)
        execution_mode_counts = Counter(record.mode.upper() for record in executions)
        execution_status_counts = Counter(record.status.upper() for record in executions)
        audit_status_counts = Counter(record.status.upper() for record in audit_logs)
        recent_audit_failures = sum(1 for record in audit_logs if record.status.upper() in {'BLOCKED', 'REJECTED', 'ERROR'})

        return {
            'broker_ready': self.dhan_client.is_ready(),
            'broker_name': 'DHAN',
            'reviewed_trade_counts': dict(reviewed_trade_counts),
            'execution_mode_counts': dict(execution_mode_counts),
            'execution_status_counts': dict(execution_status_counts),
            'audit_status_counts': dict(audit_status_counts),
            'recent_audit_failures': recent_audit_failures,
        }
