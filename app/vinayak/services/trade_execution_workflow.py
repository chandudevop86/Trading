from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.execution_audit_log_repository import (
    ExecutionAuditLogRepository,
)
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.domain.exceptions import DuplicateExecutionRequestError
from vinayak.domain.statuses import (
    BLOCKED_EXECUTION_STATUSES,
    FAILED_EXECUTION_STATUSES,
    ReviewedTradeStatus,
    SUCCESSFUL_EXECUTION_STATUSES,
    WorkflowActor,
    normalize_execution_status,
    normalize_reviewed_trade_status,
)
from vinayak.domain.transitions import validate_reviewed_trade_transition
from vinayak.execution.events import (
    build_trade_executed_event,
    build_trade_execute_requested_event,
    build_trade_execution_rejected_event,
    build_trade_exited_event,
)
from vinayak.execution.outbox_service import OutboxService


@dataclass(slots=True, frozen=True)
class WorkflowContext:
    actor: str = WorkflowActor.SYSTEM.value
    reason: str | None = None
    source: str = WorkflowActor.EXECUTION_WORKFLOW.value
    metadata: dict[str, Any] | None = None


class TradeExecutionWorkflowService:
    """Central lifecycle orchestration for reviewed trade and execution flows."""

    def __init__(
        self,
        session: Session,
        *,
        reviewed_trade_repository: ReviewedTradeRepository | None = None,
        execution_repository: ExecutionRepository | None = None,
        execution_audit_log_repository: ExecutionAuditLogRepository | None = None,
        outbox: OutboxService | None = None,
    ) -> None:
        self.session = session
        self.reviewed_trade_repository = reviewed_trade_repository or ReviewedTradeRepository(session)
        self.execution_repository = execution_repository or ExecutionRepository(session)
        self.execution_audit_log_repository = execution_audit_log_repository or ExecutionAuditLogRepository(session)
        self.outbox = outbox or OutboxService(session)

    def transition_reviewed_trade(
        self,
        reviewed_trade: ReviewedTradeRecord,
        new_status: str | ReviewedTradeStatus,
        *,
        context: WorkflowContext | None = None,
        notes: str | None = None,
        quantity: int | None = None,
        lots: int | None = None,
        allow_same_status: bool = True,
        auto_commit: bool = False,
    ) -> ReviewedTradeRecord:
        ctx = context or WorkflowContext()
        current_status = normalize_reviewed_trade_status(reviewed_trade.status)
        target_status = normalize_reviewed_trade_status(new_status)

        if current_status != target_status:
            validate_reviewed_trade_transition(current_status, target_status)
        elif not allow_same_status:
            raise ValueError(f'Reviewed trade {reviewed_trade.id} is already in status {target_status.value}.')

        updated = self.reviewed_trade_repository.update_reviewed_trade(
            reviewed_trade,
            status=target_status.value,
            notes=notes,
            quantity=quantity,
            lots=lots,
        )
        self.execution_audit_log_repository.create_lifecycle_audit(
            entity_type='reviewed_trade',
            entity_id=updated.id,
            execution_id=None,
            old_status=current_status.value,
            new_status=target_status.value,
            actor=ctx.actor,
            reason=ctx.reason,
            metadata=ctx.metadata,
            event_name='reviewed_trade.lifecycle.transition',
        )

        if auto_commit:
            self.session.commit()
            self.session.refresh(updated)
        return updated

    def approve_reviewed_trade(
        self,
        reviewed_trade: ReviewedTradeRecord,
        *,
        actor: str = WorkflowActor.REVIEW_SERVICE.value,
        reason: str | None = None,
        notes: str | None = None,
        quantity: int | None = None,
        lots: int | None = None,
        auto_commit: bool = False,
    ) -> ReviewedTradeRecord:
        return self.transition_reviewed_trade(
            reviewed_trade,
            ReviewedTradeStatus.APPROVED,
            context=WorkflowContext(actor=actor, reason=reason, source=actor),
            notes=notes,
            quantity=quantity,
            lots=lots,
            auto_commit=auto_commit,
        )

    def reject_reviewed_trade(
        self,
        reviewed_trade: ReviewedTradeRecord,
        *,
        actor: str = WorkflowActor.REVIEW_SERVICE.value,
        reason: str | None = None,
        notes: str | None = None,
        auto_commit: bool = False,
    ) -> ReviewedTradeRecord:
        return self.transition_reviewed_trade(
            reviewed_trade,
            ReviewedTradeStatus.REJECTED,
            context=WorkflowContext(actor=actor, reason=reason, source=actor),
            notes=notes,
            auto_commit=auto_commit,
        )

    def request_execution(
        self,
        reviewed_trade: ReviewedTradeRecord,
        *,
        mode: str,
        broker: str,
        actor: str = WorkflowActor.EXECUTION_SERVICE.value,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ReviewedTradeRecord:
        self.ensure_no_duplicate_execution(
            reviewed_trade_id=reviewed_trade.id,
            signal_id=reviewed_trade.signal_id,
            mode=mode,
            broker=broker,
            broker_reference=None,
        )
        updated = self.transition_reviewed_trade(
            reviewed_trade,
            ReviewedTradeStatus.EXECUTE_REQUESTED,
            context=WorkflowContext(actor=actor, reason=reason, source=actor, metadata=metadata),
            notes=reviewed_trade.notes,
        )
        event_name, payload = build_trade_execute_requested_event(
            reviewed_trade_id=updated.id,
            signal_id=updated.signal_id,
            mode=mode,
            broker=broker,
            actor=actor,
            source=actor,
            reason=reason,
            metadata=metadata,
        )
        self.outbox.enqueue(event_name=event_name, payload=payload, source=actor)
        return updated

    def complete_execution(
        self,
        reviewed_trade: ReviewedTradeRecord,
        execution: ExecutionRecord,
        *,
        actor: str = WorkflowActor.EXECUTION_SERVICE.value,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ReviewedTradeRecord:
        execution_status = normalize_execution_status(execution.status)
        if execution_status in SUCCESSFUL_EXECUTION_STATUSES:
            target_status = ReviewedTradeStatus.EXECUTED
            event_name, payload = build_trade_executed_event(
                reviewed_trade_id=reviewed_trade.id,
                signal_id=reviewed_trade.signal_id,
                execution_id=execution.id,
                mode=execution.mode,
                broker=execution.broker,
                broker_reference=execution.broker_reference,
                actor=actor,
                source=actor,
                reason=reason,
                metadata=metadata,
            )
        elif execution_status in BLOCKED_EXECUTION_STATUSES:
            target_status = ReviewedTradeStatus.EXECUTION_REJECTED
            event_name, payload = build_trade_execution_rejected_event(
                reviewed_trade_id=reviewed_trade.id,
                signal_id=reviewed_trade.signal_id,
                execution_id=execution.id,
                mode=execution.mode,
                broker=execution.broker,
                broker_reference=execution.broker_reference,
                actor=actor,
                source=actor,
                reason=reason or execution.notes,
                metadata=metadata,
            )
        elif execution_status in FAILED_EXECUTION_STATUSES:
            target_status = ReviewedTradeStatus.FAILED
            event_name, payload = build_trade_executed_event(
                reviewed_trade_id=reviewed_trade.id,
                signal_id=reviewed_trade.signal_id,
                execution_id=execution.id,
                mode=execution.mode,
                broker=execution.broker,
                broker_reference=execution.broker_reference,
                actor=actor,
                source=actor,
                reason=reason or execution.notes,
                metadata={'result': 'failed', **dict(metadata or {})},
            )
        else:
            return reviewed_trade

        updated = self.transition_reviewed_trade(
            reviewed_trade,
            target_status,
            context=WorkflowContext(actor=actor, reason=reason or execution.notes, source=actor, metadata=metadata),
            notes=reviewed_trade.notes,
        )
        self.execution_audit_log_repository.create_lifecycle_audit(
            entity_type='execution',
            entity_id=execution.id,
            execution_id=execution.id,
            old_status=None,
            new_status=execution_status.value,
            actor=actor,
            reason=reason or execution.notes,
            metadata={
                'reviewed_trade_id': reviewed_trade.id,
                'signal_id': reviewed_trade.signal_id,
                **dict(metadata or {}),
            },
            event_name='execution.lifecycle.transition',
            broker=execution.broker,
        )
        self.outbox.enqueue(event_name=event_name, payload=payload, source=actor)
        return updated

    def fail_execution(
        self,
        reviewed_trade: ReviewedTradeRecord,
        execution: ExecutionRecord,
        *,
        actor: str = WorkflowActor.EXECUTION_SERVICE.value,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ReviewedTradeRecord:
        execution.status = 'FAILED'
        return self.complete_execution(
            reviewed_trade,
            execution,
            actor=actor,
            reason=reason or execution.notes,
            metadata=metadata,
        )

    def exit_trade(
        self,
        reviewed_trade: ReviewedTradeRecord,
        *,
        execution: ExecutionRecord | None = None,
        actor: str = WorkflowActor.EXECUTION_SERVICE.value,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        auto_commit: bool = False,
    ) -> ReviewedTradeRecord:
        updated = self.transition_reviewed_trade(
            reviewed_trade,
            ReviewedTradeStatus.EXITED,
            context=WorkflowContext(actor=actor, reason=reason, source=actor, metadata=metadata),
            notes=reviewed_trade.notes,
            auto_commit=auto_commit,
        )
        event_name, payload = build_trade_exited_event(
            reviewed_trade_id=updated.id,
            signal_id=updated.signal_id,
            execution_id=getattr(execution, 'id', None),
            mode=getattr(execution, 'mode', None),
            broker=getattr(execution, 'broker', None),
            broker_reference=getattr(execution, 'broker_reference', None),
            actor=actor,
            source=actor,
            reason=reason,
            metadata=metadata,
        )
        self.outbox.enqueue(event_name=event_name, payload=payload, source=actor)
        return updated

    def ensure_no_duplicate_execution(
        self,
        *,
        reviewed_trade_id: int | None,
        signal_id: int | None,
        mode: str,
        broker: str | None = None,
        broker_reference: str | None,
    ) -> None:
        if reviewed_trade_id is not None:
            existing = self.execution_repository.get_by_reviewed_trade_mode(
                reviewed_trade_id=reviewed_trade_id,
                mode=mode,
            )
            if existing is not None:
                raise DuplicateExecutionRequestError(
                    f'Duplicate execution blocked for reviewed trade {reviewed_trade_id} in {mode} mode.'
                )
        if signal_id is not None:
            existing = self.execution_repository.get_by_signal_mode(signal_id=signal_id, mode=mode)
            if existing is not None:
                raise DuplicateExecutionRequestError(
                    f'Duplicate execution blocked for signal {signal_id} in {mode} mode.'
                )
        normalized_reference = str(broker_reference or '').strip()
        normalized_broker = str(broker or '').strip().upper()
        if normalized_broker and normalized_reference:
            existing = self.execution_repository.get_by_broker_reference(
                broker=normalized_broker,
                broker_reference=normalized_reference,
            )
            if existing is not None:
                raise DuplicateExecutionRequestError(
                    f'Duplicate execution blocked for broker {normalized_broker} broker_reference {normalized_reference}.'
                )


__all__ = ['TradeExecutionWorkflowService', 'WorkflowContext']
