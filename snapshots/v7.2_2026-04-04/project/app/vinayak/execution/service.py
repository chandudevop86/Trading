from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Iterable, Protocol

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.repositories.execution_audit_log_repository import (
    ExecutionAuditLogRepository,
)
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.live_execution_adapter import LiveExecutionAdapter
from vinayak.execution.outbox_service import OutboxService
from vinayak.execution.paper_execution_adapter import PaperExecutionAdapter
from vinayak.execution.reviewed_trade_service import ReviewedTradeService
from vinayak.messaging.events import (
    EVENT_TRADE_EXECUTED,
    EVENT_TRADE_EXECUTE_REQUESTED,
)
from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric


_SUCCESS_STATUSES = {'FILLED', 'EXECUTED', 'ACCEPTED', 'SENT'}
_BLOCKED_STATUSES = {'BLOCKED', 'REJECTED', 'SKIPPED'}
_FAILURE_STATUSES = {'FAILED'}


class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    BLOCKED = "BLOCKED"
    SKIPPED = "SKIPPED"
    EXECUTED = "EXECUTED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


@dataclass(slots=True)
class ExecutionBatchItem:
    trade_id: str
    strategy_name: str
    symbol: str
    broker: str
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    status: str = ExecutionStatus.PENDING
    executed_price: float | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, trade: dict[str, Any]) -> "ExecutionBatchItem":
        return cls(
            trade_id=str(trade.get("trade_id", "")),
            strategy_name=str(trade.get("strategy_name", "")),
            symbol=str(trade.get("symbol", "")),
            broker=str(trade.get("broker", "")),
            signal_id=trade.get("signal_id"),
            reviewed_trade_id=trade.get("reviewed_trade_id"),
            status=str(trade.get("status", ExecutionStatus.PENDING)),
            executed_price=trade.get("executed_price"),
            payload=trade,
        )


@dataclass(slots=True)
class ExecutionDecision:
    trade_id: str
    strategy_name: str
    symbol: str
    status: str
    mode: str
    block_reasons: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    execution_id: int | None = None
    broker_reference: str | None = None


@dataclass(slots=True)
class ExecutionBatchResult:
    decisions: list[ExecutionDecision] = field(default_factory=list)

    def add(self, decision: ExecutionDecision) -> None:
        self.decisions.append(decision)

    @property
    def total(self) -> int:
        return len(self.decisions)

    @property
    def executed_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.EXECUTED)

    @property
    def blocked_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.BLOCKED)

    @property
    def skipped_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.SKIPPED)

    @property
    def rejected_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.REJECTED)

    @property
    def failed_count(self) -> int:
        return sum(1 for d in self.decisions if d.status == ExecutionStatus.FAILED)


class AdapterResultLike(Protocol):
    broker: str
    status: str
    executed_price: float | None
    executed_at: Any
    broker_reference: str | None
    notes: str | None
    audit_request_payload: dict[str, Any] | None
    audit_response_payload: dict[str, Any] | None


class ExecutionService:
    def __init__(
        self,
        session: Session,
        *,
        execution_repository: ExecutionRepository | None = None,
        execution_audit_log_repository: ExecutionAuditLogRepository | None = None,
        reviewed_trade_repository: ReviewedTradeRepository | None = None,
        signal_repository: SignalRepository | None = None,
        reviewed_trade_service: ReviewedTradeService | None = None,
        paper_adapter: PaperExecutionAdapter | None = None,
        live_adapter: LiveExecutionAdapter | None = None,
        outbox: OutboxService | None = None,
    ) -> None:
        self.session = session
        self.execution_repository = execution_repository or ExecutionRepository(session)
        self.execution_audit_log_repository = (
            execution_audit_log_repository or ExecutionAuditLogRepository(session)
        )
        self.reviewed_trade_repository = (
            reviewed_trade_repository or ReviewedTradeRepository(session)
        )
        self.signal_repository = signal_repository or SignalRepository(session)
        self.reviewed_trade_service = reviewed_trade_service or ReviewedTradeService(session)
        self.paper_adapter = paper_adapter or PaperExecutionAdapter()
        self.live_adapter = live_adapter or LiveExecutionAdapter()
        self.outbox = outbox or OutboxService(session)

    def list_executions(self) -> list[ExecutionRecord]:
        return self.execution_repository.list_executions()

    def create_execution(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        mode = self._normalize_mode(command.mode)
        broker = self._normalize_broker(command.broker, mode)
        requested_status = self._normalize_requested_status(command.status, mode)
        reviewed_trade, signal_id, signal = self._resolve_execution_dependencies(command)
        trade_symbol = str(getattr(reviewed_trade, 'symbol', '') or getattr(signal, 'symbol', '') or command.symbol or '')
        trade_strategy = str(
            getattr(reviewed_trade, 'strategy_name', '')
            or getattr(signal, 'strategy_name', '')
            or command.strategy_name
            or ''
        )

        increment_metric('execution_attempt_total', 1)
        log_event(
            component='execution_service',
            event_name='execution_attempt',
            symbol=trade_symbol,
            strategy=trade_strategy,
            severity='INFO',
            message='Execution attempt started',
            context_json={
                'mode': mode,
                'broker': broker,
                'reviewed_trade_id': command.reviewed_trade_id,
                'signal_id': signal_id,
                'trade_id': command.trade_id,
            },
        )
        self.outbox.enqueue(
            event_name=EVENT_TRADE_EXECUTE_REQUESTED,
            payload={
                'mode': mode,
                'broker': broker,
                'signal_id': signal_id,
                'reviewed_trade_id': command.reviewed_trade_id,
                'trade_id': command.trade_id,
            },
            source='execution_service',
        )

        try:
            record = self.execution_repository.create_execution(
                signal_id=signal_id,
                reviewed_trade_id=command.reviewed_trade_id,
                mode=mode,
                broker=broker,
                status=ExecutionStatus.PENDING,
                executed_price=None,
                executed_at=None,
                broker_reference=None,
                notes='Execution claim reserved before adapter dispatch.',
            )
        except IntegrityError as exc:
            self.session.rollback()
            increment_metric('execution_blocked_total', 1)
            increment_metric('duplicate_execution_block_total', 1)
            duplicate_reason, duplicate_message = self._map_duplicate_integrity_error(
                exc,
                mode=mode,
                broker=broker,
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=None,
            )
            self._log_blocked_execution(
                reason=duplicate_reason,
                mode=mode,
                broker=broker,
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=None,
                symbol=trade_symbol,
                strategy=trade_strategy,
                trade_id=command.trade_id,
                message='Duplicate execution blocked while reserving execution claim',
            )
            raise ValueError(duplicate_message) from exc

        adapter = self._get_adapter(mode)
        try:
            adapter_result = adapter.execute(
                command=ExecutionCreateCommand(
                    mode=mode,
                    broker=broker,
                    signal_id=signal_id,
                    reviewed_trade_id=command.reviewed_trade_id,
                    trade_id=command.trade_id,
                    strategy_name=trade_strategy,
                    symbol=trade_symbol,
                    side=str(command.side or getattr(reviewed_trade, 'side', '') or '').upper(),
                    entry_price=command.entry_price,
                    stop_loss=command.stop_loss,
                    target_price=command.target_price,
                    quantity=command.quantity,
                    validation_status=command.validation_status,
                    reviewed_trade_status=command.reviewed_trade_status,
                    status=requested_status,
                    executed_price=command.executed_price,
                    metadata=dict(command.metadata or {}),
                ),
                reviewed_trade=reviewed_trade,
                signal=signal,
            )
        except Exception as exc:
            increment_metric('execution_failed_total', 1)
            self.execution_repository.update_execution(
                record,
                broker=broker,
                status=ExecutionStatus.FAILED,
                executed_price=None,
                executed_at=datetime.now(UTC),
                broker_reference=None,
                notes=f'Adapter failure before execution persistence: {exc}',
            )
            self.session.commit()
            self.session.refresh(record)
            log_exception(
                component='execution_service',
                event_name='execution_adapter_failed',
                exc=exc,
                symbol=trade_symbol,
                strategy=trade_strategy,
                message='Execution adapter failed after claim reservation',
                context_json={
                    'mode': mode,
                    'broker': broker,
                    'reviewed_trade_id': command.reviewed_trade_id,
                    'signal_id': signal_id,
                    'trade_id': command.trade_id,
                    'execution_id': record.id,
                },
            )
            raise

        self._validate_adapter_result(adapter_result)

        try:
            record = self.execution_repository.update_execution(
                record,
                broker=adapter_result.broker,
                status=adapter_result.status,
                executed_price=adapter_result.executed_price,
                executed_at=adapter_result.executed_at,
                broker_reference=adapter_result.broker_reference,
                notes=adapter_result.notes,
            )

            if mode == 'LIVE' and adapter_result.audit_request_payload is not None:
                self.execution_audit_log_repository.create_audit_log(
                    execution_id=record.id,
                    broker=adapter_result.broker,
                    request_payload=adapter_result.audit_request_payload,
                    response_payload=adapter_result.audit_response_payload,
                    status=adapter_result.status,
                )

            if reviewed_trade is not None and str(adapter_result.status).upper() not in {
                ExecutionStatus.BLOCKED,
                ExecutionStatus.REJECTED,
                ExecutionStatus.FAILED,
            }:
                self.reviewed_trade_service.mark_executed(
                    reviewed_trade.id,
                    notes=f'Execution recorded via {mode} mode.',
                    auto_commit=False,
                )

            self.outbox.enqueue(
                event_name=EVENT_TRADE_EXECUTED,
                payload={
                    'execution_id': record.id,
                    'signal_id': record.signal_id,
                    'reviewed_trade_id': record.reviewed_trade_id,
                    'mode': record.mode,
                    'broker': record.broker,
                    'status': record.status,
                    'broker_reference': record.broker_reference,
                },
                source='execution_service',
            )

            status_upper = str(record.status or '').upper()
            if status_upper in _SUCCESS_STATUSES:
                increment_metric('execution_success_total', 1)
                severity = 'INFO'
                message = 'Execution recorded successfully'
            elif status_upper in _BLOCKED_STATUSES:
                increment_metric('execution_blocked_total', 1)
                severity = 'WARNING'
                message = 'Execution recorded in blocked/non-fill state'
            else:
                increment_metric('execution_failed_total', 1)
                severity = 'ERROR'
                message = 'Execution recorded in failed state'

            log_event(
                component='execution_service',
                event_name='execution_result',
                symbol=trade_symbol,
                strategy=trade_strategy,
                severity=severity,
                message=message,
                context_json={
                    'execution_id': record.id,
                    'mode': record.mode,
                    'broker': record.broker,
                    'status': record.status,
                    'reviewed_trade_id': record.reviewed_trade_id,
                    'signal_id': record.signal_id,
                    'broker_reference': record.broker_reference,
                },
            )

            self.session.commit()
            self.session.refresh(record)
            return record

        except IntegrityError as exc:
            self.session.rollback()
            increment_metric('execution_blocked_total', 1)
            increment_metric('duplicate_execution_block_total', 1)
            duplicate_reason, duplicate_message = self._map_duplicate_integrity_error(
                exc,
                mode=mode,
                broker=str(adapter_result.broker or broker),
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=adapter_result.broker_reference,
            )
            self._log_blocked_execution(
                reason=duplicate_reason,
                mode=mode,
                broker=str(adapter_result.broker or broker),
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=adapter_result.broker_reference,
                symbol=trade_symbol,
                strategy=trade_strategy,
                trade_id=command.trade_id,
                message='Duplicate execution blocked by database constraint',
            )
            raise ValueError(duplicate_message) from exc
        except Exception as exc:
            self.session.rollback()
            increment_metric('execution_failed_total', 1)
            log_exception(
                component='execution_service',
                event_name='execution_persist_failed',
                exc=exc,
                symbol=trade_symbol,
                strategy=trade_strategy,
                message='Execution persistence failed',
                context_json={
                    'mode': mode,
                    'broker': broker,
                    'reviewed_trade_id': command.reviewed_trade_id,
                    'signal_id': signal_id,
                    'trade_id': command.trade_id,
                },
            )
            raise

    def execute_batch(
        self,
        trades: Iterable[ExecutionBatchItem | dict[str, Any]],
        *,
        mode: str = 'PAPER',
        context: dict[str, Any] | None = None,
        continue_on_error: bool = True,
    ) -> ExecutionBatchResult:
        result = ExecutionBatchResult()
        normalized_mode = self._normalize_mode(mode)
        ctx = context or {}

        for raw_item in trades:
            item = raw_item if isinstance(raw_item, ExecutionBatchItem) else ExecutionBatchItem.from_dict(raw_item)

            try:
                reasons = self.reviewed_trade_service.get_block_reasons(item.payload, context=ctx)
                if reasons:
                    self._log_blocked_execution(
                        reason='execution_gate_blocked',
                        mode=normalized_mode,
                        broker=item.broker,
                        reviewed_trade_id=item.reviewed_trade_id,
                        signal_id=item.signal_id,
                        broker_reference=None,
                        symbol=item.symbol,
                        strategy=item.strategy_name,
                        trade_id=item.trade_id,
                        message='Batch execution blocked before service execution',
                        block_reasons=reasons,
                    )
                    result.add(
                        ExecutionDecision(
                            trade_id=item.trade_id,
                            strategy_name=item.strategy_name,
                            symbol=item.symbol,
                            status=ExecutionStatus.BLOCKED,
                            mode=normalized_mode,
                            block_reasons=[str(r) for r in reasons],
                            payload=item.payload,
                        )
                    )
                    continue

                command = ExecutionCreateCommand(
                    mode=normalized_mode,
                    broker=item.broker,
                    signal_id=item.signal_id,
                    reviewed_trade_id=item.reviewed_trade_id,
                    trade_id=item.trade_id,
                    strategy_name=item.strategy_name,
                    symbol=item.symbol,
                    status=item.status,
                    executed_price=item.executed_price,
                    metadata=dict(item.payload or {}),
                )

                record = self.create_execution(command)
                record_status = str(record.status).upper()
                mapped_status = (
                    record_status
                    if record_status in {
                        ExecutionStatus.EXECUTED,
                        ExecutionStatus.BLOCKED,
                        ExecutionStatus.SKIPPED,
                        ExecutionStatus.REJECTED,
                        ExecutionStatus.FAILED,
                    }
                    else ExecutionStatus.EXECUTED
                )

                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=mapped_status,
                        mode=normalized_mode,
                        block_reasons=[],
                        payload={**item.payload, 'execution_record_id': record.id},
                        execution_id=record.id,
                        broker_reference=record.broker_reference,
                    )
                )

            except ValueError as exc:
                message = str(exc)
                lowered = message.lower()

                if 'empty result' in lowered or 'missing adapter result' in lowered:
                    status = ExecutionStatus.SKIPPED
                    reasons = ['gateway_returned_empty_result']
                else:
                    status = ExecutionStatus.BLOCKED
                    reasons = [message]

                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=status,
                        mode=normalized_mode,
                        block_reasons=reasons,
                        payload=item.payload,
                    )
                )

                if not continue_on_error:
                    break

            except Exception as exc:
                self.session.rollback()
                result.add(
                    ExecutionDecision(
                        trade_id=item.trade_id,
                        strategy_name=item.strategy_name,
                        symbol=item.symbol,
                        status=ExecutionStatus.FAILED,
                        mode=normalized_mode,
                        block_reasons=[f'unexpected_error: {exc}'],
                        payload=item.payload,
                    )
                )

                if not continue_on_error:
                    break

        return result

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or '').upper().strip()
        if normalized not in {'PAPER', 'LIVE'}:
            raise ValueError(f'Unsupported execution mode: {mode}')
        return normalized

    def _normalize_broker(self, broker: str, mode: str) -> str:
        normalized = str(broker or '').upper().strip()
        if mode == 'PAPER':
            if normalized in {'SIM', 'PAPER'}:
                return 'SIM'
            raise ValueError(
                f'Paper execution only supports broker SIM. Received broker: {broker}.'
            )
        if normalized != 'DHAN':
            raise ValueError(
                f'Live execution only supports broker DHAN. Received broker: {broker}.'
            )
        return normalized

    def _normalize_requested_status(self, status: str | None, mode: str) -> str | None:
        normalized = str(status or '').upper().strip()
        if mode == 'PAPER':
            if normalized in {'', 'FILLED', 'EXECUTED'}:
                return 'FILLED'
            raise ValueError(
                f'Paper execution status override must be FILLED/EXECUTED or empty. Received status: {status}.'
            )
        if normalized:
            raise ValueError('Live execution status override is not allowed. Broker response must determine status.')
        return None

    def _get_adapter(self, mode: str) -> PaperExecutionAdapter | LiveExecutionAdapter:
        return self.live_adapter if mode == 'LIVE' else self.paper_adapter

    def _resolve_execution_dependencies(
        self,
        command: ExecutionCreateCommand,
    ) -> tuple[ReviewedTradeRecord, int | None, Any | None]:
        if command.reviewed_trade_id is None:
            raise ValueError(
                'reviewed_trade_id is required. Real execution must come from an approved reviewed trade.'
            )

        reviewed_trade = self.reviewed_trade_repository.get_reviewed_trade(
            command.reviewed_trade_id
        )
        if reviewed_trade is None:
            raise ValueError(
                f'Reviewed trade {command.reviewed_trade_id} was not found.'
            )
        if str(reviewed_trade.status).upper() != 'APPROVED':
            raise ValueError(
                f'Reviewed trade {command.reviewed_trade_id} must be APPROVED before execution. '
                f'Current status: {reviewed_trade.status}.'
            )

        signal_id = reviewed_trade.signal_id
        if command.signal_id is not None and signal_id is not None and int(command.signal_id) != int(signal_id):
            raise ValueError(
                f'Execution signal_id {command.signal_id} does not match reviewed trade signal_id {signal_id}.'
            )

        signal = None
        if signal_id is not None:
            signal = self.signal_repository.get_signal(signal_id)
            if signal is None:
                raise ValueError(f'Signal {signal_id} was not found.')

        return reviewed_trade, signal_id, signal

    def _ensure_no_duplicate_execution(
        self,
        *,
        mode: str,
        broker: str,
        reviewed_trade_id: int | None,
        signal_id: int | None,
        broker_reference: str | None,
        symbol: str,
        strategy: str,
        trade_id: str,
    ) -> None:
        duplicate_reason = ''
        existing = None

        if reviewed_trade_id is not None:
            existing = self.execution_repository.get_by_reviewed_trade_mode(
                reviewed_trade_id=int(reviewed_trade_id),
                mode=mode,
            )
            if existing is not None:
                duplicate_reason = 'duplicate_reviewed_trade_mode'

        if not duplicate_reason and signal_id is not None:
            existing = self.execution_repository.get_by_signal_mode(
                signal_id=int(signal_id),
                mode=mode,
            )
            if existing is not None:
                duplicate_reason = 'duplicate_signal_mode'

        normalized_reference = str(broker_reference or '').strip()
        normalized_broker = str(broker or '').strip().upper()
        if not duplicate_reason and normalized_broker and normalized_reference:
            existing = self.execution_repository.get_by_broker_reference(
                broker=normalized_broker,
                broker_reference=normalized_reference,
            )
            if existing is not None:
                duplicate_reason = 'duplicate_broker_reference'

        if duplicate_reason:
            increment_metric('execution_blocked_total', 1)
            increment_metric('duplicate_execution_block_total', 1)
            self._log_blocked_execution(
                reason=duplicate_reason,
                mode=mode,
                broker=normalized_broker,
                reviewed_trade_id=reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=normalized_reference,
                symbol=symbol,
                strategy=strategy,
                trade_id=trade_id,
                message='Duplicate execution blocked before persistence',
                existing_execution_id=getattr(existing, 'id', None),
            )
            if duplicate_reason == 'duplicate_reviewed_trade_mode':
                raise ValueError(
                    f'Duplicate execution blocked for reviewed trade {reviewed_trade_id} in {mode} mode.'
                )
            if duplicate_reason == 'duplicate_signal_mode':
                raise ValueError(
                    f'Duplicate execution blocked for signal {signal_id} in {mode} mode.'
                )
            raise ValueError(
                f'Duplicate execution blocked for broker {normalized_broker} broker_reference {normalized_reference}.'
            )
    def _map_duplicate_integrity_error(
        self,
        exc: IntegrityError,
        *,
        mode: str,
        broker: str,
        reviewed_trade_id: int | None,
        signal_id: int | None,
        broker_reference: str | None,
    ) -> tuple[str, str]:
        error_text = str(getattr(exc, 'orig', exc) or exc)
        if 'uq_execution_broker_reference' in error_text or 'executions.broker, executions.broker_reference' in error_text:
            normalized_broker = str(broker or '').strip().upper()
            normalized_reference = str(broker_reference or '').strip()
            return (
                'duplicate_broker_reference',
                f'Duplicate execution blocked for broker {normalized_broker} broker_reference {normalized_reference}.',
            )
        if 'uq_execution_signal_mode' in error_text or 'executions.signal_id, executions.mode' in error_text:
            return (
                'duplicate_signal_mode',
                f'Duplicate execution blocked for signal {signal_id} in {mode} mode.',
            )
        return (
            'duplicate_reviewed_trade_mode',
            f'Duplicate execution blocked for reviewed trade {reviewed_trade_id} in {mode} mode.',
        )
    def _validate_adapter_result(self, result: AdapterResultLike | None) -> None:
        if result is None:
            raise ValueError('Execution adapter returned empty result.')

        missing_fields: list[str] = []
        if not getattr(result, 'broker', None):
            missing_fields.append('broker')
        if not getattr(result, 'status', None):
            missing_fields.append('status')
        if getattr(result, 'executed_at', None) is None:
            missing_fields.append('executed_at')

        if missing_fields:
            raise ValueError(
                f"Execution adapter returned invalid result. Missing fields: {', '.join(missing_fields)}"
            )

        normalized_status = str(getattr(result, 'status', '') or '').upper().strip()
        executed_price = getattr(result, 'executed_price', None)
        broker_reference = str(getattr(result, 'broker_reference', '') or '').strip()

        if normalized_status in {'FILLED', 'EXECUTED'}:
            if executed_price is None or float(executed_price) <= 0:
                raise ValueError('Execution adapter returned invalid result. Filled executions must include executed_price.')
        if normalized_status in _SUCCESS_STATUSES and not broker_reference:
            raise ValueError('Execution adapter returned invalid result. Successful executions must include broker_reference.')

    def _log_blocked_execution(
        self,
        *,
        reason: str,
        mode: str,
        broker: str,
        reviewed_trade_id: int | None,
        signal_id: int | None,
        broker_reference: str | None,
        symbol: str,
        strategy: str,
        trade_id: str,
        message: str,
        block_reasons: list[str] | None = None,
        existing_execution_id: int | None = None,
    ) -> None:
        log_event(
            component='execution_service',
            event_name='execution_blocked',
            symbol=symbol,
            strategy=strategy,
            severity='WARNING',
            message=message,
            context_json={
                'reason': reason,
                'block_reasons': list(block_reasons or []),
                'mode': mode,
                'broker': broker,
                'reviewed_trade_id': reviewed_trade_id,
                'signal_id': signal_id,
                'broker_reference': broker_reference,
                'trade_id': trade_id,
                'existing_execution_id': existing_execution_id,
            },
        )


__all__ = [
    'ExecutionBatchItem',
    'ExecutionBatchResult',
    'ExecutionCreateCommand',
    'ExecutionDecision',
    'ExecutionService',
    'ExecutionStatus',
]




