from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from vinayak.core.config import get_settings
from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.repositories.production import ProductionReadRepository, SqlAlchemyAuditRepository, SqlAlchemyExecutionRepository
from vinayak.domain.exceptions import DuplicateExecutionRequestError
from vinayak.domain.models import (
    ExecutionFailureReason,
    ExecutionMode,
    ExecutionRequest,
    ExecutionResult,
    ExecutionSide,
    ExecutionStatus,
    RiskConfig,
    Timeframe,
    TradeSignal,
    TradeSignalStatus,
    TradeSignalType,
)
from vinayak.domain.statuses import WorkflowActor
from vinayak.execution.broker.adapter_result import ExecutionAdapterResult
from vinayak.execution.canonical_service import CanonicalExecutionService
from vinayak.execution.commands import ExecutionCreateCommand
from vinayak.execution.guard import ExecutionGuard
from vinayak.execution.service import ExecutionService, _BLOCKED_STATUSES, _FAILURE_STATUSES, _SUCCESS_STATUSES
from vinayak.observability.observability_logger import log_event
from vinayak.observability.observability_metrics import increment_metric


@dataclass(slots=True)
class _AdapterBridge:
    mode: str
    command: ExecutionCreateCommand
    reviewed_trade: Any
    signal: Any | None
    adapter: Any
    last_adapter_result: ExecutionAdapterResult | None = None

    def place_order(self, request: ExecutionRequest) -> ExecutionResult:
        try:
            result = self.adapter.execute(command=self.command, reviewed_trade=self.reviewed_trade, signal=self.signal)
        except Exception as exc:
            result = ExecutionAdapterResult(
                broker=str(self.command.broker or ('DHAN' if self.mode == 'LIVE' else 'SIM')),
                status='FAILED',
                executed_price=self.command.executed_price,
                executed_at=datetime.now(UTC),
                broker_reference=None,
                notes=str(exc),
                audit_request_payload={'broker': str(self.command.broker or '')},
                audit_response_payload={'error': str(exc)},
            )
        self.last_adapter_result = result
        return ExecutionResult(
            request_id=request.request_id,
            status=self._status(result.status),
            failure_reason=self._reason(result),
            processed_at=result.executed_at or datetime.now(UTC),
            order_reference=result.broker_reference,
            message=str(result.notes or ''),
        )

    def _status(self, value: str | None) -> ExecutionStatus:
        normalized = str(value or '').upper().strip()
        if normalized in {'FILLED', 'EXECUTED'}:
            return ExecutionStatus.EXECUTED
        if normalized == 'ACCEPTED':
            return ExecutionStatus.ACCEPTED
        if normalized in {'BLOCKED', 'REJECTED', 'SKIPPED'}:
            return ExecutionStatus.REJECTED
        return ExecutionStatus.FAILED

    def _reason(self, result: ExecutionAdapterResult) -> ExecutionFailureReason:
        normalized = str(result.status or '').upper().strip()
        notes = str(result.notes or '').lower()
        if normalized in {'FILLED', 'EXECUTED', 'ACCEPTED'}:
            return ExecutionFailureReason.NONE
        if 'duplicate' in notes:
            return ExecutionFailureReason.DUPLICATE_REQUEST
        if 'cooldown' in notes:
            return ExecutionFailureReason.COOLDOWN_ACTIVE
        if 'daily loss' in notes:
            return ExecutionFailureReason.DAILY_LOSS_LIMIT
        if 'session' in notes:
            return ExecutionFailureReason.SESSION_CLOSED
        return ExecutionFailureReason.LIVE_MODE_LOCKED if self.mode == 'LIVE' else ExecutionFailureReason.INVALID_SIGNAL


class ReviewedTradeExecutionWorkflow:
    """Single reviewed-trade-to-execution application workflow."""

    def __init__(self, session, *, execution_guard: ExecutionGuard, execution_service: ExecutionService | None = None, canonical_service: CanonicalExecutionService | None = None, read_repository: ProductionReadRepository | None = None) -> None:
        self.session = session
        self.execution_service = execution_service or ExecutionService(session)
        self.read_repository = read_repository or ProductionReadRepository(session)
        self.canonical_service = canonical_service or CanonicalExecutionService(
            execution_repository=SqlAlchemyExecutionRepository(session),
            audit_repository=SqlAlchemyAuditRepository(session),
            execution_guard=execution_guard,
        )

    def execute(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        mode = self.execution_service._normalize_mode(command.mode)
        broker = self.execution_service._normalize_broker(command.broker, mode)
        requested_status = self.execution_service._normalize_requested_status(command.status, mode)
        reviewed_trade, signal_id, signal = self.execution_service._resolve_execution_dependencies(command)
        block_reasons = self.execution_service._service_block_reasons(command, mode=mode, reviewed_trade=reviewed_trade, signal=signal)
        trade_symbol = str(getattr(reviewed_trade, 'symbol', '') or getattr(signal, 'symbol', '') or command.symbol or '')
        trade_strategy = str(getattr(reviewed_trade, 'strategy_name', '') or getattr(signal, 'strategy_name', '') or command.strategy_name or '')
        if block_reasons:
            increment_metric('execution_blocked_total', 1)
            self.execution_service._log_blocked_execution(
                reason='service_level_gate_blocked',
                mode=mode,
                broker=broker,
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=None,
                symbol=trade_symbol,
                strategy=trade_strategy,
                trade_id=command.trade_id,
                message='Execution blocked by service-level readiness or risk gates',
                block_reasons=block_reasons,
            )
            raise ValueError('Execution blocked by service-level gates: ' + ', '.join(block_reasons))
        try:
            reviewed_trade = self.execution_service.workflow.request_execution(reviewed_trade, mode=mode, broker=broker, actor=WorkflowActor.EXECUTION_WORKFLOW.value, metadata=dict(command.metadata or {}))
        except DuplicateExecutionRequestError as exc:
            increment_metric('execution_blocked_total', 1)
            increment_metric('duplicate_execution_block_total', 1)
            self.execution_service._log_blocked_execution(
                reason='duplicate_execution_request',
                mode=mode,
                broker=broker,
                reviewed_trade_id=command.reviewed_trade_id,
                signal_id=signal_id,
                broker_reference=None,
                symbol=trade_symbol,
                strategy=trade_strategy,
                trade_id=command.trade_id,
                message='Duplicate execution blocked before persistence',
            )
            raise ValueError(str(exc)) from exc
        increment_metric('execution_attempt_total', 1)
        bridge = _AdapterBridge(mode, command, reviewed_trade, signal, self.execution_service._get_adapter(mode))
        if mode == 'PAPER':
            self.canonical_service.paper_adapter = bridge
        else:
            self.canonical_service.live_adapter = bridge
        request = self._build_request(command, reviewed_trade, signal, mode)
        try:
            realized_pnl = self.read_repository.total_realized_pnl()
        except Exception:
            realized_pnl = Decimal('0')
        result = self.canonical_service.execute(request, daily_realized_pnl=realized_pnl)
        record = self.execution_service.execution_repository.create_execution(
            signal_id=signal_id,
            reviewed_trade_id=reviewed_trade.id,
            mode=mode,
            broker=str(getattr(bridge.last_adapter_result, 'broker', None) or broker),
            status=self._legacy_status(result, bridge.last_adapter_result, requested_status),
            executed_price=(bridge.last_adapter_result.executed_price if bridge.last_adapter_result else None) or (float(command.executed_price) if command.executed_price is not None else float(reviewed_trade.entry_price) if str(self._legacy_status(result, bridge.last_adapter_result, requested_status)).upper() in _SUCCESS_STATUSES else None),
            executed_at=getattr(bridge.last_adapter_result, 'executed_at', None) or result.processed_at,
            broker_reference=getattr(bridge.last_adapter_result, 'broker_reference', None) or result.order_reference,
            notes=str(getattr(bridge.last_adapter_result, 'notes', None) or result.message or ''),
        )
        if mode == 'LIVE' and bridge.last_adapter_result and bridge.last_adapter_result.audit_request_payload is not None:
            self.execution_service.execution_audit_log_repository.create_audit_log(
                execution_id=record.id,
                broker=str(bridge.last_adapter_result.broker or broker),
                request_payload=bridge.last_adapter_result.audit_request_payload,
                response_payload=bridge.last_adapter_result.audit_response_payload,
                status=str(bridge.last_adapter_result.status or record.status),
                entity_type='execution',
                entity_id=record.id,
                event_name='broker.adapter.audit',
            )
        self.execution_service.workflow.complete_execution(reviewed_trade, record, actor=WorkflowActor.EXECUTION_WORKFLOW.value, reason=str(getattr(bridge.last_adapter_result, 'notes', '') or result.message or ''), metadata=dict(command.metadata or {}))
        status_upper = str(record.status or '').upper()
        if status_upper in _SUCCESS_STATUSES:
            increment_metric('execution_success_total', 1)
        elif status_upper in _BLOCKED_STATUSES:
            increment_metric('execution_blocked_total', 1)
        else:
            increment_metric('execution_failed_total', 1)
        log_event(component='reviewed_trade_execution_workflow', event_name='execution_result', symbol=trade_symbol, strategy=trade_strategy, severity='INFO' if status_upper in _SUCCESS_STATUSES else 'WARNING', message='Reviewed-trade execution completed through canonical workflow', context_json={'execution_id': record.id, 'mode': record.mode, 'status': record.status, 'reviewed_trade_id': record.reviewed_trade_id, 'signal_id': record.signal_id})
        self.session.commit()
        self.session.refresh(record)
        return record

    def _build_request(self, command: ExecutionCreateCommand, reviewed_trade: Any, signal: Any | None, mode: str) -> ExecutionRequest:
        generated_at = self._aware(getattr(signal, 'signal_time', None) or getattr(reviewed_trade, 'created_at', None) or datetime.now(UTC))
        signal_uuid = uuid5(NAMESPACE_URL, f'reviewed-trade-signal:{reviewed_trade.id}:{getattr(signal, "id", "none")}')
        trade_signal = TradeSignal(
            signal_id=signal_uuid,
            idempotency_key=f'reviewed-trade-signal-{reviewed_trade.id}-{getattr(signal, "id", "none")}-entry',
            strategy_name=str(getattr(reviewed_trade, 'strategy_name', '') or getattr(signal, 'strategy_name', '') or command.strategy_name or 'REVIEWED_TRADE'),
            symbol=str(getattr(reviewed_trade, 'symbol', '') or getattr(signal, 'symbol', '') or command.symbol or ''),
            timeframe=self._timeframe((command.metadata or {}).get('timeframe') or (command.metadata or {}).get('interval')),
            signal_type=TradeSignalType.ENTRY,
            status=TradeSignalStatus.VALIDATED,
            generated_at=generated_at,
            candle_timestamp=generated_at,
            side=ExecutionSide.SELL if str(getattr(reviewed_trade, 'side', '') or command.side or '').upper() == 'SELL' else ExecutionSide.BUY,
            entry_price=Decimal(str(command.entry_price if command.entry_price is not None else getattr(reviewed_trade, 'entry_price', getattr(signal, 'entry_price', 0)) or 0)),
            stop_loss=Decimal(str(command.stop_loss if command.stop_loss is not None else getattr(reviewed_trade, 'stop_loss', getattr(signal, 'stop_loss', 0)) or 0)),
            target_price=Decimal(str(command.target_price if command.target_price is not None else getattr(reviewed_trade, 'target_price', getattr(signal, 'target_price', 0)) or 0)),
            quantity=Decimal(str(max(int(command.quantity or getattr(reviewed_trade, 'quantity', 1) or 1), 1))),
            confidence=Decimal('0.8'),
            rationale=str((command.metadata or {}).get('rationale') or getattr(reviewed_trade, 'notes', '') or 'reviewed_trade_execution'),
            metadata={'reviewed_trade_id': reviewed_trade.id, 'signal_id': getattr(signal, 'id', None), **dict(command.metadata or {})},
        )
        settings = get_settings()
        metadata = dict(command.metadata or {})
        return ExecutionRequest(
            idempotency_key=f'reviewed-trade-execution-{reviewed_trade.id}-{mode.lower()}',
            requested_at=self._aware(metadata.get('requested_at') or generated_at),
            mode=ExecutionMode.LIVE if mode == 'LIVE' else ExecutionMode.PAPER,
            signal=trade_signal,
            risk=RiskConfig(
                risk_per_trade_pct=Decimal(str(metadata.get('risk_per_trade_pct') or '1')),
                max_daily_loss_pct=Decimal(str(metadata.get('max_daily_loss') or metadata.get('max_daily_loss_pct') or '3')),
                max_trades_per_day=int(metadata.get('max_trades_per_day') or 5),
                cooldown_minutes=int(metadata.get('cooldown_minutes') or 15),
                allow_live_trading=bool(mode == 'LIVE' and settings.execution.live_trading_enabled),
                live_unlock_token_required=False,
            ),
            account_id=str(metadata.get('account_id') or ('live' if mode == 'LIVE' else 'paper')),
        )

    def _legacy_status(self, result: ExecutionResult, adapter_result: ExecutionAdapterResult | None, requested_status: str | None) -> str:
        if adapter_result is not None and str(adapter_result.status or '').strip():
            return str(adapter_result.status).upper().strip()
        if result.status == ExecutionStatus.EXECUTED:
            return str(requested_status or 'EXECUTED').upper().strip()
        if result.status == ExecutionStatus.ACCEPTED:
            return 'ACCEPTED'
        if result.status == ExecutionStatus.REJECTED:
            return 'BLOCKED'
        return 'FAILED'

    def _aware(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.replace(tzinfo=UTC) if value.tzinfo is None or value.utcoffset() is None else value.astimezone(UTC)
        return datetime.now(UTC)

    def _timeframe(self, value: Any) -> Timeframe:
        mapping = {'1m': Timeframe.M1, '5m': Timeframe.M5, '15m': Timeframe.M15, '30m': Timeframe.M30, '1h': Timeframe.H1, '1d': Timeframe.D1}
        return mapping.get(str(value or '').strip().lower(), Timeframe.M5)


__all__ = ['ReviewedTradeExecutionWorkflow']
