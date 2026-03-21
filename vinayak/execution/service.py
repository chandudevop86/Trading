from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.orm import Session

from vinayak.db.models.execution import ExecutionRecord
from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.db.repositories.execution_audit_log_repository import ExecutionAuditLogRepository
from vinayak.db.repositories.execution_repository import ExecutionRepository
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.execution.broker.adapter_result import ExecutionAdapterResult
from vinayak.execution.broker.dhan_client import DhanClient, DhanClientConfigError, DhanClientRequestError
from vinayak.execution.broker.payload_builder import build_dhan_order_request
from vinayak.execution.broker.response_mapper import map_dhan_response
from vinayak.execution.reviewed_trade_service import ReviewedTradeService


@dataclass
class ExecutionCreateCommand:
    mode: str
    broker: str
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    status: str | None = None
    executed_price: float | None = None


class PaperExecutionAdapter:
    def execute(
        self,
        command: ExecutionCreateCommand,
        reviewed_trade: ReviewedTradeRecord | None,
        signal: SignalRecord | None,
    ) -> ExecutionAdapterResult:
        reference_price = command.executed_price
        if reference_price is None and reviewed_trade is not None:
            reference_price = reviewed_trade.entry_price
        if reference_price is None and signal is not None:
            reference_price = signal.entry_price
        return ExecutionAdapterResult(
            broker=command.broker or 'PAPER_SIM',
            status=command.status or 'FILLED',
            executed_price=reference_price,
            executed_at=datetime.now(UTC),
            broker_reference=f'PAPER-{uuid4().hex[:10].upper()}',
            notes='Paper execution adapter simulated the fill.',
        )


class LiveExecutionAdapter:
    def __init__(self) -> None:
        self.client = DhanClient(
            client_id=os.getenv('DHAN_CLIENT_ID'),
            access_token=os.getenv('DHAN_ACCESS_TOKEN'),
        )

    def execute(
        self,
        command: ExecutionCreateCommand,
        reviewed_trade: ReviewedTradeRecord | None,
        signal: SignalRecord | None,
    ) -> ExecutionAdapterResult:
        reference_price = command.executed_price
        if reference_price is None and reviewed_trade is not None:
            reference_price = reviewed_trade.entry_price
        if reference_price is None and signal is not None:
            reference_price = signal.entry_price

        if not self.client.is_ready():
            return ExecutionAdapterResult(
                broker=command.broker,
                status=command.status or 'BLOCKED',
                executed_price=reference_price,
                executed_at=None,
                broker_reference=None,
                notes='Live adapter blocked routing because Dhan credentials are missing.',
                audit_request_payload={'symbol': reviewed_trade.symbol if reviewed_trade is not None else (signal.symbol if signal is not None else '')},
                audit_response_payload={'error': 'Dhan credentials are missing.'},
            )

        try:
            order_request = build_dhan_order_request(
                reviewed_trade=reviewed_trade,
                signal=signal,
                fallback_price=reference_price,
            )
            request_payload = order_request.to_payload()
        except ValueError as exc:
            return ExecutionAdapterResult(
                broker=command.broker,
                status='BLOCKED',
                executed_price=reference_price,
                executed_at=None,
                broker_reference=None,
                notes=f'Live adapter blocked routing because order preparation failed: {exc}',
                audit_request_payload={'symbol': reviewed_trade.symbol if reviewed_trade is not None else (signal.symbol if signal is not None else '')},
                audit_response_payload={'error': str(exc)},
            )

        try:
            route_result = self.client.place_order(order_request)
        except (DhanClientConfigError, DhanClientRequestError) as exc:
            return ExecutionAdapterResult(
                broker=command.broker,
                status='BLOCKED',
                executed_price=reference_price,
                executed_at=None,
                broker_reference=None,
                notes=f'Live adapter blocked routing because Dhan order placement failed: {exc}',
                audit_request_payload=request_payload,
                audit_response_payload={'error': str(exc)},
            )

        mapped = map_dhan_response(
            route_result,
            broker=command.broker,
            fallback_status=command.status or 'PENDING_LIVE_ROUTE',
            fallback_price=reference_price,
        )
        if mapped.audit_request_payload is None:
            mapped.audit_request_payload = request_payload
        return mapped


class ExecutionService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.execution_repository = ExecutionRepository(session)
        self.execution_audit_log_repository = ExecutionAuditLogRepository(session)
        self.reviewed_trade_repository = ReviewedTradeRepository(session)
        self.signal_repository = SignalRepository(session)
        self.reviewed_trade_service = ReviewedTradeService(session)
        self.paper_adapter = PaperExecutionAdapter()
        self.live_adapter = LiveExecutionAdapter()

    def list_executions(self) -> list[ExecutionRecord]:
        return self.execution_repository.list_executions()

    def create_execution(self, command: ExecutionCreateCommand) -> ExecutionRecord:
        reviewed_trade = None
        if command.reviewed_trade_id is not None:
            reviewed_trade = self.reviewed_trade_repository.get_reviewed_trade(command.reviewed_trade_id)
            if reviewed_trade is None:
                raise ValueError(f'Reviewed trade {command.reviewed_trade_id} was not found.')
            if reviewed_trade.status.upper() != 'APPROVED':
                raise ValueError(
                    f'Reviewed trade {command.reviewed_trade_id} must be APPROVED before execution. Current status: {reviewed_trade.status}.'
                )

        signal_id = command.signal_id
        if signal_id is None and reviewed_trade is not None:
            signal_id = reviewed_trade.signal_id

        signal = None
        if signal_id is not None:
            signal = self.signal_repository.get_signal(signal_id)
            if signal is None:
                raise ValueError(f'Signal {signal_id} was not found.')

        mode = command.mode.upper()
        adapter = self.live_adapter if mode == 'LIVE' else self.paper_adapter
        result = adapter.execute(
            command=ExecutionCreateCommand(
                mode=mode,
                broker=command.broker,
                signal_id=signal_id,
                reviewed_trade_id=command.reviewed_trade_id,
                status=command.status,
                executed_price=command.executed_price,
            ),
            reviewed_trade=reviewed_trade,
            signal=signal,
        )

        record = self.execution_repository.create_execution(
            signal_id=signal_id,
            reviewed_trade_id=command.reviewed_trade_id,
            mode=mode,
            broker=result.broker,
            status=result.status,
            executed_price=result.executed_price,
            executed_at=result.executed_at,
            broker_reference=result.broker_reference,
            notes=result.notes,
        )
        if mode == 'LIVE' and result.audit_request_payload is not None:
            self.execution_audit_log_repository.create_audit_log(
                execution_id=record.id,
                broker=result.broker,
                request_payload=result.audit_request_payload,
                response_payload=result.audit_response_payload,
                status=result.status,
            )
        if reviewed_trade is not None and result.status not in {'BLOCKED', 'REJECTED'}:
            self.reviewed_trade_service.mark_executed(
                reviewed_trade.id,
                notes=f'Execution recorded via {mode} mode.',
            )
        self.session.commit()
        self.session.refresh(record)
        return record
