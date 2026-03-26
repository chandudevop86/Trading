from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.db.repositories.reviewed_trade_repository import ReviewedTradeRepository
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.messaging.outbox import OutboxService
from vinayak.messaging.topics import EVENT_TRADE_REVIEWED

ALLOWED_REVIEWED_TRADE_STATUSES = {'REVIEWED', 'APPROVED', 'REJECTED', 'EXECUTED'}
ALLOWED_STATUS_TRANSITIONS = {
    'REVIEWED': {'APPROVED', 'REJECTED'},
    'APPROVED': {'REVIEWED', 'REJECTED', 'EXECUTED'},
    'REJECTED': {'REVIEWED'},
    'EXECUTED': set(),
}


@dataclass
class ReviewedTradeCreateCommand:
    signal_id: int | None = None
    strategy_name: str | None = None
    symbol: str | None = None
    side: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    quantity: int = 1
    lots: int = 1
    status: str = 'REVIEWED'
    notes: str | None = None


@dataclass
class ReviewedTradeStatusUpdateCommand:
    reviewed_trade_id: int
    status: str
    notes: str | None = None
    quantity: int | None = None
    lots: int | None = None


class ReviewedTradeService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.repository = ReviewedTradeRepository(session)
        self.signal_repository = SignalRepository(session)
        self.outbox = OutboxService(session)

    def list_reviewed_trades(self) -> list[ReviewedTradeRecord]:
        return self.repository.list_reviewed_trades()

    def get_reviewed_trade(self, reviewed_trade_id: int) -> ReviewedTradeRecord | None:
        return self.repository.get_reviewed_trade(reviewed_trade_id)

    def create_reviewed_trade(self, command: ReviewedTradeCreateCommand, *, auto_commit: bool = True) -> ReviewedTradeRecord:
        signal_record = self._get_signal_if_needed(command.signal_id)

        strategy_name = command.strategy_name or (signal_record.strategy_name if signal_record else None)
        symbol = command.symbol or (signal_record.symbol if signal_record else None)
        side = command.side or (signal_record.side if signal_record else None)
        entry_price = command.entry_price if command.entry_price is not None else (signal_record.entry_price if signal_record else None)
        stop_loss = command.stop_loss if command.stop_loss is not None else (signal_record.stop_loss if signal_record else None)
        target_price = command.target_price if command.target_price is not None else (signal_record.target_price if signal_record else None)

        missing_fields = [
            name
            for name, value in {
                'strategy_name': strategy_name,
                'symbol': symbol,
                'side': side,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'target_price': target_price,
            }.items()
            if value is None
        ]
        if missing_fields:
            raise ValueError(f'Missing reviewed trade fields: {", ".join(missing_fields)}')

        status = command.status.upper()
        if status not in ALLOWED_REVIEWED_TRADE_STATUSES:
            raise ValueError(f'Unsupported reviewed trade status: {status}')

        record = self.repository.create_reviewed_trade(
            signal_id=command.signal_id,
            strategy_name=str(strategy_name),
            symbol=str(symbol),
            side=str(side),
            entry_price=float(entry_price),
            stop_loss=float(stop_loss),
            target_price=float(target_price),
            quantity=command.quantity,
            lots=command.lots,
            status=status,
            notes=command.notes,
        )
        self._enqueue_review_event(record, action='created')
        return self._finalize(record, auto_commit=auto_commit)

    def create_reviewed_trade_from_signal(
        self,
        signal_id: int,
        quantity: int = 1,
        lots: int = 1,
        notes: str | None = None,
        status: str = 'REVIEWED',
    ) -> ReviewedTradeRecord:
        return self.create_reviewed_trade(
            ReviewedTradeCreateCommand(
                signal_id=signal_id,
                quantity=quantity,
                lots=lots,
                notes=notes,
                status=status,
            )
        )

    def update_reviewed_trade_status(self, command: ReviewedTradeStatusUpdateCommand, *, auto_commit: bool = True) -> ReviewedTradeRecord:
        record = self.repository.get_reviewed_trade(command.reviewed_trade_id)
        if record is None:
            raise ValueError(f'Reviewed trade {command.reviewed_trade_id} was not found.')

        next_status = command.status.upper()
        if next_status not in ALLOWED_REVIEWED_TRADE_STATUSES:
            raise ValueError(f'Unsupported reviewed trade status: {next_status}')

        current_status = record.status.upper()
        if next_status != current_status and next_status not in ALLOWED_STATUS_TRANSITIONS.get(current_status, set()):
            raise ValueError(f'Cannot move reviewed trade from {current_status} to {next_status}.')

        updated = self.repository.update_reviewed_trade(
            record,
            status=next_status,
            notes=command.notes,
            quantity=command.quantity,
            lots=command.lots,
        )
        self._enqueue_review_event(updated, action='status_updated')
        return self._finalize(updated, auto_commit=auto_commit)

    def mark_executed(self, reviewed_trade_id: int, notes: str | None = None, *, auto_commit: bool = True) -> ReviewedTradeRecord:
        return self.update_reviewed_trade_status(
            ReviewedTradeStatusUpdateCommand(
                reviewed_trade_id=reviewed_trade_id,
                status='EXECUTED',
                notes=notes,
            ),
            auto_commit=auto_commit,
        )

    def _get_signal_if_needed(self, signal_id: int | None) -> SignalRecord | None:
        if signal_id is None:
            return None
        signal_record = self.signal_repository.get_signal(signal_id)
        if signal_record is None:
            raise ValueError(f'Signal {signal_id} was not found.')
        return signal_record

    def _enqueue_review_event(self, record: ReviewedTradeRecord, *, action: str) -> None:
        self.outbox.enqueue(
            event_name=EVENT_TRADE_REVIEWED,
            payload={
                'action': action,
                'reviewed_trade_id': record.id,
                'signal_id': record.signal_id,
                'strategy_name': record.strategy_name,
                'symbol': record.symbol,
                'side': record.side,
                'status': record.status,
                'quantity': record.quantity,
                'lots': record.lots,
            },
            source='reviewed_trade_service',
        )

    def _finalize(self, record: ReviewedTradeRecord, *, auto_commit: bool) -> ReviewedTradeRecord:
        if auto_commit:
            self.session.commit()
            self.session.refresh(record)
        else:
            self.session.flush()
        return record
