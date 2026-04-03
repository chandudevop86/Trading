from __future__ import annotations

from sqlalchemy.orm import Session

from vinayak.db.models.reviewed_trade import ReviewedTradeRecord


class ReviewedTradeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_reviewed_trade(
        self,
        strategy_name: str,
        symbol: str,
        side: str,
        entry_price: float,
        stop_loss: float,
        target_price: float,
        quantity: int = 1,
        lots: int = 1,
        status: str = 'REVIEWED',
        signal_id: int | None = None,
        notes: str | None = None,
    ) -> ReviewedTradeRecord:
        record = ReviewedTradeRecord(
            signal_id=signal_id,
            strategy_name=strategy_name,
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            quantity=quantity,
            lots=lots,
            status=status,
            notes=notes,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_reviewed_trade(self, reviewed_trade_id: int) -> ReviewedTradeRecord | None:
        return self.session.get(ReviewedTradeRecord, reviewed_trade_id)

    def update_reviewed_trade(
        self,
        record: ReviewedTradeRecord,
        *,
        status: str | None = None,
        notes: str | None = None,
        quantity: int | None = None,
        lots: int | None = None,
    ) -> ReviewedTradeRecord:
        if status is not None:
            record.status = status
        if notes is not None:
            record.notes = notes
        if quantity is not None:
            record.quantity = quantity
        if lots is not None:
            record.lots = lots
        self.session.add(record)
        self.session.flush()
        return record

    def list_reviewed_trades(self) -> list[ReviewedTradeRecord]:
        return list(self.session.query(ReviewedTradeRecord).order_by(ReviewedTradeRecord.id.desc()).all())
