from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import ReviewedTradeResponse, SignalListResponse, SignalResponse
from vinayak.api.schemas.strategy import SignalReviewCreateRequest
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.execution.reviewed_trade_service import ReviewedTradeService


router = APIRouter(prefix='/signals', tags=['signals'], dependencies=[Depends(require_admin_session)])


def _to_signal_response(record) -> SignalResponse:
    return SignalResponse(
        id=record.id,
        strategy_name=record.strategy_name,
        symbol=record.symbol,
        side=record.side,
        entry_price=record.entry_price,
        stop_loss=record.stop_loss,
        target_price=record.target_price,
        signal_time=record.signal_time,
        status=record.status,
        metadata={},
    )


def _to_reviewed_trade_response(record) -> ReviewedTradeResponse:
    return ReviewedTradeResponse(
        id=record.id,
        signal_id=record.signal_id,
        strategy_name=record.strategy_name,
        symbol=record.symbol,
        side=record.side,
        entry_price=record.entry_price,
        stop_loss=record.stop_loss,
        target_price=record.target_price,
        quantity=record.quantity,
        lots=record.lots,
        status=record.status,
        notes=record.notes,
        created_at=record.created_at,
    )


@router.get('', response_model=SignalListResponse)
def list_signals(db: Session = Depends(get_db)) -> SignalListResponse:
    repository = SignalRepository(db)
    records = repository.list_signals()
    signals = [_to_signal_response(record) for record in records]
    return SignalListResponse(total=len(signals), signals=signals)


@router.post('/{signal_id}/review', response_model=ReviewedTradeResponse)
def create_reviewed_trade_from_signal(
    signal_id: int,
    request: SignalReviewCreateRequest,
    db: Session = Depends(get_db),
) -> ReviewedTradeResponse:
    service = ReviewedTradeService(db)
    try:
        record = service.create_reviewed_trade_from_signal(
            signal_id=signal_id,
            quantity=request.quantity,
            lots=request.lots,
            status=request.status,
            notes=request.notes,
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if 'was not found' in message else 422
        raise HTTPException(status_code=status_code, detail=message) from exc
    return _to_reviewed_trade_response(record)
