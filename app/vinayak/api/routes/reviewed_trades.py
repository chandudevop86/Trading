from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from vinayak.api.dependencies.admin_auth import require_admin_session
from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import ReviewedTradeListResponse, ReviewedTradeResponse
from vinayak.api.schemas.strategy import ReviewedTradeCreateRequest, ReviewedTradeStatusUpdateRequest
from vinayak.execution.reviewed_trade_service import ReviewedTradeCreateCommand, ReviewedTradeStatusUpdateCommand
from vinayak.execution.runtime import build_execution_facade


router = APIRouter(prefix='/reviewed-trades', tags=['reviewed-trades'], dependencies=[Depends(require_admin_session)])


def _execution_facade(db: Session):
    return build_execution_facade(db)


def _to_response(record) -> ReviewedTradeResponse:
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


@router.get('', response_model=ReviewedTradeListResponse)
def list_reviewed_trades(db: Session = Depends(get_db)) -> ReviewedTradeListResponse:
    records = _execution_facade(db).list_reviewed_trades()
    reviewed_trades = [_to_response(record) for record in records]
    return ReviewedTradeListResponse(total=len(reviewed_trades), reviewed_trades=reviewed_trades)


@router.post('', response_model=ReviewedTradeResponse)
def create_reviewed_trade(request: ReviewedTradeCreateRequest, db: Session = Depends(get_db)) -> ReviewedTradeResponse:
    try:
        record = _execution_facade(db).create_reviewed_trade(
            ReviewedTradeCreateCommand(
                signal_id=request.signal_id,
                strategy_name=request.strategy_name,
                symbol=request.symbol,
                side=request.side,
                entry_price=request.entry_price,
                stop_loss=request.stop_loss,
                target_price=request.target_price,
                quantity=request.quantity,
                lots=request.lots,
                status=request.status,
                notes=request.notes,
            )
        )
    except ValueError as exc:
        raise HTTPException(status_code=422 if 'Missing reviewed trade fields' in str(exc) or 'Unsupported' in str(exc) else 404, detail=str(exc)) from exc
    return _to_response(record)


@router.patch('/{reviewed_trade_id}', response_model=ReviewedTradeResponse)
def update_reviewed_trade_status(
    reviewed_trade_id: int,
    request: ReviewedTradeStatusUpdateRequest,
    db: Session = Depends(get_db),
) -> ReviewedTradeResponse:
    try:
        record = _execution_facade(db).update_reviewed_trade_status(
            ReviewedTradeStatusUpdateCommand(
                reviewed_trade_id=reviewed_trade_id,
                status=request.status,
                notes=request.notes,
                quantity=request.quantity,
                lots=request.lots,
            )
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if 'was not found' in message else 422
        raise HTTPException(status_code=status_code, detail=message) from exc
    return _to_response(record)
