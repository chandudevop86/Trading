from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from vinayak.api.dependencies.db import get_db
from vinayak.api.schemas.signal import BreakoutRunResponse, SignalResponse
from vinayak.api.schemas.strategy import BreakoutRunRequest, DemandSupplyRunRequest, MtfRunRequest, OneTradeDayRunRequest
from vinayak.db.repositories.signal_repository import SignalRepository
from vinayak.strategies.breakout.service import Candle, run_breakout_strategy
from vinayak.strategies.demand_supply.service import run_demand_supply_strategy
from vinayak.strategies.indicator.service import run_indicator_strategy
from vinayak.strategies.mtf.service import run_mtf_strategy
from vinayak.strategies.one_trade_day.service import run_one_trade_day_strategy


router = APIRouter(prefix='/strategies', tags=['strategies'])


def _persist_signals(db: Session, signals: list, save_signals: bool) -> int:
    persisted_count = 0
    if save_signals and signals:
        repository = SignalRepository(db)
        for signal in signals:
            repository.create_signal(signal)
            persisted_count += 1
        db.commit()
    return persisted_count


def _to_response(signals: list, persisted_count: int) -> BreakoutRunResponse:
    return BreakoutRunResponse(
        signal_count=len(signals),
        persisted_count=persisted_count,
        signals=[
            SignalResponse(
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
                side=signal.side,
                entry_price=signal.entry_price,
                stop_loss=signal.stop_loss,
                target_price=signal.target_price,
                signal_time=signal.signal_time,
                metadata=signal.metadata,
            )
            for signal in signals
        ],
    )


@router.post('/breakout/run', response_model=BreakoutRunResponse)
def run_breakout(request: BreakoutRunRequest, db: Session = Depends(get_db)) -> BreakoutRunResponse:
    candles = [Candle(timestamp=i.timestamp, open=i.open, high=i.high, low=i.low, close=i.close, volume=i.volume) for i in request.candles]
    signals = run_breakout_strategy(candles=candles, symbol=request.symbol, capital=request.capital, risk_pct=request.risk_pct, rr_ratio=request.rr_ratio)
    return _to_response(signals, _persist_signals(db, signals, request.save_signals))


@router.post('/demand-supply/run', response_model=BreakoutRunResponse)
def run_demand_supply(request: DemandSupplyRunRequest, db: Session = Depends(get_db)) -> BreakoutRunResponse:
    candles = [Candle(timestamp=i.timestamp, open=i.open, high=i.high, low=i.low, close=i.close, volume=i.volume) for i in request.candles]
    signals = run_demand_supply_strategy(
        candles=candles,
        symbol=request.symbol,
        capital=request.capital,
        risk_pct=request.risk_pct,
        rr_ratio=request.rr_ratio,
        include_fvg=request.include_fvg,
        include_bos=request.include_bos,
    )
    return _to_response(signals, _persist_signals(db, signals, request.save_signals))


@router.post('/indicator/run', response_model=BreakoutRunResponse)
def run_indicator(request: BreakoutRunRequest, db: Session = Depends(get_db)) -> BreakoutRunResponse:
    candles = [Candle(timestamp=i.timestamp, open=i.open, high=i.high, low=i.low, close=i.close, volume=i.volume) for i in request.candles]
    signals = run_indicator_strategy(candles=candles, symbol=request.symbol)
    return _to_response(signals, _persist_signals(db, signals, request.save_signals))


@router.post('/one-trade-day/run', response_model=BreakoutRunResponse)
def run_one_trade_day(request: OneTradeDayRunRequest, db: Session = Depends(get_db)) -> BreakoutRunResponse:
    candles = [Candle(timestamp=i.timestamp, open=i.open, high=i.high, low=i.low, close=i.close, volume=i.volume) for i in request.candles]
    signals = run_one_trade_day_strategy(
        candles=candles,
        symbol=request.symbol,
        capital=request.capital,
        risk_pct=request.risk_pct,
        rr_ratio=request.rr_ratio,
        entry_cutoff_hhmm=request.entry_cutoff_hhmm,
    )
    return _to_response(signals, _persist_signals(db, signals, request.save_signals))


@router.post('/mtf/run', response_model=BreakoutRunResponse)
def run_mtf(request: MtfRunRequest, db: Session = Depends(get_db)) -> BreakoutRunResponse:
    candles = [Candle(timestamp=i.timestamp, open=i.open, high=i.high, low=i.low, close=i.close, volume=i.volume) for i in request.candles]
    signals = run_mtf_strategy(
        candles=candles,
        symbol=request.symbol,
        capital=request.capital,
        risk_pct=request.risk_pct,
        rr_ratio=request.rr_ratio,
        ema_period=request.ema_period,
        setup_mode=request.setup_mode,
        require_retest_strength=request.require_retest_strength,
    )
    return _to_response(signals, _persist_signals(db, signals, request.save_signals))
