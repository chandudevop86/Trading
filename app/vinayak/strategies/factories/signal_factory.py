from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from vinayak.domain.models import (
    Candle,
    ExecutionSide,
    Timeframe,
    TradeSignal,
    TradeSignalStatus,
    TradeSignalType,
)


def build_entry_signal(
    *,
    strategy_name: str,
    symbol: str,
    timeframe: Timeframe,
    candle: Candle,
    side: ExecutionSide,
    entry_price: Decimal,
    stop_loss: Decimal,
    target_price: Decimal,
    quantity: Decimal,
    confidence: Decimal,
    rationale: str,
    metadata: dict[str, Any] | None = None,
) -> TradeSignal:
    timestamp_slug = candle.timestamp.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')
    return TradeSignal(
        idempotency_key=f'{symbol.upper()}-{strategy_name.upper()}-{timestamp_slug}-{side.value}',
        strategy_name=strategy_name,
        symbol=symbol,
        timeframe=timeframe,
        signal_type=TradeSignalType.ENTRY,
        status=TradeSignalStatus.CREATED,
        generated_at=datetime.now(UTC),
        candle_timestamp=candle.timestamp,
        side=side,
        entry_price=entry_price,
        stop_loss=stop_loss,
        target_price=target_price,
        quantity=quantity,
        confidence=confidence,
        rationale=rationale,
        metadata=metadata or {},
    )


def build_no_trade_signal(
    *,
    strategy_name: str,
    symbol: str,
    timeframe: Timeframe,
    candle: Candle,
    rationale: str,
    metadata: dict[str, Any] | None = None,
) -> TradeSignal:
    timestamp_slug = candle.timestamp.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')
    return TradeSignal(
        idempotency_key=f'{symbol.upper()}-{strategy_name.upper()}-{timestamp_slug}-NO_TRADE',
        strategy_name=strategy_name,
        symbol=symbol,
        timeframe=timeframe,
        signal_type=TradeSignalType.NO_TRADE,
        status=TradeSignalStatus.CREATED,
        generated_at=datetime.now(UTC),
        candle_timestamp=candle.timestamp,
        confidence=Decimal('0'),
        rationale=rationale,
        metadata=metadata or {},
    )
