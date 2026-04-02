from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ExecutionCreateCommand:
    mode: str
    broker: str
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    trade_id: str = ''
    strategy_name: str = ''
    symbol: str = ''
    side: str = ''
    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    quantity: int | None = None
    validation_status: str = ''
    reviewed_trade_status: str = ''
    status: str | None = None
    executed_price: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


__all__ = ["ExecutionCreateCommand"]
