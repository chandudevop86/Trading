from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExecutionCreateCommand:
    mode: str
    broker: str
    signal_id: int | None = None
    reviewed_trade_id: int | None = None
    status: str | None = None
    executed_price: float | None = None


__all__ = ["ExecutionCreateCommand"]
