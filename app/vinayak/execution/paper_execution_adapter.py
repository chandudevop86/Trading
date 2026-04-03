from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from vinayak.execution.broker.adapter_result import ExecutionAdapterResult


class PaperExecutionAdapter:
    def execute(self, *, command, reviewed_trade=None, signal=None) -> ExecutionAdapterResult:
        executed_price = command.executed_price
        if executed_price is None and reviewed_trade is not None:
            executed_price = reviewed_trade.entry_price
        if executed_price is None and signal is not None:
            executed_price = signal.entry_price

        return ExecutionAdapterResult(
            broker=str(command.broker or "SIM"),
            status=str(command.status or "FILLED"),
            executed_price=executed_price,
            executed_at=datetime.now(UTC),
            broker_reference=f"PAPER-{uuid4().hex[:12].upper()}",
            notes="Paper execution simulated by the local adapter.",
        )


__all__ = ["PaperExecutionAdapter"]
