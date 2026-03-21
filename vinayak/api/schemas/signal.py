from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SignalResponse(BaseModel):
    id: int | None = None
    strategy_name: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    target_price: float
    signal_time: datetime
    status: str = Field(default='NEW')
    metadata: dict[str, Any]


class BreakoutRunResponse(BaseModel):
    signal_count: int
    persisted_count: int
    signals: list[SignalResponse]


class SignalListResponse(BaseModel):
    total: int
    signals: list[SignalResponse]


class ReviewedTradeResponse(BaseModel):
    id: int
    signal_id: int | None
    strategy_name: str
    symbol: str
    side: str
    entry_price: float
    stop_loss: float
    target_price: float
    quantity: int
    lots: int
    status: str
    notes: str | None
    created_at: datetime


class ReviewedTradeListResponse(BaseModel):
    total: int
    reviewed_trades: list[ReviewedTradeResponse]


class ExecutionResponse(BaseModel):
    id: int
    signal_id: int | None
    reviewed_trade_id: int | None
    mode: str
    broker: str
    status: str
    executed_price: float | None
    executed_at: datetime | None
    broker_reference: str | None
    notes: str | None


class ExecutionListResponse(BaseModel):
    total: int
    executions: list[ExecutionResponse]


class ExecutionAuditLogResponse(BaseModel):
    id: int
    execution_id: int
    broker: str
    request_payload: str
    response_payload: str | None
    status: str
    created_at: datetime


class ExecutionAuditLogListResponse(BaseModel):
    total: int
    audit_logs: list[ExecutionAuditLogResponse]


class DashboardSummaryResponse(BaseModel):
    broker_ready: bool
    broker_name: str
    reviewed_trade_counts: dict[str, int]
    execution_mode_counts: dict[str, int]
    execution_status_counts: dict[str, int]
    audit_status_counts: dict[str, int]
    recent_audit_failures: int
