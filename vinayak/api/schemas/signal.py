from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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
    validation_summary: dict[str, Any] = Field(default_factory=dict)
    system_status: str = 'NOT_READY'


class LiveOhlcvRowResponse(BaseModel):
    model_config = ConfigDict(extra='allow')

    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    price: float
    interval: str = ''
    provider: str = ''
    symbol: str = ''
    source: str = ''
    is_closed: bool = True


class LiveOhlcvResponse(BaseModel):
    symbol: str
    interval: str
    period: str
    total: int
    candles: list[LiveOhlcvRowResponse]


class LiveAnalysisSignalRow(BaseModel):
    model_config = ConfigDict(extra='allow')

    strategy: str | None = None
    symbol: str | None = None
    side: str | None = None
    trade_no: int | None = None
    trade_label: str | None = None
    timestamp: str | None = None
    entry_time: str | None = None
    entry_price: float | None = None
    stop_loss: float | None = None
    target_price: float | None = None
    option_strike: str | None = None
    option_type: str | None = None
    strike_price: int | None = None
    spot_price: float | None = None


class LiveAnalysisExecutionRow(BaseModel):
    model_config = ConfigDict(extra='allow')

    trade_id: str | None = None
    trade_key: str | None = None
    side: str | None = None
    execution_status: str | None = None
    trade_status: str | None = None
    broker_name: str | None = None
    price: float | None = None
    reason: str | None = None


class LiveAnalysisExecutionSummary(BaseModel):
    mode: str
    executed_count: int
    blocked_count: int
    error_count: int
    skipped_count: int
    duplicate_count: int



class ReportArtifactLocation(BaseModel):
    local_path: str
    s3_uri: str | None = None
    s3_error: str | None = None


class LiveAnalysisReportArtifacts(BaseModel):
    json_report: ReportArtifactLocation
    summary_report: ReportArtifactLocation

class LiveAnalysisResponse(BaseModel):
    symbol: str
    interval: str
    period: str
    strategy: str
    generated_at: str
    candle_count: int
    signal_count: int
    side_counts: dict[str, int]
    candles: list[LiveOhlcvRowResponse]
    signals: list[LiveAnalysisSignalRow]
    telegram_sent: bool
    telegram_error: str
    telegram_payload: dict[str, Any]
    execution_summary: LiveAnalysisExecutionSummary
    execution_rows: list[LiveAnalysisExecutionRow]
    validation_summary: dict[str, Any] = Field(default_factory=dict)
    report_artifacts: LiveAnalysisReportArtifacts




