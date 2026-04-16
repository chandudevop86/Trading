from __future__ import annotations

"""Portfolio and active-position guard evaluation."""

from vinayak.execution.guards.types import WorkspaceGuardContext, WorkspaceGuardResult
from vinayak.execution.risk_engine import PortfolioRiskConfig, allocate_position_size


def evaluate_portfolio_guard(context: WorkspaceGuardContext) -> WorkspaceGuardResult:
    reasons: list[str] = []
    if context.state_repository.active_trade_exists(mode=context.execution_mode):
        reasons.append("ACTIVE_TRADE_EXISTS")

    historical_rows = context.state_repository.list_open_position_rows(mode=context.execution_mode) + list(context.current_batch_rows)
    allocation = allocate_position_size(
        context.candidate,
        historical_rows,
        PortfolioRiskConfig(
            capital=max(float(context.capital or 0.0), 0.0),
            per_trade_risk_pct=context.per_trade_risk_pct,
            max_position_value=context.max_position_value,
            max_open_positions=context.max_open_positions,
            max_symbol_exposure_pct=context.max_symbol_exposure_pct,
            max_portfolio_exposure_pct=context.max_portfolio_exposure_pct,
            max_open_risk_pct=context.max_open_risk_pct,
            kill_switch_enabled=context.kill_switch_enabled,
        ),
    )
    adjusted_candidate = dict(context.candidate)
    adjusted_candidate["quantity"] = int(allocation.quantity)
    if allocation.adjustment_reasons:
        adjusted_candidate["allocation_adjustment_reasons"] = list(allocation.adjustment_reasons)
    reasons.extend(allocation.block_reasons)
    return WorkspaceGuardResult(
        reasons=list(dict.fromkeys(reasons)),
        risk_snapshot=dict(allocation.snapshot),
        candidate=adjusted_candidate,
    )
