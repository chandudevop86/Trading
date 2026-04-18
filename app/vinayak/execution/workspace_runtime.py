from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy.orm import Session

from vinayak.db.repositories.execution_state_repository import ExecutionStateRepository
from vinayak.execution.runtime import build_execution_facade
from vinayak.observability.observability_logger import log_event
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric


def run_workspace_execution(
    *,
    strategy: str,
    symbol: str,
    candidates: list[dict[str, object]],
    execution_mode: str,
    paper_log_path: str,
    live_log_path: str,
    capital: float | None,
    per_trade_risk_pct: float | None,
    max_trades_per_day: int | None,
    max_daily_loss: float | None,
    max_position_value: float | None,
    max_open_positions: int | None,
    max_symbol_exposure_pct: float | None,
    max_portfolio_exposure_pct: float | None,
    max_open_risk_pct: float | None,
    kill_switch_enabled: bool,
    security_map_path: str,
    resolve_live_kwargs,
    db_session: Session,
):
    import vinayak.execution.gateway as gateway_module

    cycle_started = time.perf_counter()
    set_metric('trading_app_up', 1)
    set_metric('portfolio_kill_switch_active', 1 if kill_switch_enabled else 0)
    mode = str(execution_mode or 'NONE').upper()
    output_path = Path(str(live_log_path if mode == 'LIVE' else paper_log_path))
    historical_rows = gateway_module._existing_rows(output_path)
    batch_keys: set[str] = set()
    result = gateway_module.WorkspaceExecutionResult()
    rows_to_write: list[dict[str, object]] = []

    execution_facade = build_execution_facade(db_session)
    state_repository = ExecutionStateRepository(db_session)
    live_kwargs = dict(resolve_live_kwargs(security_map_path) if resolve_live_kwargs is not None and mode == 'LIVE' else {})
    broker_name = str(live_kwargs.get('broker_name', 'SIM' if mode != 'LIVE' else 'DHAN'))

    for candidate in candidates:
        reasons, unique_key, risk_snapshot, adjusted_candidate = gateway_module._guard_reasons(
            candidate,
            rows_to_write,
            batch_keys,
            execution_mode=mode,
            state_repository=state_repository,
            capital=capital,
            per_trade_risk_pct=per_trade_risk_pct,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
            max_position_value=max_position_value,
            max_open_positions=max_open_positions,
            max_symbol_exposure_pct=max_symbol_exposure_pct,
            max_portfolio_exposure_pct=max_portfolio_exposure_pct,
            max_open_risk_pct=max_open_risk_pct,
            kill_switch_enabled=kill_switch_enabled,
        )
        row = dict(adjusted_candidate)
        row['trade_key'] = unique_key
        row['broker_name'] = broker_name
        row['execution_mode'] = mode
        row['price'] = row.get('entry_price')
        row['risk_snapshot'] = risk_snapshot
        if row.get('allocation_adjustment_reasons'):
            increment_metric('capital_allocator_adjustments_total', 1)
        set_metric('portfolio_open_positions', int(risk_snapshot.get('open_positions', 0)))
        set_metric('portfolio_open_exposure_value', float(risk_snapshot.get('open_notional', 0.0)))
        set_metric('portfolio_open_risk_value', float(risk_snapshot.get('open_risk', 0.0)))
        set_metric('portfolio_kill_switch_active', 1 if kill_switch_enabled else 0)
        set_metric('portfolio_daily_loss_limit', float(abs(max_daily_loss)) if max_daily_loss else 0.0)
        set_metric('portfolio_max_trades_per_day', int(max_trades_per_day or 0))
        set_metric('portfolio_per_trade_risk_pct', float(per_trade_risk_pct or 0.0))

        if reasons:
            row['execution_status'] = 'BLOCKED'
            row['trade_status'] = 'BLOCKED'
            row['reason'] = ', '.join(reasons)
            row['blocked_reason'] = row['reason']
            row['duplicate_reason'] = 'DUPLICATE_TRADE' if 'DUPLICATE_TRADE' in reasons else ''
            result.rows.append(row)
            increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
            increment_metric('paper_trade_expected_blocks_total', 1 if mode == 'PAPER' else 0)
            if 'DUPLICATE_TRADE' in reasons:
                increment_metric('duplicate_trade_blocks_total', 1)
            if any(reason.startswith('MAX_') or 'KILL_SWITCH' in reason for reason in reasons):
                increment_metric('risk_guard_blocks_total', 1)
            log_event(component='execution_gateway', event_name='trade_execution_blocked', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade blocked before execution', context_json={'reasons': reasons, 'trade_id': row.get('trade_id', ''), 'risk_snapshot': risk_snapshot})
            result.blocked_rows.append(row)
            result.blocked_count += 1
            if 'DUPLICATE_TRADE' in reasons:
                result.duplicate_count += 1
            rows_to_write.append(row)
            batch_keys.add(unique_key)
            continue

        try:
            execution_request = gateway_module._build_workspace_execution_request(
                row,
                execution_mode=mode,
                capital=capital,
                per_trade_risk_pct=per_trade_risk_pct,
                max_trades_per_day=max_trades_per_day,
                max_daily_loss=max_daily_loss,
            )
            row['request_id'] = str(execution_request.request_id)
            row['signal_id'] = str(execution_request.signal.signal_id)
            execution_result = execution_facade.execute_request(execution_request)
            row['execution_id'] = str(execution_result.execution_id)
            row['execution_status'] = str(execution_result.status.value or 'REJECTED').upper()
            row['trade_status'] = 'EXECUTED' if row['execution_status'] == 'EXECUTED' else row['execution_status']
            row['broker_name'] = broker_name
            row['broker_reference'] = str(execution_result.order_reference or '')
            row['price'] = gateway_module._safe_float(row.get('entry_price'))
            row['executed_at_utc'] = execution_result.processed_at.isoformat() if execution_result.processed_at is not None else ''
            row['reason'] = str(execution_result.message or '')
            row['duplicate_reason'] = 'DUPLICATE_TRADE' if row['execution_status'] == 'REJECTED' and 'duplicate' in str(execution_result.message or '').lower() else ''
            if mode == 'LIVE' and row['execution_status'] == 'REJECTED':
                row['reason'] = 'LIVE_REQUIRES_APPROVED_REVIEWED_TRADE'
                row['reason_codes'] = list(dict.fromkeys(list(row.get('reason_codes', [])) + ['LIVE_REQUIRES_APPROVED_REVIEWED_TRADE']))
            if row['execution_status'] == 'EXECUTED':
                result.executed_rows.append(row)
                result.executed_count += 1
                increment_metric('paper_trades_executed_total', 1 if mode == 'PAPER' else 0)
                set_metric('executed_paper_trades_today', result.executed_count if mode == 'PAPER' else 0)
                log_event(component='execution_gateway', event_name='trade_execution_success', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='INFO', message='Trade executed', context_json={'trade_id': row.get('trade_id', ''), 'mode': mode, 'status': row.get('execution_status', '')})
            else:
                row['blocked_reason'] = row['reason']
                result.blocked_rows.append(row)
                result.blocked_count += 1
                increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
                increment_metric('paper_trade_expected_blocks_total', 1 if mode == 'PAPER' else 0)
                log_event(component='execution_gateway', event_name='trade_execution_nonfill', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='WARNING', message='Trade did not reach executed state', context_json={'trade_id': row.get('trade_id', ''), 'mode': mode, 'status': row.get('execution_status', ''), 'reason': row.get('reason', '')})
            result.rows.append(row)
            rows_to_write.append(row)
            batch_keys.add(unique_key)
        except Exception as exc:
            row['execution_status'] = 'ERROR'
            row['trade_status'] = 'ERROR'
            row['reason'] = str(exc)
            row['duplicate_reason'] = ''
            result.rows.append(row)
            increment_metric('paper_trade_rejections_total', 1 if mode == 'PAPER' else 0)
            increment_metric('paper_trade_errors_total', 1 if mode == 'PAPER' else 0)
            log_event(component='execution_gateway', event_name='trade_execution_error', symbol=row.get('symbol', ''), strategy=row.get('strategy_name', ''), severity='ERROR', message='Trade execution failed', context_json={'trade_id': row.get('trade_id', ''), 'error': str(exc)})
            result.error_rows.append(row)
            result.error_count += 1
            rows_to_write.append(row)
            batch_keys.add(unique_key)

    gateway_module._write_rows(output_path, rows_to_write, existing_rows=historical_rows)
    duration = round(time.perf_counter() - cycle_started, 4)
    set_metric('trading_cycle_duration_seconds', duration)
    record_stage('execute', status='SUCCESS' if result.error_count == 0 else 'WARN', duration_seconds=duration, symbol=symbol, strategy=strategy, message='Workspace execution cycle finished')
    set_metric('paper_trade_rejections_total', int(gateway_module._safe_int(result.blocked_count + result.error_count)))
    return result


__all__ = ['run_workspace_execution']
