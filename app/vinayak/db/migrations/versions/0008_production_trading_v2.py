"""production trading v2 schema

Revision ID: 0008_production_trading_v2
Revises: 0007_execution_uniqueness_guards
Create Date: 2026-04-14
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0008_production_trading_v2'
down_revision = '0007_execution_uniqueness_guards'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'strategy_runs_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('strategy_name', sa.String(length=64), nullable=False),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('timeframe', sa.String(length=16), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('idx_strategy_runs_v2_symbol_timeframe', 'strategy_runs_v2', ['symbol', 'timeframe'])

    op.create_table(
        'signals_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('strategy_run_id', sa.String(length=36), sa.ForeignKey('strategy_runs_v2.id'), nullable=True),
        sa.Column('idempotency_key', sa.String(length=128), nullable=False),
        sa.Column('strategy_name', sa.String(length=64), nullable=False),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('timeframe', sa.String(length=16), nullable=False),
        sa.Column('signal_type', sa.String(length=16), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('side', sa.String(length=8), nullable=True),
        sa.Column('entry_price', sa.Numeric(18, 6), nullable=True),
        sa.Column('stop_loss', sa.Numeric(18, 6), nullable=True),
        sa.Column('target_price', sa.Numeric(18, 6), nullable=True),
        sa.Column('quantity', sa.Numeric(18, 6), nullable=True),
        sa.Column('confidence', sa.Numeric(10, 6), nullable=False),
        sa.Column('rationale', sa.Text(), nullable=False),
        sa.Column('generated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('candle_timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('idempotency_key', name='uq_signals_v2_idempotency_key'),
    )
    op.create_index('idx_signals_v2_symbol_timeframe', 'signals_v2', ['symbol', 'timeframe'])
    op.create_index('idx_signals_v2_strategy_status', 'signals_v2', ['strategy_name', 'status'])

    op.create_table(
        'execution_requests_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('signal_id', sa.String(length=36), sa.ForeignKey('signals_v2.id'), nullable=False),
        sa.Column('idempotency_key', sa.String(length=128), nullable=False),
        sa.Column('mode', sa.String(length=16), nullable=False),
        sa.Column('account_id', sa.String(length=64), nullable=False),
        sa.Column('requested_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('idempotency_key', name='uq_execution_requests_v2_idempotency_key'),
    )
    op.create_index('idx_execution_requests_v2_mode_created', 'execution_requests_v2', ['mode', 'created_at'])

    op.create_table(
        'executions_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('request_id', sa.String(length=36), sa.ForeignKey('execution_requests_v2.id'), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('failure_reason', sa.String(length=64), nullable=False),
        sa.Column('order_reference', sa.String(length=128), nullable=True),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint('request_id', name='uq_executions_v2_request_id'),
    )
    op.create_index('idx_executions_v2_status', 'executions_v2', ['status'])

    op.create_table(
        'positions_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('side', sa.String(length=8), nullable=False),
        sa.Column('quantity', sa.Numeric(18, 6), nullable=False),
        sa.Column('average_price', sa.Numeric(18, 6), nullable=False),
        sa.Column('mark_price', sa.Numeric(18, 6), nullable=False),
        sa.Column('is_open', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('snapshot_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('idx_positions_v2_symbol_open', 'positions_v2', ['symbol', 'is_open'])

    op.create_table(
        'validation_logs_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('signal_id', sa.String(length=36), sa.ForeignKey('signals_v2.id'), nullable=False),
        sa.Column('is_valid', sa.Boolean(), nullable=False),
        sa.Column('reason', sa.String(length=128), nullable=False),
        sa.Column('detail', sa.Text(), nullable=False),
        sa.Column('validated_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('idx_validation_logs_v2_signal', 'validation_logs_v2', ['signal_id'])

    op.create_table(
        'audit_logs_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('correlation_id', sa.String(length=36), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('payload_json', sa.Text(), nullable=False),
        sa.Column('occurred_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('idx_audit_logs_v2_event_type_created', 'audit_logs_v2', ['event_type', 'occurred_at'])

    op.create_table(
        'backtest_reports_v2',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('strategy_name', sa.String(length=64), nullable=False),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('timeframe', sa.String(length=16), nullable=False),
        sa.Column('trade_count', sa.Integer(), nullable=False),
        sa.Column('hit_ratio', sa.Numeric(10, 6), nullable=False),
        sa.Column('profit_factor', sa.Numeric(18, 6), nullable=False),
        sa.Column('max_drawdown', sa.Numeric(18, 6), nullable=False),
        sa.Column('average_r_multiple', sa.Numeric(18, 6), nullable=False),
        sa.Column('generated_at', sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index('idx_backtest_reports_v2_strategy_symbol', 'backtest_reports_v2', ['strategy_name', 'symbol'])


def downgrade() -> None:
    op.drop_index('idx_backtest_reports_v2_strategy_symbol', table_name='backtest_reports_v2')
    op.drop_table('backtest_reports_v2')
    op.drop_index('idx_audit_logs_v2_event_type_created', table_name='audit_logs_v2')
    op.drop_table('audit_logs_v2')
    op.drop_index('idx_validation_logs_v2_signal', table_name='validation_logs_v2')
    op.drop_table('validation_logs_v2')
    op.drop_index('idx_positions_v2_symbol_open', table_name='positions_v2')
    op.drop_table('positions_v2')
    op.drop_index('idx_executions_v2_status', table_name='executions_v2')
    op.drop_table('executions_v2')
    op.drop_index('idx_execution_requests_v2_mode_created', table_name='execution_requests_v2')
    op.drop_table('execution_requests_v2')
    op.drop_index('idx_signals_v2_strategy_status', table_name='signals_v2')
    op.drop_index('idx_signals_v2_symbol_timeframe', table_name='signals_v2')
    op.drop_table('signals_v2')
    op.drop_index('idx_strategy_runs_v2_symbol_timeframe', table_name='strategy_runs_v2')
    op.drop_table('strategy_runs_v2')
