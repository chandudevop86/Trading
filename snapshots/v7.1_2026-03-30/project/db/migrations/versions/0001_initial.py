from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'signals',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('strategy_name', sa.String(length=80), nullable=False),
        sa.Column('symbol', sa.String(length=40), nullable=False),
        sa.Column('side', sa.String(length=10), nullable=False),
        sa.Column('entry_price', sa.Float(), nullable=False),
        sa.Column('stop_loss', sa.Float(), nullable=False),
        sa.Column('target_price', sa.Float(), nullable=False),
        sa.Column('signal_time', sa.DateTime(), nullable=False),
        sa.Column('status', sa.String(length=30), nullable=False, server_default='NEW'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )

    op.create_table(
        'reviewed_trades',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('signal_id', sa.Integer(), sa.ForeignKey('signals.id'), nullable=True),
        sa.Column('strategy_name', sa.String(length=80), nullable=False),
        sa.Column('symbol', sa.String(length=40), nullable=False),
        sa.Column('side', sa.String(length=10), nullable=False),
        sa.Column('entry_price', sa.Float(), nullable=False),
        sa.Column('stop_loss', sa.Float(), nullable=False),
        sa.Column('target_price', sa.Float(), nullable=False),
        sa.Column('quantity', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('lots', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('status', sa.String(length=30), nullable=False, server_default='REVIEWED'),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )

    op.create_table(
        'executions',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('signal_id', sa.Integer(), sa.ForeignKey('signals.id'), nullable=True),
        sa.Column('reviewed_trade_id', sa.Integer(), sa.ForeignKey('reviewed_trades.id'), nullable=True),
        sa.Column('mode', sa.String(length=20), nullable=False),
        sa.Column('broker', sa.String(length=40), nullable=False),
        sa.Column('status', sa.String(length=30), nullable=False),
        sa.Column('executed_price', sa.Float(), nullable=True),
        sa.Column('executed_at', sa.DateTime(), nullable=True),
        sa.Column('broker_reference', sa.String(length=80), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )

    op.create_table(
        'execution_audit_logs',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('execution_id', sa.Integer(), sa.ForeignKey('executions.id'), nullable=False),
        sa.Column('broker', sa.String(length=40), nullable=False),
        sa.Column('request_payload', sa.Text(), nullable=False),
        sa.Column('response_payload', sa.Text(), nullable=True),
        sa.Column('status', sa.String(length=30), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table('execution_audit_logs')
    op.drop_table('executions')
    op.drop_table('reviewed_trades')
    op.drop_table('signals')
