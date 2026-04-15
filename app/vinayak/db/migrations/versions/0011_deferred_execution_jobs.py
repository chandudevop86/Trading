"""add deferred execution jobs

Revision ID: 0011_deferred_execution_jobs
Revises: 0010_live_analysis_jobs
Create Date: 2026-04-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0011_deferred_execution_jobs'
down_revision = '0010_live_analysis_jobs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'deferred_execution_jobs',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('source_job_id', sa.String(length=36), nullable=True),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('strategy', sa.String(length=64), nullable=False),
        sa.Column('execution_mode', sa.String(length=16), nullable=False),
        sa.Column('request_payload', sa.Text(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('attempt_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('signal_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('outbox_event_id', sa.Integer(), nullable=True),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('result_payload', sa.Text(), nullable=True),
        sa.Column('requested_at', sa.DateTime(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index(
        'idx_deferred_execution_jobs_status_requested',
        'deferred_execution_jobs',
        ['status', 'requested_at'],
        unique=False,
    )
    op.create_index(
        'idx_deferred_execution_jobs_symbol_strategy',
        'deferred_execution_jobs',
        ['symbol', 'strategy', 'status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_deferred_execution_jobs_symbol_strategy', table_name='deferred_execution_jobs')
    op.drop_index('idx_deferred_execution_jobs_status_requested', table_name='deferred_execution_jobs')
    op.drop_table('deferred_execution_jobs')
