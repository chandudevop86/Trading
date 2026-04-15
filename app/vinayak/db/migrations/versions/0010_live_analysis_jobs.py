"""add persistent live analysis jobs

Revision ID: 0010_live_analysis_jobs
Revises: 0009_execution_lifecycle_audit
Create Date: 2026-04-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0010_live_analysis_jobs'
down_revision = '0009_execution_lifecycle_audit'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'live_analysis_jobs',
        sa.Column('id', sa.String(length=36), primary_key=True),
        sa.Column('dedup_key', sa.String(length=191), nullable=False),
        sa.Column('symbol', sa.String(length=64), nullable=False),
        sa.Column('interval', sa.String(length=16), nullable=False),
        sa.Column('period', sa.String(length=16), nullable=False),
        sa.Column('strategy', sa.String(length=64), nullable=False),
        sa.Column('request_payload', sa.Text(), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('attempt_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error', sa.Text(), nullable=True),
        sa.Column('result_payload', sa.Text(), nullable=True),
        sa.Column('requested_at', sa.DateTime(), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=True),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )
    op.create_index(
        'idx_live_analysis_jobs_status_requested',
        'live_analysis_jobs',
        ['status', 'requested_at'],
        unique=False,
    )
    op.create_index(
        'idx_live_analysis_jobs_dedup_status',
        'live_analysis_jobs',
        ['dedup_key', 'status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_live_analysis_jobs_dedup_status', table_name='live_analysis_jobs')
    op.drop_index('idx_live_analysis_jobs_status_requested', table_name='live_analysis_jobs')
    op.drop_table('live_analysis_jobs')
