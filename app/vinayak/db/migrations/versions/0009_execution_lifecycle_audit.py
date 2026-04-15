"""extend execution audit logs for lifecycle transitions

Revision ID: 0009_execution_lifecycle_audit
Revises: 0008_production_trading_v2
Create Date: 2026-04-15
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0009_execution_lifecycle_audit'
down_revision = '0008_production_trading_v2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('execution_audit_logs') as batch_op:
        batch_op.alter_column('execution_id', existing_type=sa.Integer(), nullable=True)
        batch_op.add_column(sa.Column('entity_type', sa.String(length=40), nullable=True))
        batch_op.add_column(sa.Column('entity_id', sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column('event_name', sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column('old_status', sa.String(length=30), nullable=True))
        batch_op.add_column(sa.Column('new_status', sa.String(length=30), nullable=True))
        batch_op.add_column(sa.Column('actor', sa.String(length=80), nullable=True))
        batch_op.add_column(sa.Column('reason', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('metadata_json', sa.Text(), nullable=True))

    op.create_index(
        'idx_execution_audit_logs_entity',
        'execution_audit_logs',
        ['entity_type', 'entity_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_execution_audit_logs_entity', table_name='execution_audit_logs')
    with op.batch_alter_table('execution_audit_logs') as batch_op:
        batch_op.drop_column('metadata_json')
        batch_op.drop_column('reason')
        batch_op.drop_column('actor')
        batch_op.drop_column('new_status')
        batch_op.drop_column('old_status')
        batch_op.drop_column('event_name')
        batch_op.drop_column('entity_id')
        batch_op.drop_column('entity_type')
        batch_op.alter_column('execution_id', existing_type=sa.Integer(), nullable=False)
