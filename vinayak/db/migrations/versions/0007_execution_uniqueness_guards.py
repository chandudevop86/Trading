from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0007_execution_uniqueness_guards'
down_revision = '0006_execution_additional_indexes'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())
    if 'executions' not in table_names:
        return

    unique_names = {item.get('name') for item in inspector.get_unique_constraints('executions')}
    if 'uq_execution_signal_mode' not in unique_names:
        op.create_unique_constraint('uq_execution_signal_mode', 'executions', ['signal_id', 'mode'])
    if 'uq_execution_broker_reference' not in unique_names:
        op.create_unique_constraint('uq_execution_broker_reference', 'executions', ['broker', 'broker_reference'])

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_signal_id' not in index_names:
        op.create_index('idx_signal_id', 'executions', ['signal_id'], unique=False)
    if 'idx_mode' not in index_names:
        op.create_index('idx_mode', 'executions', ['mode'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())
    if 'executions' not in table_names:
        return

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_mode' in index_names:
        op.drop_index('idx_mode', table_name='executions')
    if 'idx_signal_id' in index_names:
        op.drop_index('idx_signal_id', table_name='executions')

    unique_names = {item.get('name') for item in inspector.get_unique_constraints('executions')}
    if 'uq_execution_broker_reference' in unique_names:
        op.drop_constraint('uq_execution_broker_reference', 'executions', type_='unique')
    if 'uq_execution_signal_mode' in unique_names:
        op.drop_constraint('uq_execution_signal_mode', 'executions', type_='unique')
