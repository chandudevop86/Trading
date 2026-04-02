from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0003_execution_constraints'
down_revision = '0002_outbox_events'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = inspector.get_table_names()
    if 'executions' not in table_names:
        return

    unique_names = {item.get('name') for item in inspector.get_unique_constraints('executions')}
    if 'uq_reviewed_trade_execution' not in unique_names:
        op.create_unique_constraint('uq_reviewed_trade_execution', 'executions', ['reviewed_trade_id', 'mode'])

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_signal_mode' not in index_names:
        op.create_index('idx_signal_mode', 'executions', ['signal_id', 'mode'], unique=False)
    if 'idx_broker_ref' not in index_names:
        op.create_index('idx_broker_ref', 'executions', ['broker_reference'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'executions' not in inspector.get_table_names():
        return

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_broker_ref' in index_names:
        op.drop_index('idx_broker_ref', table_name='executions')
    if 'idx_signal_mode' in index_names:
        op.drop_index('idx_signal_mode', table_name='executions')

    unique_names = {item.get('name') for item in inspector.get_unique_constraints('executions')}
    if 'uq_reviewed_trade_execution' in unique_names:
        op.drop_constraint('uq_reviewed_trade_execution', 'executions', type_='unique')
