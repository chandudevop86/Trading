from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0006_execution_additional_indexes'
down_revision = '0005_users'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())
    if 'executions' not in table_names:
        return

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_reviewed_trade_id' not in index_names:
        op.create_index('idx_reviewed_trade_id', 'executions', ['reviewed_trade_id'], unique=False)
    if 'idx_reviewed_trade_mode' not in index_names:
        op.create_index('idx_reviewed_trade_mode', 'executions', ['reviewed_trade_id', 'mode'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())
    if 'executions' not in table_names:
        return

    index_names = {item.get('name') for item in inspector.get_indexes('executions')}
    if 'idx_reviewed_trade_mode' in index_names:
        op.drop_index('idx_reviewed_trade_mode', table_name='executions')
    if 'idx_reviewed_trade_id' in index_names:
        op.drop_index('idx_reviewed_trade_id', table_name='executions')
