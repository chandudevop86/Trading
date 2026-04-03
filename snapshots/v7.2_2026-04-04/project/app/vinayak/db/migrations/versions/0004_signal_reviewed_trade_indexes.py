from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0004_signal_reviewed_trade_indexes'
down_revision = '0003_execution_constraints'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())

    if 'signals' in table_names:
        signal_indexes = {item.get('name') for item in inspector.get_indexes('signals')}
        if 'idx_signals_symbol_time' not in signal_indexes:
            op.create_index('idx_signals_symbol_time', 'signals', ['symbol', 'signal_time'], unique=False)
        if 'idx_signals_status_time' not in signal_indexes:
            op.create_index('idx_signals_status_time', 'signals', ['status', 'signal_time'], unique=False)

    if 'reviewed_trades' in table_names:
        reviewed_indexes = {item.get('name') for item in inspector.get_indexes('reviewed_trades')}
        if 'idx_reviewed_trades_status_created' not in reviewed_indexes:
            op.create_index('idx_reviewed_trades_status_created', 'reviewed_trades', ['status', 'created_at'], unique=False)
        if 'idx_reviewed_trades_signal_status' not in reviewed_indexes:
            op.create_index('idx_reviewed_trades_signal_status', 'reviewed_trades', ['signal_id', 'status'], unique=False)

        reviewed_uniques = {item.get('name') for item in inspector.get_unique_constraints('reviewed_trades')}
        if 'uq_reviewed_trade_signal_id' not in reviewed_uniques:
            op.create_unique_constraint('uq_reviewed_trade_signal_id', 'reviewed_trades', ['signal_id'])


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_names = set(inspector.get_table_names())

    if 'reviewed_trades' in table_names:
        reviewed_indexes = {item.get('name') for item in inspector.get_indexes('reviewed_trades')}
        if 'idx_reviewed_trades_signal_status' in reviewed_indexes:
            op.drop_index('idx_reviewed_trades_signal_status', table_name='reviewed_trades')
        if 'idx_reviewed_trades_status_created' in reviewed_indexes:
            op.drop_index('idx_reviewed_trades_status_created', table_name='reviewed_trades')

        reviewed_uniques = {item.get('name') for item in inspector.get_unique_constraints('reviewed_trades')}
        if 'uq_reviewed_trade_signal_id' in reviewed_uniques:
            op.drop_constraint('uq_reviewed_trade_signal_id', 'reviewed_trades', type_='unique')

    if 'signals' in table_names:
        signal_indexes = {item.get('name') for item in inspector.get_indexes('signals')}
        if 'idx_signals_status_time' in signal_indexes:
            op.drop_index('idx_signals_status_time', table_name='signals')
        if 'idx_signals_symbol_time' in signal_indexes:
            op.drop_index('idx_signals_symbol_time', table_name='signals')
