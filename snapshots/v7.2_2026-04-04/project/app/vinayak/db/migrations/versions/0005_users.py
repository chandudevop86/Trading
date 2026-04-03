"""add users table

Revision ID: 0005_users
Revises: 0004_signal_reviewed_trade_indexes
Create Date: 2026-04-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0005_users'
down_revision = '0004_signal_reviewed_trade_indexes'
branch_labels = None
depends_on = None


def _has_table(bind, table_name: str) -> bool:
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_index(bind, table_name: str, index_name: str) -> bool:
    inspector = sa.inspect(bind)
    return any(index.get('name') == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    if not _has_table(bind, 'users'):
        op.create_table(
            'users',
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('username', sa.String(length=80), nullable=False),
            sa.Column('password_hash', sa.String(length=255), nullable=False),
            sa.Column('role', sa.String(length=20), nullable=False),
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
        )
    if not _has_index(bind, 'users', 'idx_users_username'):
        op.create_index('idx_users_username', 'users', ['username'], unique=True)
    if not _has_index(bind, 'users', 'idx_users_role_active'):
        op.create_index('idx_users_role_active', 'users', ['role', 'is_active'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    if _has_table(bind, 'users'):
        inspector = sa.inspect(bind)
        index_names = {index.get('name') for index in inspector.get_indexes('users')}
        if 'idx_users_role_active' in index_names:
            op.drop_index('idx_users_role_active', table_name='users')
        if 'idx_users_username' in index_names:
            op.drop_index('idx_users_username', table_name='users')
        op.drop_table('users')
