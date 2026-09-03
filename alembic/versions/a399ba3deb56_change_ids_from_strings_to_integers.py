"""change ids from strings to integers

Revision ID: a399ba3deb56
Revises: faa6198cbed2
Create Date: 2025-12-16 19:25:56.050154

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a399ba3deb56'
down_revision: Union[str, Sequence[str], None] = 'faa6198cbed2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == 'sqlite':
        # SQLite doesn't support ALTER COLUMN for type changes directly
        # Use batch operations which recreate the table with new schema

        # guild.server_id: VARCHAR -> Integer
        with op.batch_alter_table('guild') as batch_op:
            batch_op.alter_column('server_id',
                                  existing_type=sa.VARCHAR(length=128),
                                  type_=sa.Integer(),
                                  existing_nullable=True)

        # markov_channel columns: VARCHAR -> Integer
        with op.batch_alter_table('markov_channel') as batch_op:
            batch_op.alter_column('channel_id',
                                  existing_type=sa.VARCHAR(length=128),
                                  type_=sa.Integer(),
                                  existing_nullable=True)
            batch_op.alter_column('server_id',
                                  existing_type=sa.VARCHAR(length=128),
                                  type_=sa.Integer(),
                                  existing_nullable=True)
            batch_op.alter_column('last_message_id',
                                  existing_type=sa.VARCHAR(length=128),
                                  type_=sa.Integer(),
                                  existing_nullable=True)

        # playlist.server_id: VARCHAR -> Integer
        with op.batch_alter_table('playlist') as batch_op:
            batch_op.alter_column('server_id',
                                  existing_type=sa.VARCHAR(length=128),
                                  type_=sa.Integer(),
                                  existing_nullable=True)
    else:
        # PostgreSQL and other databases can use ALTER COLUMN directly.
        #
        # postgresql_using is not optional here. Postgres refuses to cast
        # VARCHAR to INTEGER implicitly ("column ... cannot be cast
        # automatically to type integer") and alembic runs the whole upgrade in
        # one transaction, so without these the chain does not merely stop at
        # this revision -- it rolls back to zero relations. This branch was
        # authored against sqlite, where the batch_alter_table above recreates
        # the table and the cast never comes up.
        op.alter_column('guild', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True,
                   postgresql_using='server_id::integer')
        op.alter_column('markov_channel', 'channel_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True,
                   postgresql_using='channel_id::integer')
        op.alter_column('markov_channel', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True,
                   postgresql_using='server_id::integer')
        op.alter_column('markov_channel', 'last_message_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True,
                   postgresql_using='last_message_id::integer')
        op.alter_column('playlist', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True,
                   postgresql_using='server_id::integer')


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == 'sqlite':
        # SQLite: Use batch operations which recreate the table with old schema

        # playlist.server_id: Integer -> VARCHAR
        with op.batch_alter_table('playlist') as batch_op:
            batch_op.alter_column('server_id',
                                  existing_type=sa.Integer(),
                                  type_=sa.VARCHAR(length=128),
                                  existing_nullable=True)

        # markov_channel columns: Integer -> VARCHAR
        with op.batch_alter_table('markov_channel') as batch_op:
            batch_op.alter_column('last_message_id',
                                  existing_type=sa.Integer(),
                                  type_=sa.VARCHAR(length=128),
                                  existing_nullable=True)
            batch_op.alter_column('server_id',
                                  existing_type=sa.Integer(),
                                  type_=sa.VARCHAR(length=128),
                                  existing_nullable=True)
            batch_op.alter_column('channel_id',
                                  existing_type=sa.Integer(),
                                  type_=sa.VARCHAR(length=128),
                                  existing_nullable=True)

        # guild.server_id: Integer -> VARCHAR
        with op.batch_alter_table('guild') as batch_op:
            batch_op.alter_column('server_id',
                                  existing_type=sa.Integer(),
                                  type_=sa.VARCHAR(length=128),
                                  existing_nullable=True)
    else:
        # PostgreSQL and other databases
        op.alter_column('playlist', 'server_id',
                   existing_type=sa.Integer(),
                   type_=sa.VARCHAR(length=128),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'last_message_id',
                   existing_type=sa.Integer(),
                   type_=sa.VARCHAR(length=128),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'server_id',
                   existing_type=sa.Integer(),
                   type_=sa.VARCHAR(length=128),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'channel_id',
                   existing_type=sa.Integer(),
                   type_=sa.VARCHAR(length=128),
                   existing_nullable=True)
        op.alter_column('guild', 'server_id',
                   existing_type=sa.Integer(),
                   type_=sa.VARCHAR(length=128),
                   existing_nullable=True)
