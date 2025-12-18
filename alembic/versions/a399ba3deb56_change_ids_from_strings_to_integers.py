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


def convert_column_type_sqlite(batch_op, column_name: str, new_type, cast_type: str):
    """
    Helper function to convert column type in SQLite using temp column approach.

    Args:
        batch_op: Alembic batch operation context
        column_name: Name of the column to convert
        new_type: SQLAlchemy type for the new column (e.g., sa.Integer())
        cast_type: SQL CAST type as string (e.g., 'INTEGER' or 'TEXT')
    """
    temp_name = f'{column_name}_temp'

    # Add temp column with new type
    batch_op.add_column(sa.Column(temp_name, new_type, nullable=True))

    # Copy data with cast
    batch_op.execute(f'UPDATE {batch_op.impl.table.name} SET {temp_name} = CAST({column_name} AS {cast_type})')

    # Drop old column
    batch_op.drop_column(column_name)

    # Rename temp to original name
    batch_op.alter_column(temp_name, new_column_name=column_name)


def upgrade() -> None:
    """Upgrade schema."""
    # SQLite doesn't support ALTER COLUMN for type changes, so we use temp columns
    bind = op.get_bind()

    if bind.dialect.name == 'sqlite':
        # guild.server_id: VARCHAR -> Integer
        with op.batch_alter_table('guild') as batch_op:
            convert_column_type_sqlite(batch_op, 'server_id', sa.Integer(), 'INTEGER')

        # markov_channel columns: VARCHAR -> Integer
        with op.batch_alter_table('markov_channel') as batch_op:
            convert_column_type_sqlite(batch_op, 'channel_id', sa.Integer(), 'INTEGER')
            convert_column_type_sqlite(batch_op, 'server_id', sa.Integer(), 'INTEGER')
            convert_column_type_sqlite(batch_op, 'last_message_id', sa.Integer(), 'INTEGER')

        # playlist.server_id: VARCHAR -> Integer
        with op.batch_alter_table('playlist') as batch_op:
            convert_column_type_sqlite(batch_op, 'server_id', sa.Integer(), 'INTEGER')
    else:
        # PostgreSQL and other databases can use ALTER COLUMN directly
        op.alter_column('guild', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'channel_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True)
        op.alter_column('markov_channel', 'last_message_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True)
        op.alter_column('playlist', 'server_id',
                   existing_type=sa.VARCHAR(length=128),
                   type_=sa.Integer(),
                   existing_nullable=True)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()

    if bind.dialect.name == 'sqlite':
        # playlist.server_id: Integer -> VARCHAR
        with op.batch_alter_table('playlist') as batch_op:
            convert_column_type_sqlite(batch_op, 'server_id', sa.VARCHAR(length=128), 'TEXT')

        # markov_channel columns: Integer -> VARCHAR
        with op.batch_alter_table('markov_channel') as batch_op:
            convert_column_type_sqlite(batch_op, 'last_message_id', sa.VARCHAR(length=128), 'TEXT')
            convert_column_type_sqlite(batch_op, 'server_id', sa.VARCHAR(length=128), 'TEXT')
            convert_column_type_sqlite(batch_op, 'channel_id', sa.VARCHAR(length=128), 'TEXT')

        # guild.server_id: Integer -> VARCHAR
        with op.batch_alter_table('guild') as batch_op:
            convert_column_type_sqlite(batch_op, 'server_id', sa.VARCHAR(length=128), 'TEXT')
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
