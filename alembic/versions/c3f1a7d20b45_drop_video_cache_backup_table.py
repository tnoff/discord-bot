"""drop video cache backup table

Revision ID: c3f1a7d20b45
Revises: bf20d91d337c
Create Date: 2026-08-27 12:00:00.000000

The object-storage backup table (added in 0f696315a882) never gained a
caller. Its model and its three helper functions had no references outside
their own unit tests, so nothing has ever written a row to it.

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3f1a7d20b45'
down_revision: Union[str, Sequence[str], None] = 'bf20d91d337c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_table('video_cache_backup')


def downgrade() -> None:
    """Downgrade schema."""
    op.create_table('video_cache_backup',
    sa.Column('id', sa.INTEGER(), nullable=False),
    sa.Column('video_cache_id', sa.INTEGER(), nullable=True),
    sa.Column('storage', sa.VARCHAR(length=1024), nullable=True),
    sa.Column('bucket_name', sa.VARCHAR(length=1024), nullable=True),
    sa.Column('object_path', sa.VARCHAR(length=1024), nullable=True),
    sa.ForeignKeyConstraint(['video_cache_id'], ['video_cache.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
