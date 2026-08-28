"""backfill playlist created_at

Every `playlist` and `playlist_item` row written before this has
`created_at IS NULL`: nothing ever passed the column, and it carried no
default. The queries that order by it therefore returned heap order, which
is stable only until rows are deleted and reinserted -- and the history
playlist deletes and reinserts on every play.

The model now defaults the column, so new rows carry a real timestamp. That
alone would make things *worse* for the existing rows rather than better:
`ORDER BY created_at ASC` puts NULLs last in postgres, so every pre-existing
playlist item would sort behind every new one, and the history playlist's
eviction -- which deletes the oldest items -- would start deleting the
newest ones first.

So the backfill is not cosmetic, it is the other half of the fix. Values are
derived from `id` rather than set to a single constant: ids are sequential,
so ordering by the backfilled timestamps reproduces insertion order, and a
constant would leave every row tied and back in heap order. The epoch base
puts all backfilled rows before anything created from now on, which is true.

Revision ID: d1c8b4e6f7a2
Revises: c3f1a7d20b45
Create Date: 2026-08-27

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd1c8b4e6f7a2'
down_revision: Union[str, Sequence[str], None] = 'c3f1a7d20b45'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Backfill NULL created_at from row id, preserving insertion order."""
    for table in ('playlist', 'playlist_item'):
        op.execute(
            f"UPDATE {table} "  # table name is a literal from the tuple above, not input
            "SET created_at = TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00' "
            "+ (id * INTERVAL '1 second') "
            "WHERE created_at IS NULL"
        )


def downgrade() -> None:
    """Restore the NULLs for rows that look backfilled.

    Bounded to 1970 so a row created normally after the upgrade is not wiped
    by a downgrade. Rows written between the upgrade and the downgrade keep
    their real timestamps.
    """
    for table in ('playlist', 'playlist_item'):
        op.execute(
            f"UPDATE {table} "  # table name is a literal from the tuple above, not input
            "SET created_at = NULL "
            "WHERE created_at < TIMESTAMP WITH TIME ZONE '1971-01-01 00:00:00+00'"
        )
