'''
Serializable view of the guild analytics table.

Fourth and last of the table-scoped slices in
projects/discord-db-tier-extraction, and the smallest: two functions and two
call sites. It is also the clearest example of what the seam rule is for.

`ensure_guild_video_analytics` returned a live `GuildVideoAnalytics`, and
`!music-stats` then read six columns off it -- including `created_at`, which it
formats. That works only because the read happens inside the session block that
loaded the row. Move the store behind HTTP and there is no row to read, so the
signature has to name something that survives the trip. It does not survive
because we serialize it; it survives because we decided what the caller
actually needed, which is the six numbers, not the row.
'''
from datetime import datetime

from pydantic import BaseModel


class GuildAnalyticsEntry(BaseModel):
    '''One guild's play totals, detached from any DB session.'''
    total_plays: int
    cached_plays: int
    total_duration_days: int
    total_duration_seconds: int
    created_at: datetime

    @classmethod
    def from_row(cls, row) -> 'GuildAnalyticsEntry':
        '''
        Build an entry from a live GuildVideoAnalytics, reading eagerly.

        row : A GuildVideoAnalytics instance, still attached to its session
        '''
        return cls(
            total_plays=row.total_plays,
            cached_plays=row.cached_plays,
            total_duration_days=row.total_duration_days,
            total_duration_seconds=row.total_duration_seconds,
            created_at=row.created_at,
        )
