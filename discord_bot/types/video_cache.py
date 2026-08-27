'''
Serializable view of one `video_cache` row.

The video-cache catalog is the first slice of projects/discord-db-tier-extraction to
get a Protocol, and this type is what makes that Protocol implementable twice.
Today `get_deletable_entries` hands back live SQLAlchemy `VideoCache` instances:
objects bound to the session that loaded them, whose attributes are lazy reads
against a connection the caller does not own. Neither property survives a
network hop, so a store that answers over HTTP cannot satisfy a signature
written in terms of them.

VideoCacheEntry is the same row with both properties removed — a plain pydantic
model, fully materialized at construction, `model_dump`/`model_validate` clean.
It mirrors the table rather than the three fields `cache_cleanup` reads
(`id`, `video_url`, `base_path`) because the remaining columns are exactly what
`get_webpage_url_item` rebuilds a MediaDownload from, and that method is the
next one across the seam.

`from_row` is the only place the ORM object is touched. Keeping the conversion
here rather than at each call site is what lets `discord_bot.database` drop out
of a caller's import chain once its store is remote.
'''
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class VideoCacheEntry(BaseModel):
    '''One cached download's catalog row, detached from any DB session.'''
    id: int
    video_id: Optional[str] = None
    video_url: Optional[str] = None
    title: Optional[str] = None
    uploader: Optional[str] = None
    duration: Optional[int] = None
    extractor: Optional[str] = None
    last_iterated_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    count: Optional[int] = None
    ready_for_deletion: Optional[bool] = None
    file_size_bytes: Optional[int] = None
    base_path: Optional[str] = None
    storage_type: Optional[str] = None

    @classmethod
    def from_row(cls, row) -> 'VideoCacheEntry':
        '''
        Build an entry from a SQLAlchemy VideoCache row.

        Reads every mapped attribute eagerly, while the row is still attached.
        A caller that returned the row itself and read `.base_path` later would
        get a DetachedInstanceError once the session closed -- which is the
        in-process shape of the same bug an HTTP store would hit.

        row : VideoCache ORM instance
        '''
        return cls(
            id=row.id,
            video_id=row.video_id,
            video_url=row.video_url,
            title=row.title,
            uploader=row.uploader,
            duration=row.duration,
            extractor=row.extractor,
            last_iterated_at=row.last_iterated_at,
            created_at=row.created_at,
            count=row.count,
            ready_for_deletion=row.ready_for_deletion,
            file_size_bytes=row.file_size_bytes,
            base_path=str(row.base_path) if row.base_path is not None else None,
            storage_type=row.storage_type,
        )
