'''
Serializable views of the playlist tables.

Third slice of projects/discord-db-tier-extraction, and the largest: fifteen
`database_functions` and roughly as many session blocks in `cogs/music.py`.
Same rule as the two before it -- nothing crossing the seam may be bound to the
session that loaded it -- and the same consequence, that a signature naming
`database.Playlist` is one only the in-process implementation can satisfy.

`PlaylistItemAddOutcome` is the type that carries the most weight here. Adding
items is a loop in three places (saving a queue, merging two playlists, the
post-play history write), each sending a Discord message per item, and each
stopping when the playlist fills. A boolean per item cannot express that: the
caller has to tell "already in the playlist" from "playlist is full" from
"added", because it says something different for each and stops for one of
them. Returning outcomes in order keeps that decision in the cog while the
round trip stays one per batch.

Naming note: `PlaylistItemAddOutcome` is not `types/playlist_add_result.py`'s
`PlaylistAddResult`. That one is a search resolution on its way to a playlist;
this one is the answer from the store about a row.
'''
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class PlaylistEntry(BaseModel):
    '''One playlist row, detached from any DB session.'''
    id: int
    name: Optional[str] = None
    server_id: Optional[int] = None
    last_queued: Optional[datetime] = None
    created_at: Optional[datetime] = None
    is_history: Optional[bool] = None

    @classmethod
    def from_row(cls, row) -> 'PlaylistEntry':
        '''
        Build an entry from a live Playlist, reading every column eagerly.

        row : A Playlist instance, still attached to its session
        '''
        return cls(
            id=row.id,
            name=row.name,
            server_id=row.server_id,
            last_queued=row.last_queued,
            created_at=row.created_at,
            is_history=row.is_history,
        )


class PlaylistItemEntry(BaseModel):
    '''One playlist item row, detached from any DB session.'''
    id: int
    title: Optional[str] = None
    video_url: Optional[str] = None
    uploader: Optional[str] = None
    playlist_id: Optional[int] = None
    created_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row) -> 'PlaylistItemEntry':
        '''
        Build an entry from a live PlaylistItem, reading every column eagerly.

        row : A PlaylistItem instance, still attached to its session
        '''
        return cls(
            id=row.id,
            title=row.title,
            video_url=row.video_url,
            uploader=row.uploader,
            playlist_id=row.playlist_id,
            created_at=row.created_at,
        )


class PlaylistItemWrite(BaseModel):
    '''One item on its way into a playlist.

    Deliberately not a PlaylistItemEntry: an item being written has no id and
    no created_at yet, and a type with optional-everything would let a caller
    pass a half-built row where a complete one is meant.
    '''
    video_url: str
    title: Optional[str] = None
    uploader: Optional[str] = None


class PlaylistItemAddStatus(str, Enum):
    '''Why one item did or did not land in a playlist.'''
    ADDED = 'added'
    DUPLICATE = 'duplicate'
    PLAYLIST_FULL = 'playlist_full'


class PlaylistItemAddOutcome(BaseModel):
    '''What happened to one item in an add batch.

    `PLAYLIST_FULL` is terminal for the batch and is reported against the item
    that hit the ceiling. Items after it were not attempted and get no outcome,
    which matches what the loops did before: they stopped at the first
    PlaylistMaxLength and said nothing about the rest.
    '''
    video_url: str
    title: Optional[str] = None
    status: PlaylistItemAddStatus
    item_id: Optional[int] = None
