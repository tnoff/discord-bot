from datetime import datetime, timezone

from sqlalchemy.orm import declarative_base
from sqlalchemy import BigInteger, Column, DateTime, Integer, String, Boolean
from sqlalchemy import ForeignKey, UniqueConstraint

BASE = declarative_base()

#
# Markov Tables
#

class MarkovChannel(BASE):
    '''
    Markov channel
    '''
    __tablename__ = 'markov_channel'
    __table_args__ = (
        UniqueConstraint('channel_id', 'server_id',
                         name='_unique_markov_channel'),
    )
    id = Column(Integer, primary_key=True)
    channel_id = Column(BigInteger)
    server_id = Column(BigInteger)
    last_message_id = Column(BigInteger)

class MarkovRelation(BASE):
    '''
    Markov Relation
    '''
    __tablename__ = 'markov_relation'
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey('markov_channel.id'))  # FK to markov_channel.id (int32)
    leader_word = Column(String(255))
    follower_word = Column(String(255))
    created_at = Column(DateTime(timezone=True))

#
# Music Tables
#

def utcnow() -> datetime:
    '''
    Current UTC time, used as the insert-time default for created_at columns.

    A callable, not a value: SQLAlchemy evaluates `default=` once per INSERT.
    Passing `datetime.now(timezone.utc)` directly would freeze the import-time
    clock into every row for the life of the process.
    '''
    return datetime.now(timezone.utc)


class Playlist(BASE):
    '''
    Playlist
    '''
    __tablename__ = 'playlist'
    __table_args__ = (
        UniqueConstraint('name', 'server_id',
                         name='_server_playlist'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    server_id = Column(BigInteger)
    last_queued = Column(DateTime(timezone=True), nullable=True)
    # Defaulted at the model rather than at each construction site: nothing that
    # built a Playlist ever passed it, so every row in the table has a NULL here
    # and every `ORDER BY created_at` over them is unordered.
    created_at = Column(DateTime(timezone=True), default=utcnow)
    is_history = Column(Boolean)


class PlaylistItem(BASE):
    '''
    Playlist Item
    '''
    __tablename__ = 'playlist_item'
    __table_args__ = (
        UniqueConstraint('video_url', 'playlist_id',
                         name='_unique_playlist_video'),
    )
    id = Column(Integer, primary_key=True)
    title = Column(String(256))
    video_url = Column(String(256))
    uploader = Column(String(256))
    playlist_id = Column(Integer, ForeignKey('playlist.id'))
    # Same as Playlist.created_at, and it matters more here: the history
    # playlist's eviction deletes "the oldest" items by this column.
    created_at = Column(DateTime(timezone=True), default=utcnow)


class VideoCache(BASE):
    '''
    Cached downloaded videos
    '''
    __tablename__ = 'video_cache'
    id = Column(Integer, primary_key=True)
    # YTDLP Keys
    video_id = Column(String(32))
    video_url = Column(String(256))
    title = Column(String(1024))
    uploader = Column(String(1024))
    duration = Column(Integer) # In seconds
    extractor = Column(String(256))
    # Other metadata
    last_iterated_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True))
    count = Column(Integer)
    ready_for_deletion = Column(Boolean)
    file_size_bytes = Column(Integer, nullable=True)
    # File paths
    base_path = Column(String(2048))
    storage_type = Column(String(16), nullable=True)  # 's3' or 'local'


class Guild(BASE):
    '''
    Discord Guild
    '''
    __tablename__ = 'guild'
    id = Column(Integer, primary_key=True)
    server_id = Column(BigInteger)

class GuildVideoAnalytics(BASE):
    '''
    Analytic Data of played videos
    '''
    __tablename__ = 'server_video_analytics'
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, ForeignKey('guild.id'))
    total_plays = Column(Integer, default=0)
    cached_plays = Column(Integer, default=0)
    total_duration_days = Column(Integer, default=0)
    total_duration_seconds = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
