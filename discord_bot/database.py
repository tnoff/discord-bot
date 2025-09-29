from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, DateTime, BigInteger, Integer, String, Boolean
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
    channel_id = Column(String(128))
    server_id = Column(String(128))
    last_message_id = Column(String(128))

class MarkovRelation(BASE):
    '''
    Markov Relation
    '''
    __tablename__ = 'markov_relation'
    id = Column(Integer, primary_key=True)
    channel_id = Column(Integer, ForeignKey('markov_channel.id'))
    leader_word = Column(String(255))
    follower_word = Column(String(255))
    created_at = Column(DateTime)

#
# Music Tables
#

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
    server_id = Column(String(128))
    last_queued = Column(DateTime, nullable=True)
    created_at = Column(DateTime)
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
    created_at = Column(DateTime)


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
    last_iterated_at = Column(DateTime)
    created_at = Column(DateTime)
    count = Column(Integer)
    ready_for_deletion = Column(Boolean)
    # File paths
    base_path = Column(String(2048))


class VideoCacheBackup(BASE):
    '''
    Video Cache Backup in Object Storage
    '''
    __tablename__ = 'video_cache_backup'
    id = Column(Integer, primary_key=True)
    video_cache_id = Column(Integer, ForeignKey('video_cache.id'))
    storage = Column(String(1024))
    bucket_name = Column(String(1024))
    object_path = Column(String(1024))

class Guild(BASE):
    '''
    Discord Guild
    '''
    __tablename__ = 'guild'
    id = Column(Integer, primary_key=True)
    server_id = Column(String(128))

class GuildVideoAnalytics(BASE):
    '''
    Analytic Data of played videos
    '''
    __tablename__ = 'server_video_analytics'
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, ForeignKey('guild.id'))
    total_plays = Column(Integer, default=0)
    cached_plays = Column(Integer, default=0)
    total_duration_seconds = Column(BigInteger, default=0)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
