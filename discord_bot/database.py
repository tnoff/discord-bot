from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, DateTime, Integer, String, Boolean
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


class VideoCacheGuild(BASE):
    '''
    Map video cache to a guild
    '''
    __tablename__ = 'video_cache_guild'
    __table_args__ = (
        UniqueConstraint('video_cache_id', 'guild_id',
                         name='_unique_cache_guild'),
    )
    id = Column(Integer, primary_key=True)
    guild_id = Column(Integer, ForeignKey('guild.id'))
    video_cache_id = Column(Integer, ForeignKey('video_cache.id'))


class VideoRequestAnalytics(BASE):
    '''
    Analytics for video download requests and cache usage patterns
    '''
    __tablename__ = 'video_request_analytics'
    id = Column(Integer, primary_key=True)
    # Request identification
    server_id = Column(String(128))  # Discord server ID
    # Video information
    video_url = Column(String(256))   # Final video URL (webpage_url)
    search_string = Column(String(512))  # Original search string
    search_type = Column(String(32))  # SearchType enum value
    video_title = Column(String(1024), nullable=True)  # Video title if available
    video_id = Column(String(32), nullable=True)       # Video ID if available
    extractor = Column(String(256), nullable=True)     # yt-dlp extractor used
    # Cache analytics
    cache_hit_pre_queue = Column(Boolean, default=False)   # Found in cache before entering download queue
    cache_hit_post_queue = Column(Boolean, default=False)  # Found in cache during download (yt-dlp filter)
    download_attempted = Column(Boolean, default=False)    # Whether download was attempted
    download_successful = Column(Boolean, default=False)   # Whether download completed successfully
    # Timing
    created_at = Column(DateTime)
