'''
The whole ``music`` block, for the in-process doubles that stand in for the other pods.

The gateway's own ``MusicConfig`` holds only what the gateway reads. The doubles
(``attach_in_process_broker`` / ``_download`` / ``_search``) stand up the broker, downloader and
search workers over a gateway cog, and those read the rest of the block: yt-dlp options, retry and
backoff, failure tracking, the video cache, per-guild priority. Real pods each read their own keys;
this model is the union, with the defaults those pods apply, so the doubles get the same values.
'''
from typing import Optional

from pydantic import BaseModel, Field

from discord_core.interfaces.download_client_protocol import RETRY_BACKOFF_SECONDS_MINIMUM
from discord_core.types.video_cache import MusicCacheConfig
from discord_gateway.cogs.music import MusicGeneralConfig, MusicPlayerConfig, MusicStorageConfig


class PlaylistConfig(BaseModel):
    '''music.playlist'''
    server_playlist_max_size: int = Field(default=64, ge=1)


class ServerQueuePriorityConfig(BaseModel):
    '''One music.download.server_queue_priority entry.'''
    server_id: int
    priority: int


class DownloadConfig(BaseModel):
    '''music.download, as the downloader, search and commands pods read it.'''
    download_dir_path: Optional[str] = None
    max_video_length: int = Field(default=900, ge=1)
    extra_ytdlp_options: dict = Field(default_factory=dict)
    banned_videos_list: list[str] = Field(default_factory=list)
    youtube_wait_period_minimum: int = Field(default=30, ge=1)
    youtube_wait_period_max_variance: int = Field(default=10, ge=1)
    youtube_egress_key: str = 'default'
    server_queue_priority: list[ServerQueuePriorityConfig] = Field(default_factory=list)
    cache: MusicCacheConfig = Field(default_factory=MusicCacheConfig)
    storage: Optional[MusicStorageConfig] = None
    normalize_audio: bool = False
    max_download_retries: int = Field(default=3, ge=1)
    retry_backoff_seconds_minimum: int = Field(default=RETRY_BACKOFF_SECONDS_MINIMUM, ge=0)
    max_youtube_music_search_retries: int = Field(default=3, ge=1)
    failure_tracking_max_size: int = Field(default=100, ge=1)
    failure_tracking_max_age_seconds: int = Field(default=600, ge=1)


class PodMusicConfig(BaseModel):
    '''The ``music`` block as the pods other than the gateway read it.'''
    general: MusicGeneralConfig = Field(default_factory=MusicGeneralConfig)
    player: MusicPlayerConfig = Field(default_factory=MusicPlayerConfig)
    playlist: PlaylistConfig = Field(default_factory=PlaylistConfig)
    download: DownloadConfig = Field(default_factory=DownloadConfig)


def pod_music_config(cog) -> PodMusicConfig:
    '''The full music config for a cog or a ``ResultsPod``: its own ``pod_config``, else its settings.'''
    explicit = getattr(cog, 'pod_config', None)
    if explicit is not None:
        return explicit
    return PodMusicConfig.model_validate(cog.settings.get('music', {}))
