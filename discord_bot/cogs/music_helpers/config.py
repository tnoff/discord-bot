from typing import Optional

from pydantic import BaseModel, Field, model_validator


class SpotifyCredentialsConfig(BaseModel):
    '''Spotify API credentials configuration'''
    client_id: str
    client_secret: str


class ServerQueuePriorityConfig(BaseModel):
    '''Server queue priority configuration'''
    server_id: int
    priority: int


class MusicCacheConfig(BaseModel):
    '''Music cache configuration'''
    enable_cache_files: bool = False
    max_cache_files: int = Field(default=2048, ge=1)
    max_cache_size_mb: Optional[int] = Field(default=None, ge=1)


class MusicStorageConfig(BaseModel):
    '''Music storage backend configuration'''
    bucket_name: str
    prefetch_limit: int = Field(default=5, ge=0)


class MusicDownloadConfig(BaseModel):
    '''Music download configuration'''
    download_dir_path: Optional[str] = None
    max_video_length: int = Field(default=900, ge=1)
    extra_ytdlp_options: dict = Field(default_factory=dict)
    banned_videos_list: list[str] = Field(default_factory=list)
    youtube_wait_period_minimum: int = Field(default=30, ge=1)
    youtube_wait_period_max_variance: int = Field(default=10, ge=1)
    spotify_credentials: Optional[SpotifyCredentialsConfig] = None
    youtube_api_key: Optional[str] = None
    server_queue_priority: list[ServerQueuePriorityConfig] = Field(default_factory=list)
    cache: MusicCacheConfig = Field(default_factory=MusicCacheConfig)
    storage: Optional[MusicStorageConfig] = None
    normalize_audio: bool = False
    max_download_retries: int = Field(default=3, ge=1)
    max_youtube_music_search_retries: int = Field(default=3, ge=1)
    # Mostly to keep a cap on the queue to avoid issues
    failure_tracking_max_size: int = Field(default=100, ge=1)
    # Recommended to be at least an hour
    failure_tracking_max_age_seconds: int = Field(default=600, ge=1)

    @model_validator(mode='after')
    def validate_cache_requires_storage(self) -> 'MusicDownloadConfig':
        '''Require storage when enable_cache_files is set.'''
        if self.cache.enable_cache_files and self.storage is None:  #pylint:disable=no-member
            raise ValueError('enable_cache_files requires storage to be configured')
        return self
