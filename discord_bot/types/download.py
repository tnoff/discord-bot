from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field

from discord_bot.types.playlist_add_request import AnyMediaRequest


class LifecycleEvent(str, Enum):
    '''
    Lifecycle event for broker status updates.
    Emitted by the download worker (BACKOFF/IN_PROGRESS/RETRY/DISCARDED) and by
    the bot pod (QUEUED/COMPLETED/FAILED) so the broker can drive its bundle UI.
    Maps to a PUT /requests/{uuid}/status body for the HTTP broker.
    '''
    QUEUED = 'queued'
    BACKOFF = 'backoff'
    IN_PROGRESS = 'in_progress'
    RETRY = 'retry'
    RETRY_SEARCH = 'retry_search'
    DISCARDED = 'discarded'
    COMPLETED = 'completed'
    FAILED = 'failed'


class LifecycleStatusUpdate(BaseModel):
    '''
    Status update payload pushed to MediaBroker.update_request_status.
    failure_reason carries the user-facing message for FAILED events; the
    broker stores it on the MediaRequest so bundle renders pick it up.
    '''
    event: LifecycleEvent
    error_detail: str | None = None
    backoff_seconds: int | None = None
    failure_reason: str | None = None


class DownloadErrorType(str, Enum):
    '''Serializable error classification for download failures'''
    RETRYABLE = 'retryable'
    BOT_FLAGGED = 'bot_flagged'
    RETRY_LIMIT_EXCEEDED = 'retry_limit_exceeded'
    PRIVATE_VIDEO = 'private_video'
    TERMS_VIOLATION = 'terms_violation'
    UNAVAILABLE = 'unavailable'
    AGE_RESTRICTED = 'age_restricted'
    INVALID_FORMAT = 'invalid_format'
    NOT_FOUND = 'not_found'
    FILE_NOT_FOUND = 'file_not_found'
    TOO_LONG = 'too_long'
    BANNED = 'banned'


class DownloadStatus(BaseModel):
    '''
    Download Status
    '''
    success: bool
    error_type: DownloadErrorType | None = None
    user_message: str | None = None
    error_detail: str | None = None


class DownloadResult(BaseModel):
    '''
    Represent a complete download result from the client
    '''
    status: DownloadStatus
    media_request: AnyMediaRequest
    ytdlp_data: dict | None
    file_name: Path | None
    download_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    post_process_timestamp: datetime | None = None
    file_size_bytes: int | None = None
    span_context: dict | None = None
