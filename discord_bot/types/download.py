from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field

from discord_bot.types.media_request import MediaRequest


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
    media_request: MediaRequest
    ytdlp_data: dict | None
    file_name: Path | None
    download_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
