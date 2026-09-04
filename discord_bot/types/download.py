from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

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
    # True when a FAILED event is a content rejection (the video doesn't qualify)
    # rather than a download that broke.  Carried here so the bundle UI can say
    # "rejected" instead of "failed" — see REJECTION_ERROR_TYPES.
    rejected: bool = False
    # Authoritative attempt number for RETRY / RETRY_SEARCH events. The worker
    # increments its own copy of the request; the broker's display copy never
    # sees that bump, so it is carried here and stored on the request for the
    # "attempt N/M" retry summary to render.
    retry_count: int | None = None
    # Authoritative retry budget (the M in "attempt N/M"), from the same worker
    # that owns retry_count. The broker reads its own max_download_retries /
    # max_youtube_music_search_retries out of its own config file, so whenever
    # the two configs disagree the summary renders a real N against a stale M
    # ("attempt 4/3" in prod on 2026-08-13, downloader at 5 vs broker default 3).
    # Carrying it with the count keeps one owner for both halves of the ratio.
    max_retries: int | None = None


class DownloadErrorType(str, Enum):
    '''Serializable error classification for download failures'''
    RETRYABLE = 'retryable'
    BOT_FLAGGED = 'bot_flagged'
    RETRY_LIMIT_EXCEEDED = 'retry_limit_exceeded'
    # Transient contention: every pool exit was in-flight/backed off, so the item
    # was never attempted. Re-queued as-is without consuming a retry or a RETRY UI.
    NO_EXIT_AVAILABLE = 'no_exit_available'
    PRIVATE_VIDEO = 'private_video'
    TERMS_VIOLATION = 'terms_violation'
    UNAVAILABLE = 'unavailable'
    AGE_RESTRICTED = 'age_restricted'
    INVALID_FORMAT = 'invalid_format'
    NOT_FOUND = 'not_found'
    FILE_NOT_FOUND = 'file_not_found'
    TOO_LONG = 'too_long'
    BANNED = 'banned'


# Terminal errors where the *video* is the problem — nothing about the download
# broke, we looked at the item and declined it.  These render as "rejected" in
# the bundle UI so a 900-second-limit hit doesn't read like a crashed download.
# Everything else (retry limit exhausted, missing file, bot flagging, no search
# result) stays a failure.
REJECTION_ERROR_TYPES = frozenset({
    DownloadErrorType.PRIVATE_VIDEO,
    DownloadErrorType.TERMS_VIOLATION,
    DownloadErrorType.UNAVAILABLE,
    DownloadErrorType.AGE_RESTRICTED,
    DownloadErrorType.INVALID_FORMAT,
    DownloadErrorType.TOO_LONG,
    DownloadErrorType.BANNED,
})


# yt-dlp's YouTube errors are assembled from two sources with different owners.
# The leading sentence is YouTube's own `playabilityStatus.reason`, served from
# their API and free to change with no yt-dlp release; yt-dlp then appends its
# own tail (the cookies/login hint, the captcha note, the rate-limit note).
# Classification therefore matches the LEADING sentence only -- see the
# age-restriction comment in interfaces/download_protocols.py for what happens
# when a matcher reaches into the tail.
#
# The other half of the problem is punctuation. Production emits
# "Sign in to confirm you’re not a bot" with U+2019, not an ASCII apostrophe,
# and yt-dlp pads its appended URLs with double spaces. The bot matcher survives
# that only because it is written as two fragments straddling the apostrophe --
# as one natural string it would be silently dead, exactly like the age gate was.
# Folding the punctuation before matching removes that trap for every matcher
# instead of relying on each one to dodge it.
_PUNCTUATION_FOLD = str.maketrans({
    '‘': "'", '’': "'", '‚': "'", '‛': "'",
    '“': '"', '”': '"',
    '–': '-', '—': '-',
    ' ': ' ',
})


def normalize_ytdlp_error(error_str: str) -> str:
    '''
    Fold a yt-dlp error message to a form that is safe to substring-match:
    typographic punctuation to its ASCII equivalent, runs of whitespace to a
    single space.

    The result is for MATCHING ONLY. The raw message is what gets recorded on
    the span and carried in error_detail, so nothing a human or a log reader
    sees is rewritten.
    '''
    return ' '.join(error_str.translate(_PUNCTUATION_FOLD).split())


def is_rejection(error_type: DownloadErrorType | None) -> bool:
    '''True when a terminal download error means the video was declined rather
    than the download failing.'''
    return error_type in REJECTION_ERROR_TYPES


class DownloadStatus(BaseModel):
    '''
    Download Status
    '''
    success: bool
    error_type: DownloadErrorType | None = None
    user_message: str | None = None
    error_detail: str | None = None


# The only keys any consumer reads out of yt-dlp's info dict. MediaDownload
# lifts these six in __post_init__; the playlist-add path in cogs/music.py reads
# webpage_url/title/uploader; both download workers read extractor. The cache-hit
# path in video_cache_client already fabricates exactly this shape by hand, so it
# is the de-facto contract -- this just makes the download path agree with it.
YTDLP_DATA_KEYS = ('id', 'title', 'webpage_url', 'uploader', 'duration', 'extractor')


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

    @field_validator('ytdlp_data')
    @classmethod
    def project_ytdlp_data(cls, value: dict | None) -> dict | None:
        '''
        Reduce yt-dlp's raw info dict to the keys consumers actually read.

        That dict is not JSON-safe. An HLS download makes yt-dlp attach
        FFmpegFixupM3u8PP instances under ``__postprocessors``, and
        ``register_download_result`` then died on ``model_dump(mode='json')``
        with ``PydanticSerializationError: Unable to serialize unknown type``.
        The download itself had already succeeded, so the failure surfaced as a
        stuck request rather than a download error.

        Projecting is deliberate over blacklisting the offending types: yt-dlp
        is free to embed new objects in that dict at any version, and a
        blacklist would have to keep chasing them. It also drops the entire
        format list, which is the bulk of the payload crossing HTTP and Redis.

        Runs on deserialization too, so a round-trip through the broker is a
        no-op rather than a second projection.
        '''
        if value is None:
            return None
        return {key: value[key] for key in YTDLP_DATA_KEYS if key in value}
