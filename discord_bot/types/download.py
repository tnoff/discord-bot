from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from discord_bot.types.media_request import MediaRequest


@dataclass
class DownloadStatus():
    '''
    Download Status
    '''
    success: bool
    exception: Exception = None

@dataclass
class DownloadResult():
    '''
    Represent a complete download result from the client
    '''
    status: DownloadStatus
    media_request: MediaRequest
    ytdlp_data: dict | None
    file_name: Path | None
    download_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
