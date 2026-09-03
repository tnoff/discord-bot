from dataclasses import dataclass, field, InitVar
from pathlib import Path
from uuid import uuid4

from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.otel import MediaRequestNaming, MusicMediaDownloadNaming

@dataclass
class MediaDownload():
    '''
    Source file of downloaded content

    file_path                   :   Path to ytdl file
    ytdl_data                   :   Ytdl download dict (InitVar, not stored)
    media_request               :   Media request passed to yt-dlp
    cache_hit                   :   If mediadownload was created via a cache hit
    '''
    # Primary fields (passed to __init__)
    file_path: Path
    ytdl_data: InitVar[dict]  # Only used during init, not stored as field
    media_request: MediaRequest
    cache_hit: bool = False

    # YT-DLP metadata fields (extracted from ytdl_data in __post_init__)
    id: str | None = field(init=False, default=None)
    title: str | None = field(init=False, default=None)
    webpage_url: str | None = field(init=False, default=None)
    uploader: str | None = field(init=False, default=None)
    duration: int | None = field(init=False, default=None)
    extractor: str | None = field(init=False, default=None)

    # Other fields
    uuid: str = field(init=False)
    file_size_bytes: int | None = field(default=None)

    def __post_init__(self, ytdl_data: dict):
        '''
        Extract YT-DLP fields from ytdl_data dict
        '''
        self.uuid = str(uuid4())
        # Extract only the keys we want from ytdl_data
        self.id = ytdl_data.get('id')
        self.title = ytdl_data.get('title')
        self.webpage_url = ytdl_data.get('webpage_url')
        self.uploader = ytdl_data.get('uploader')
        self.duration = ytdl_data.get('duration')
        self.extractor = ytdl_data.get('extractor')

    def __str__(self):
        '''
        Expose as string
        '''
        return f'{self.webpage_url}' #pylint:disable=no-member

def media_download_to_dict(media_download: MediaDownload) -> dict:
    '''
    Serialise a MediaDownload and its MediaRequest to a wire-friendly dict.

    Lives beside the type rather than in any one client because two of them now
    send this shape: the broker client and the video-cache store. The six
    ytdl_data keys are exactly the ones __post_init__ reads back out -- the raw
    yt-dlp dict is an InitVar and was never stored, so there is nothing else of
    it to lose.

    media_download : The download to serialise
    '''
    return {
        'request': media_download.media_request.model_dump(mode='json'),
        'file_path': str(media_download.file_path) if media_download.file_path else None,
        'file_size_bytes': media_download.file_size_bytes,
        'cache_hit': media_download.cache_hit,
        'ytdl_data': {
            'id': media_download.id,
            'title': media_download.title,
            'webpage_url': media_download.webpage_url,
            'uploader': media_download.uploader,
            'duration': media_download.duration,
            'extractor': media_download.extractor,
        },
    }


def media_download_from_dict(data: dict, media_request: MediaRequest) -> MediaDownload:
    '''
    Rebuild a MediaDownload from the dict media_download_to_dict produced.

    The MediaRequest is passed in rather than parsed from *data*: the caller
    already holds the request it asked about, and rebuilding a second equal-but-
    distinct instance would give the returned download a different `uuid` than
    the one the caller is tracking.

    data : The dict shape produced by media_download_to_dict
    media_request : The caller's own request, attached to the result
    '''
    file_path = Path(data['file_path']) if data.get('file_path') else None
    media_download = MediaDownload(file_path, data.get('ytdl_data', {}), media_request,
                                   cache_hit=bool(data.get('cache_hit', False)))
    media_download.file_size_bytes = data.get('file_size_bytes')
    return media_download


def media_download_attributes(media_download: MediaDownload) -> dict:
    '''
    Get span attributes for a source download
    '''
    return {
            MediaRequestNaming.UUID.value: str(media_download.media_request.uuid),
            MusicMediaDownloadNaming.VIDEO_URL.value: media_download.webpage_url,
    }
