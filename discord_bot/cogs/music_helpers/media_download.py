from dataclasses import dataclass, field, InitVar
from pathlib import Path
from shutil import copyfile

from discord_bot.cogs.music_helpers.media_request import MediaRequest
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
    base_path: Path | None = field(init=False, default=None)

    def __post_init__(self, ytdl_data: dict):
        '''
        Extract YT-DLP fields from ytdl_data dict
        '''
        # Extract only the keys we want from ytdl_data
        self.id = ytdl_data.get('id')
        self.title = ytdl_data.get('title')
        self.webpage_url = ytdl_data.get('webpage_url')
        self.uploader = ytdl_data.get('uploader')
        self.duration = ytdl_data.get('duration')
        self.extractor = ytdl_data.get('extractor')

        # Set base_path to file_path initially
        self.base_path = self.file_path

    def ready_file(self, guild_path: Path = None):
        '''
        Ready file for server

        Copy file as symlink

        file_dir : Relocate to specific file dir
        move_file : Move file instead of a symlink
        '''
        guild_path = guild_path or self.file_path.parent / f'{self.media_request.guild_id}'
        guild_path.mkdir(exist_ok=True)
        if self.base_path:
            # The modified time of download videos can be the time when it was actually uploaded to youtube
            # Touch here to update the modified time, so that the cleanup check works as intendend
            # Rename file to a random uuid name, that way we can have diff videos with same/similar names
            uuid_path = guild_path / f'{self.media_request.uuid}{"".join(i for i in self.file_path.suffixes)}'
            # We should copy the file here, instead of symlink
            # That way we can handle a case in which the original download was removed from cache
            if not self.base_path.exists():
                # Usually happened if you stopped bot while downloading
                raise FileNotFoundError('Unable to locate base path')
            copyfile(str(self.base_path), str(uuid_path))
            self.file_path = uuid_path

    def delete(self):
        '''
        Delete file

        '''
        self.file_path.unlink(missing_ok=True)

    def __str__(self):
        '''
        Expose as string
        '''
        return f'{self.webpage_url}' #pylint:disable=no-member

def media_download_attributes(media_download: MediaDownload) -> dict:
    '''
    Get span attributes for a source download
    '''
    return {
            MediaRequestNaming.UUID.value: str(media_download.media_request.uuid),
            MusicMediaDownloadNaming.VIDEO_URL.value: media_download.webpage_url,
    }
