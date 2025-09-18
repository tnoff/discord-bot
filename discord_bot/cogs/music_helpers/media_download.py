from pathlib import Path
from shutil import copyfile

from discord_bot.cogs.music_helpers.common import YT_DLP_KEYS
from discord_bot.cogs.music_helpers.media_request import MediaRequest
from discord_bot.utils.otel import MediaRequestNaming, MusicMediaDownloadNaming

class MediaDownload():
    '''
    Source file of downloaded content
    '''
    def __init__(self, file_path: Path, ytdl_data: dict, media_request: MediaRequest,
                 cache_hit: bool = False):
        '''
        Init source file

        file_path                   :   Path to ytdl file
        ytdl_data                   :   Ytdl download dict
        media_request               :   Media request passed to yt-dlp
        cache_hit                   :   If mediadownload was created via a cache hit
        '''
        # Keep only keys we want, has alot of metadata we dont care about
        for key in YT_DLP_KEYS:
            setattr(self, key, ytdl_data.get(key, None))

        self.media_request = media_request

        # File path: Path of file to be used in audio play, in guilds path
        # Base path: Path of file that was copied over to guilds path
        self.file_path = file_path
        self.base_path = file_path
        self.cache_hit = cache_hit

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
