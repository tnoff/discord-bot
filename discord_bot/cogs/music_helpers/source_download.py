from pathlib import Path
from shutil import copyfile
from uuid import uuid4

from discord_bot.cogs.music_helpers.common import YT_DLP_KEYS
from discord_bot.cogs.music_helpers.source_dict import SourceDict

class SourceDownload():
    '''
    Source file of downloaded content
    '''
    def __init__(self, file_path: Path, ytdl_data: dict, source_dict: SourceDict):
        '''
        Init source file

        file_path                   :   Path to ytdl file
        ytdl_data                   :   Ytdl download dict
        source_dict                 :   Source dict passed to yt-dlp
        '''
        # Keep only keys we want, has alot of metadata we dont care about
        for key in YT_DLP_KEYS:
            setattr(self, key, ytdl_data.get(key, None))

        self.source_dict = source_dict

        # File path: Path of file to be used in audio play, in guilds path
        # Base path: Path of file that was copied over to guilds path
        self.file_path = file_path
        self.base_path = file_path

        if self.file_path:
            # The modified time of download videos can be the time when it was actually uploaded to youtube
            # Touch here to update the modified time, so that the cleanup check works as intendend
            # Rename file to a random uuid name, that way we can have diff videos with same/similar names
            guild_path = file_path.parent / f'{source_dict.guild_id}'
            guild_path.mkdir(exist_ok=True)
            uuid_path = guild_path / f'{uuid4()}{"".join(i for i in file_path.suffixes)}'
            # We should copy the file here, instead of symlink
            # That way we can handle a case in which the original download was removed from cache
            try:
                copyfile(str(self.base_path), str(uuid_path))
                self.file_path = uuid_path
            except FileNotFoundError:
                # Usually happened if you stopped bot while downloading
                pass

    def delete(self, delete_original=False):
        '''
        Delete file

        If delete original passed, delete base path and original file
        '''
        self.file_path.unlink(missing_ok=True)
        if delete_original:
            self.base_path.unlink(missing_ok=True)

    def __str__(self):
        '''
        Expose as string
        '''
        return f'{self.webpage_url}' #pylint:disable=no-member
