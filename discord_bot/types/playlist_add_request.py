from typing import Literal

from discord_bot.types.media_request import MediaRequest


class PlaylistAddRequest(MediaRequest):
    '''MediaRequest variant for adding a track to a playlist without playing it.'''
    download_file: Literal[False] = False
    playlist_id: int
