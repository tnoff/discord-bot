from dataclasses import dataclass

from discord_core.types.media_download import MediaDownload

@dataclass
class HistoryPlaylistItem:
    '''
    Item to update history playlists
    '''
    playlist_id: int
    media_download: MediaDownload
