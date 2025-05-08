from discord_bot.cogs.music_helpers.source_download import SourceDownload

class HistoryPlaylistItem:
    '''
    Item to update history playlists
    '''
    def __init__(self, playlist_id: int, source_download: SourceDownload):
        self.playlist_id = playlist_id
        self.source_download = source_download
