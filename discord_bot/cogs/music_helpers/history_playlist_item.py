from discord_bot.cogs.music_helpers.media_download import MediaDownload

class HistoryPlaylistItem:
    '''
    Item to update history playlists
    '''
    def __init__(self, playlist_id: int, media_download: MediaDownload):
        self.playlist_id = playlist_id
        self.media_download = media_download
