# Temporary twin of discord_gateway/command_logic/playlist_view.py until the gateway pins the core release that has it (#1030).
# pylint: disable=duplicate-code
from discord_core.interfaces.database_protocols import PlaylistStore


async def playlist_public_view(playlist_store: PlaylistStore, playlist_id: int, guild_id: int):
    '''
    Get playlist by db id, and view which public index servers see it as
    '''
    playlist = await playlist_store.get_playlist(playlist_id)
    if not playlist:
        return None
    if playlist.server_id != guild_id:
        return None
    if playlist.is_history:
        return 0

    # The index IS the position in list order, which is why that order is
    # part of the store's contract rather than a detail of this loop.
    for (count, playlist_obj) in enumerate(await playlist_store.list_playlists(guild_id)):
        if playlist_id == playlist_obj.id:
            return count + 1
    return None
