from types import SimpleNamespace

import pytest

from discord_commands.utils.playlist_view import playlist_public_view


class FakeStore:
    '''Only the two reads the view makes.'''
    def __init__(self, playlists):
        self.playlists = playlists

    async def get_playlist(self, playlist_id):
        return next((item for item in self.playlists if item.id == playlist_id), None)

    async def list_playlists(self, guild_id):
        return [item for item in self.playlists if item.server_id == guild_id and not item.is_history]


def playlist(playlist_id, server_id=1, is_history=False):
    return SimpleNamespace(id=playlist_id, server_id=server_id, is_history=is_history)


@pytest.mark.asyncio
async def test_unknown_playlist_has_no_view():
    assert await playlist_public_view(FakeStore([]), 5, 1) is None


@pytest.mark.asyncio
async def test_another_guilds_playlist_has_no_view():
    assert await playlist_public_view(FakeStore([playlist(5, server_id=2)]), 5, 1) is None


@pytest.mark.asyncio
async def test_history_playlist_is_index_zero():
    assert await playlist_public_view(FakeStore([playlist(5, is_history=True)]), 5, 1) == 0


@pytest.mark.asyncio
async def test_other_playlists_are_numbered_by_list_order_from_one():
    store = FakeStore([playlist(7), playlist(9)])
    assert await playlist_public_view(store, 7, 1) == 1
    assert await playlist_public_view(store, 9, 1) == 2


@pytest.mark.asyncio
async def test_playlist_missing_from_the_guilds_list_has_no_view():
    class Stale(FakeStore):
        async def list_playlists(self, guild_id):
            return []
    assert await playlist_public_view(Stale([playlist(7)]), 7, 1) is None
