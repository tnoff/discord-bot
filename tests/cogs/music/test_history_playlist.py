from tempfile import TemporaryDirectory

import pytest

from discord_bot.database import Playlist, PlaylistItem
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import mock_session, fake_source_dict, fake_source_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_history_playlist_update(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1

            # Run twice to exercise dupes aren't created
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_history_playlist_update_delete_extra_items(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'playlist': {
                'server_playlist_max_size': 1,
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            s2 = fake_source_dict(fake_context)
            sd2 = SourceDownload(sd.file_path, {'webpage_url': 'https://foo.example.dos'}, s2)
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd2))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1
