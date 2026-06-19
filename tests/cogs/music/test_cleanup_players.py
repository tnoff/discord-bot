from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context, fake_source_dict #pylint:disable=unused-import

@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=True, join_channel=fake_context['channel'])
    fake_context['channel'].members = [fake_context['bot'].user]

    # Mock the timeout behavior to return True immediately
    player = cog.players[fake_context['guild'].id]
    mocker.patch.object(player, 'voice_channel_inactive_timeout', return_value=True)

    await cog.cleanup_players()
    # Since cleanup_players calls cleanup() which removes from dict, player should be gone
    assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio
async def test_cleanup_players_bot_shutdown(fake_context):  # pylint: disable=redefined-outer-name
    """cleanup_players raises ExitEarlyException when bot_shutdown_event is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await cog.cleanup_players()


@pytest.mark.asyncio
async def test_cleanup_players_no_players(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup_players returns early without doing anything when there are no players."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert not cog.players
    await cog.cleanup_players()
    assert not cog.players


@pytest.mark.asyncio
async def test_cleanup_marks_search_queue_items_discarded(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup marks items in youtube_music_search_queue as discarded."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Add an item to the search queue for this guild.  In production the request
    # is registered with the broker before it lands in a queue, so register it
    # here too — cleanup discards it via a broker lifecycle push.
    request = fake_source_dict(fake_context)
    await cog.media_broker.register_request(request)
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, request)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    # The request state should have been marked discarded
    from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage  # pylint: disable=import-outside-toplevel
    assert request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio
async def test_cleanup_skips_bundle_different_guild(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup skips bundles that belong to a different guild."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Register a broker bundle for a DIFFERENT guild
    from tests.helpers import FakeGuild  # pylint: disable=import-outside-toplevel
    other_guild = FakeGuild()
    bundle_uuid = await cog.create_bundle(other_guild.id, fake_context['channel'].id)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    # Bundle for the other guild should still be present (not cleaned up)
    assert bundle_uuid in await cog.broker_client.list_bundles_for_guild(other_guild.id)


@pytest.mark.asyncio
async def test_cleanup_skips_bundle_with_active_playlist_add(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup skips bundles that still have active (non-terminal) PlaylistAddRequest items."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Create a broker bundle for THIS guild with an active PlaylistAddRequest
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='https://example.com/playlist', has_search_banner=True,
    )
    search_result = SearchResult(search_type=SearchType.YOUTUBE, raw_search_string='https://example.com/video')
    playlist_req = PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name='tester',
        requester_id=fake_context['author'].id,
        search_result=search_result,
        playlist_id=1,
    )
    playlist_req.bundle_uuid = bundle_uuid
    await cog.media_broker.register_request(playlist_req)
    # playlist_req is in SEARCHING/non-terminal state and download_file=False.
    # It must live in a queue so the cleanup preserve_predicate keeps it (and
    # records its bundle as preserved).
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, playlist_req)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    # Bundle should NOT have been torn down (skipped due to active playlist-add)
    assert bundle_uuid in await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)


@pytest.mark.asyncio
async def test_cleanup_removes_guild_player_dir(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup removes the guild's player subdirectory (prefetched files) when it exists."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    with TemporaryDirectory() as tmp_dir:
        cog.player_dir = Path(tmp_dir)
        guild_player_path = cog.player_dir / str(fake_context['guild'].id)
        guild_player_path.mkdir()
        # Simulate a prefetched file left in the guild player dir
        (guild_player_path / 'prefetched.webm').write_bytes(b'data')
        assert guild_player_path.exists()

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

        assert not guild_player_path.exists()


@pytest.mark.asyncio
async def test_cleanup_skips_player_dir_on_bot_shutdown(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup does not remove guild player dir on BOT_SHUTDOWN (cog_unload handles it)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    with TemporaryDirectory() as tmp_dir:
        cog.player_dir = Path(tmp_dir)
        guild_player_path = cog.player_dir / str(fake_context['guild'].id)
        guild_player_path.mkdir()
        assert guild_player_path.exists()

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

        assert guild_player_path.exists()
