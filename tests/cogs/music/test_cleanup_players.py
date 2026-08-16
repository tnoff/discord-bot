from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType, MultipleMutableType, MediaRequestLifecycleStage
from discord_bot.utils.otel import DiscordContextNaming

from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import FakeGuild, FakeVoiceClient, fake_engine, fake_context, fake_source_dict #pylint:disable=unused-import

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
    await cog.youtube_music_search_client.submit(fake_context['guild'].id, request)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    # The request state should have been marked discarded
    assert request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio
async def test_cleanup_marks_download_queue_items_discarded(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup marks items in the download queue as discarded on a non-shutdown reason.

    The search-queue mirror of this lives above.  Both drain only when the guild
    itself is going away; BOT_SHUTDOWN parks them instead (see below).
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    request = fake_source_dict(fake_context)
    await cog.media_broker.register_request(request)
    await cog.download_client.submit(fake_context['guild'].id, request)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    assert request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_parks_download_queue(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """BOT_SHUTDOWN leaves the download queue alone instead of clearing it.

    Clearing drops the request from the queue but only the follow-up DISCARDED push
    reaps its broker registry entry, so a SIGKILL mid-loop stranded entries in the
    in_flight zone until the 24h TTL.  The downloader tier outlives this pod and
    works the backlog on its own.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    request = fake_source_dict(fake_context)
    await cog.media_broker.register_request(request)
    await cog.download_client.submit(fake_context['guild'].id, request)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    # Still queued, and never marked discarded
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 1
    assert request.lifecycle_stage != MediaRequestLifecycleStage.DISCARDED
    # Puts are not blocked either, so an in-flight submit racing the shutdown lands
    # rather than raising PutsBlocked
    await cog.download_client.submit(fake_context['guild'].id, fake_source_dict(fake_context))
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 2


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_parks_search_queue(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """BOT_SHUTDOWN parks the search queue too — the search tier outlives this pod."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    request = fake_source_dict(fake_context)
    await cog.media_broker.register_request(request)
    await cog.youtube_music_search_client.submit(fake_context['guild'].id, request)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    assert await cog.youtube_music_search_client.queue_size(fake_context['guild'].id) == 1
    assert request.lifecycle_stage != MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_keeps_bundle(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """BOT_SHUTDOWN leaves broker bundles standing.

    The requests they track are parked, not discarded, so each bundle keeps
    rendering real progress and releases itself once its requests go terminal.
    Deleting it here dropped a live progress UI, and doing so after the O(queue)
    discard loop is what made teardown the first casualty of the grace period.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    bundle_uuid = await cog.create_bundle(fake_context['guild'].id, fake_context['channel'].id)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    assert bundle_uuid in await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_clears_queue_message(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """BOT_SHUTDOWN clears the play-order message rather than stranding a queue
    listing in the channel that describes a play queue no process owns."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    play_order_key = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    calls = [call.args for call in cog.dispatcher.update_mutable.call_args_list]
    assert play_order_key in [call[0] for call in calls]
    # Player is already popped, so the rendered content is empty — the table clears
    assert [call[2] for call in calls if call[0] == play_order_key] == [[]]


@pytest.mark.asyncio
async def test_cleanup_skips_bundle_different_guild(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup skips bundles that belong to a different guild."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Register a broker bundle for a DIFFERENT guild
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
    await cog.youtube_music_search_client.submit(fake_context['guild'].id, playlist_req)

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


@pytest.mark.asyncio
async def test_cleanup_disconnect_error_still_reaps_player(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """A voice-client disconnect error is logged but does not abort cleanup or
    leave the player behind (the strand this fix prevents)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'],
                         create_player=True, join_channel=fake_context['channel'])

    # get_player(join_channel=...) connected a voice client; make its disconnect blow up
    async def _boom():
        raise RuntimeError('gateway boom')
    fake_context['guild'].voice_client.disconnect = _boom

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    # Disconnect raised, but cleanup still completed and removed the player
    assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio
async def test_cleanup_orphaned_voice_client_disconnected(fake_context):  # pylint: disable=redefined-outer-name
    """The sweep disconnects a voice client that has no backing player."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    orphan_guild = FakeGuild()
    voice_client = FakeVoiceClient(guild=orphan_guild)
    voice_client.disconnect = AsyncMock()
    cog.bot.voice_clients = [voice_client]

    await cog._cleanup_orphaned_voice_clients()  # pylint: disable=protected-access

    voice_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_cleanup_orphaned_skips_backed_and_guildless(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """The sweep leaves alone clients backed by a player, or with no guild."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=True)

    backed = FakeVoiceClient(guild=fake_context['guild'])  # guild IS in cog.players
    backed.disconnect = AsyncMock()
    guildless = FakeVoiceClient(guild=None)
    guildless.disconnect = AsyncMock()
    cog.bot.voice_clients = [backed, guildless]

    await cog._cleanup_orphaned_voice_clients()  # pylint: disable=protected-access

    backed.disconnect.assert_not_awaited()
    guildless.disconnect.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_orphaned_disconnect_error_swallowed(fake_context):  # pylint: disable=redefined-outer-name
    """A disconnect error during the orphan sweep is logged, not raised."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    orphan_guild = FakeGuild()
    voice_client = FakeVoiceClient(guild=orphan_guild)

    async def _boom():
        raise RuntimeError('boom')
    voice_client.disconnect = _boom
    cog.bot.voice_clients = [voice_client]

    # Must not raise
    await cog._cleanup_orphaned_voice_clients()  # pylint: disable=protected-access


def test_voice_clients_connected_gauge(fake_context):  # pylint: disable=redefined-outer-name
    """The voice-clients gauge reports one observation per connected guild and
    skips clients that have no guild."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    guild_a = FakeGuild()
    cog.bot.voice_clients = [FakeVoiceClient(guild=guild_a), FakeVoiceClient(guild=None)]

    observations = cog._Music__voice_clients_connected_callback(None)  # pylint: disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 1
    assert observations[0].attributes[DiscordContextNaming.GUILD.value] == guild_a.id


def test_voice_clients_connected_gauge_reports_zero_when_disconnected(fake_context):  # pylint: disable=redefined-outer-name
    """With nothing connected the gauge reports an explicit 0 rather than no
    series — same reason as active_players: an idle bot must not read as a dead
    one."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot.voice_clients = []

    observations = cog._Music__voice_clients_connected_callback(None)  # pylint: disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 0
    assert not (observations[0].attributes or {})


def test_voice_clients_connected_gauge_reports_zero_when_all_lack_guilds(fake_context):  # pylint: disable=redefined-outer-name
    """A client with no guild is skipped; if that leaves nothing, the gauge still
    reports 0 rather than an empty list."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot.voice_clients = [FakeVoiceClient(guild=None)]

    observations = cog._Music__voice_clients_connected_callback(None)  # pylint: disable=protected-access

    assert len(observations) == 1
    assert observations[0].value == 0
