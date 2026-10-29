from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from discord_core.cogs.music_helpers.common import MultipleMutableType
from discord_core.clients.http_commands_client import HttpCommandsClient
from discord_core.exceptions import ExitEarlyException
from discord_core.interfaces.commands_client import CommandsUnavailable
from discord_core.utils.discord_context import DiscordContextNaming

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.types.cleanup_reason import CleanupReason

from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import FakeGuild, FakeVoiceClient, fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import attach_in_process_broker
from tests.helpers import attach_in_process_search
from tests.helpers import attach_in_process_download

@pytest.fixture(name='teardown_guild', autouse=True)
def fixture_teardown_guild(mocker):
    '''The commands pod, which cleanup asks to clear the guild's queued work.'''
    return mocker.patch.object(HttpCommandsClient, 'teardown_guild', new_callable=AsyncMock)


@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    attach_in_process_download(cog)
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert not cog.players
    await cog.cleanup_players()
    assert not cog.players


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_leaves_the_queue_and_its_message(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """BOT_SHUTDOWN keeps the guild's queue open and its play-order message up: the queue is what the
    next gateway picks up, and the broker (not the cog) owns that message."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    play_order_key = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    assert play_order_key not in [call.args[0] for call in cog.dispatcher.update_mutable.call_args_list]
    assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed is False


@pytest.mark.asyncio
async def test_cleanup_asks_the_commands_pod_to_tear_the_guild_down(mocker, fake_context, teardown_guild):  # pylint: disable=redefined-outer-name
    """The queues and the bundles are the commands pod's to clear; the gateway only asks."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    teardown_guild.assert_awaited_once_with(fake_context['guild'].id)
    assert fake_context['guild'].id not in cog.players
    assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed


@pytest.mark.asyncio
async def test_cleanup_bot_shutdown_does_not_tear_the_guild_down(mocker, fake_context, teardown_guild):  # pylint: disable=redefined-outer-name
    """A restart parks the queued work and leaves the bundles standing: the downloader and search
    tiers outlive this pod and every request reaches a terminal state that reaps its own entry."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    bundle_uuid = await cog.broker_client.create_bundle(fake_context['guild'].id, fake_context['channel'].id)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    teardown_guild.assert_not_awaited()
    assert bundle_uuid in await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)


@pytest.mark.asyncio
async def test_cleanup_retries_a_teardown_the_pod_did_not_take_once(mocker, fake_context, teardown_guild):  # pylint: disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    sleep = mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    teardown_guild.side_effect = [CommandsUnavailable('restarting'), None]

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

    assert teardown_guild.await_args_list == [call(fake_context['guild'].id)] * 2
    sleep.assert_any_await(1.0)


@pytest.mark.asyncio
async def test_cleanup_goes_on_when_the_commands_pod_stays_unreachable(mocker, fake_context, teardown_guild):  # pylint: disable=redefined-outer-name
    """The guild's queued work is left to the result loop, but the voice disconnect, the player's own
    cleanup, the broker state and the player dir are not held up behind a pod that is down."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    teardown_guild.side_effect = CommandsUnavailable('down')
    warning = mocker.spy(cog.logger, 'warning')

    with TemporaryDirectory() as tmp_dir:
        cog.player_dir = Path(tmp_dir)
        guild_player_path = cog.player_dir / str(fake_context['guild'].id)
        guild_player_path.mkdir()

        await cog.cleanup(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

        assert not guild_player_path.exists()
    assert teardown_guild.await_count == 2
    assert 'not cleared' in warning.call_args.args[0]
    assert fake_context['guild'].id not in cog.players
    assert (await cog.broker_client.get_guild_queue(fake_context['guild'].id)).closed


@pytest.mark.asyncio
async def test_cleanup_removes_guild_player_dir(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """cleanup removes the guild's player subdirectory (prefetched files) when it exists."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    attach_in_process_download(cog)
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
    attach_in_process_broker(cog)
    attach_in_process_download(cog)
    attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
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
