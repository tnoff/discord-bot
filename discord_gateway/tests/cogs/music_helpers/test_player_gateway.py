import logging
from asyncio import TimeoutError as async_timeout
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.interfaces.player_gateway import EnsureResult, PlayerStatus

from discord_gateway.cogs.music_helpers.player_gateway import CogPlayerGateway
from discord_gateway.types.cleanup_reason import CleanupReason


def make_player(playing=True, connected=True, title='Song', text_channel_id=22):
    voice_client = MagicMock()
    voice_client.is_playing.return_value = playing
    return SimpleNamespace(
        guild=SimpleNamespace(voice_client=voice_client if connected else None),
        text_channel=SimpleNamespace(id=text_channel_id),
        current_media_download=SimpleNamespace(title=title),
        video_skipped=False,
        destroy=MagicMock(),
    )


def make_gateway(players=None):
    channels = {55: SimpleNamespace(id=55), 22: SimpleNamespace(id=22), 33: SimpleNamespace(id=33)}
    bot = SimpleNamespace(
        get_channel=channels.get,
        fetch_channel=AsyncMock(side_effect=lambda channel_id: SimpleNamespace(id=channel_id)),
        get_guild=lambda guild_id: SimpleNamespace(id=guild_id),
    )
    cog = SimpleNamespace(
        players=players or {}, bot=bot, logger=logging.getLogger('test'),
        download_client=SimpleNamespace(queue_size=AsyncMock(return_value=2)),
        get_player=AsyncMock(), _save_player_session=AsyncMock())
    return CogPlayerGateway(cog), cog


@pytest.mark.asyncio
async def test_status_without_player():
    gateway, cog = make_gateway()
    assert await gateway.status(11) == PlayerStatus(exists=False)
    cog.download_client.queue_size.assert_not_called()


@pytest.mark.asyncio
async def test_status():
    gateway, _ = make_gateway({11: make_player(playing=True)})
    assert await gateway.status(11) == PlayerStatus(
        exists=True, voice_connected=True, is_playing=True, pending_downloads=2, text_channel_id=22)
    gateway, _ = make_gateway({11: make_player(connected=False)})
    status = await gateway.status(11)
    assert (status.voice_connected, status.is_playing) == (False, False)


@pytest.mark.asyncio
async def test_ensure_resolves_ids_and_joins():
    gateway, cog = make_gateway()
    cog.get_player.return_value = make_player()
    assert await gateway.ensure(11, 55, 22) == EnsureResult(ok=True)
    kwargs = cog.get_player.call_args.kwargs
    assert kwargs['join_channel'].id == 55
    assert kwargs['guild'].id == 11
    assert kwargs['text_channel'].id == 22


@pytest.mark.asyncio
async def test_ensure_when_the_player_already_explained():
    '''get_player tells the channel about a ClientException itself and returns nothing'''
    gateway, cog = make_gateway()
    cog.get_player.return_value = None
    assert await gateway.ensure(11, 55, 22) == EnsureResult(ok=False)


@pytest.mark.asyncio
async def test_ensure_timeout_is_returned_not_raised():
    gateway, cog = make_gateway()
    cog.get_player.side_effect = async_timeout('slow')
    result = await gateway.ensure(11, 55, 22)
    assert result.ok is False
    assert result.error.startswith('Bot cannot join channel')


@pytest.mark.asyncio
async def test_channel_falls_back_to_fetch():
    gateway, cog = make_gateway()
    cog.get_player.return_value = make_player()
    await gateway.ensure(11, 999, 22)
    assert cog.get_player.call_args.kwargs['join_channel'].id == 999
    cog.bot.fetch_channel.assert_awaited_once_with(999)


@pytest.mark.asyncio
async def test_stop_destroys_the_player():
    player = make_player()
    gateway, cog = make_gateway()
    cog.get_player.return_value = player
    await gateway.stop(11, 22)
    player.destroy.assert_called_once_with(reason=CleanupReason.USER_STOP)


@pytest.mark.asyncio
async def test_stop_without_a_player():
    gateway, cog = make_gateway()
    cog.get_player.return_value = None
    await gateway.stop(11, 22)


@pytest.mark.asyncio
async def test_move_messages_saves_the_session():
    player = make_player(text_channel_id=22)
    gateway, cog = make_gateway({11: player})
    await gateway.move_messages(11, 33)
    assert player.text_channel.id == 33
    cog._save_player_session.assert_awaited_once_with(player.guild, player)  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_move_messages_without_a_player_does_nothing():
    gateway, cog = make_gateway()
    await gateway.move_messages(11, 33)
    cog._save_player_session.assert_not_called()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_wake_notifies_the_guilds_player():
    player = make_player()
    player.notify_enqueued = MagicMock()
    gateway, _ = make_gateway({11: player})
    await gateway.wake(11)
    player.notify_enqueued.assert_called_once_with()


@pytest.mark.asyncio
async def test_wake_without_a_player_does_nothing():
    gateway, _ = make_gateway()
    await gateway.wake(11)


@pytest.mark.asyncio
async def test_add_cached_queues_on_the_guilds_player():
    player = make_player()
    gateway, cog = make_gateway({11: player})
    cog.add_source_to_player = AsyncMock(return_value=True)
    download = SimpleNamespace(media_request='request')
    assert await gateway.add_cached(11, download) is True
    cog.add_source_to_player.assert_awaited_once_with(download, player)


@pytest.mark.asyncio
async def test_add_cached_without_a_player_does_nothing():
    gateway, cog = make_gateway()
    cog.add_source_to_player = AsyncMock()
    assert await gateway.add_cached(11, SimpleNamespace(media_request='request')) is False
    cog.add_source_to_player.assert_not_called()


@pytest.mark.asyncio
async def test_has_live_player():
    shutting_down = make_player()
    shutting_down.shutdown_called = True
    live = make_player()
    live.shutdown_called = False
    gateway, _ = make_gateway({11: live, 12: shutting_down})
    assert await gateway.has_live_player(11) is True
    assert await gateway.has_live_player(12) is False
    assert await gateway.has_live_player(13) is False
