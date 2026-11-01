from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from discord_core.clients.http_commands_client import HttpCommandsClient
from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException
from discord_core.interfaces.commands_client import CommandsUnavailable

from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult

from discord_gateway.cogs.common import (
    COMMANDS_UNAVAILABLE_MESSAGE, CogHelperBase, build_commands_client, envelope_from_ctx,
)

from tests.helpers import fake_context  #pylint:disable=unused-import
from tests.helpers import FakeMessage, FakeMessageDispatcher


class _MinimalConfig(BaseModel):
    value: int = 1


# ---------------------------------------------------------------------------
# __init__: config_model without settings_prefix (line 41)
# ---------------------------------------------------------------------------

def test_config_model_requires_settings_prefix(fake_context):  #pylint:disable=redefined-outer-name
    '''Raises CogMissingRequiredArg when config_model given but settings_prefix omitted'''
    with pytest.raises(CogMissingRequiredArg) as exc:
        CogHelperBase(fake_context['bot'], {}, fake_context['dispatcher'], config_model=_MinimalConfig)
    assert 'settings prefix' in str(exc.value)


# ---------------------------------------------------------------------------
# gate_tasks_on_db_restore (lines 66-72)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gate_tasks_no_backup_cog_calls_start_fn_directly(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''When no DatabaseBackup cog is present, start_tasks_fn is called immediately'''
    cog = CogHelperBase(fake_context['bot'], {}, fake_context['dispatcher'])
    mocker.patch.object(fake_context['bot'], 'get_cog', return_value=None)
    start_fn = MagicMock()
    await cog.gate_tasks_on_db_restore(start_fn)
    start_fn.assert_called_once()


@pytest.mark.asyncio
async def test_gate_tasks_with_backup_cog_creates_init_task(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''When DatabaseBackup cog is present, an init task is created'''
    cog = CogHelperBase(fake_context['bot'], {}, fake_context['dispatcher'])
    backup_cog = MagicMock()
    backup_cog.wait_for_tables = AsyncMock()
    mocker.patch.object(fake_context['bot'], 'get_cog', return_value=backup_cog)
    fake_loop = MagicMock()
    # Close the coroutine so it is not left unawaited (avoids ResourceWarning)
    def _close_coro(coro):
        coro.close()
        return MagicMock()
    fake_loop.create_task.side_effect = _close_coro
    fake_context['bot'].loop = fake_loop
    start_fn = MagicMock()
    await cog.gate_tasks_on_db_restore(start_fn)
    fake_loop.create_task.assert_called_once()


# ---------------------------------------------------------------------------
# _await_restore_then_start (lines 75-77)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_await_restore_then_start_waits_then_calls(fake_context):  #pylint:disable=redefined-outer-name
    '''_await_restore_then_start waits for tables then calls start_tasks_fn'''
    cog = CogHelperBase(fake_context['bot'], {}, fake_context['dispatcher'])
    cog.REQUIRED_TABLES = ['fake_table']  #pylint:disable=attribute-defined-outside-init
    backup_cog = MagicMock()
    backup_cog.wait_for_tables = AsyncMock()
    start_fn = MagicMock()

    await getattr(cog, '_await_restore_then_start')(backup_cog, start_fn)

    backup_cog.wait_for_tables.assert_called_once_with(cog.REQUIRED_TABLES)
    start_fn.assert_called_once()


# ---------------------------------------------------------------------------
# register_result_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_result_queue_sets_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''register_result_queue stores a queue from the dispatcher on the cog'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.register_result_queue()
    assert cog._result_queue is not None  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# dispatch_channel_history: fire-and-forget, result on queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_channel_history_delivers_to_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_channel_history submits a request; result arrives on _result_queue'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.register_result_queue()
    msg = FakeMessage(channel=fake_context['channel'])
    fake_context['channel'].messages = [msg]
    await cog.dispatch_channel_history(
        fake_context['guild'].id,
        fake_context['channel'].id,
    )
    result = cog._result_queue.get_nowait()  #pylint:disable=protected-access
    assert isinstance(result, ChannelHistoryResult)
    assert len(result.messages) == 1
    assert result.messages[0].id == msg.id


@pytest.mark.asyncio
async def test_dispatch_channel_history_error_delivers_to_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''When channel not found, a ChannelHistoryResult with error arrives on _result_queue'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.register_result_queue()
    await cog.dispatch_channel_history(fake_context['guild'].id, 999999)
    result = cog._result_queue.get_nowait()  #pylint:disable=protected-access
    assert isinstance(result, ChannelHistoryResult)
    assert result.error is not None


# ---------------------------------------------------------------------------
# dispatch_guild_emojis: fire-and-forget, result on queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_guild_emojis_delivers_to_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_guild_emojis submits a request; GuildEmojisResult arrives on _result_queue'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.register_result_queue()
    fake_emoji = MagicMock()
    fake_context['guild'].emojis = [fake_emoji]
    await cog.dispatch_guild_emojis(fake_context['guild'].id)
    result = cog._result_queue.get_nowait()  #pylint:disable=protected-access
    assert isinstance(result, GuildEmojisResult)
    assert result.emojis == [fake_emoji]


# ---------------------------------------------------------------------------
# dispatch_message: routes through dispatcher, returns content
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_message_delivers_and_returns_content(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_message delivers to the channel and returns the content string'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    result = await cog.dispatch_message(
        fake_context['guild'].id,
        fake_context['channel'].id,
        'hello dispatcher',
    )
    assert result == 'hello dispatcher'
    assert 'hello dispatcher' in fake_context['channel'].messages_sent


# ---------------------------------------------------------------------------
# dispatch_delete: routes through dispatcher
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_delete_removes_message(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_delete routes through the dispatcher and removes the message'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    msg = FakeMessage(channel=fake_context['channel'])
    fake_context['channel'].messages = [msg]
    await cog.dispatch_delete(
        fake_context['guild'].id,
        fake_context['channel'].id,
        msg.id,
    )
    assert msg.deleted


# ---------------------------------------------------------------------------
# dispatch_fetch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_fetch_delegates_to_dispatcher(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_fetch routes through MessageDispatcher.fetch_object and returns the result.'''
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    func = AsyncMock(return_value=42)
    result = await cog.dispatch_fetch(fake_context['guild'].id, func)
    assert result == 42
    func.assert_called_once()


def test_envelope_from_ctx(fake_context):  #pylint:disable=redefined-outer-name
    '''Ids and names are read off ctx; no voice state means no voice channel'''
    ctx = fake_context['context']
    ctx.author.voice = None
    env = envelope_from_ctx(ctx)
    assert env.guild_id == fake_context['guild'].id
    assert env.channel_id == fake_context['channel'].id
    assert env.author_id == fake_context['author'].id
    assert env.author_display_name == fake_context['author'].display_name
    assert env.channel_name == fake_context['channel'].name
    assert env.author_voice_channel_id is None
    assert env.author_voice_channel_name is None
    assert env.author_voice_guild_id is None


def test_envelope_from_ctx_in_voice(fake_context):  #pylint:disable=redefined-outer-name
    '''The author's voice channel id and name are carried when they are connected'''
    ctx = fake_context['context']
    ctx.author.voice = SimpleNamespace(channel=SimpleNamespace(id=77, name='music', guild=SimpleNamespace(id=88)))
    env = envelope_from_ctx(ctx)
    assert env.author_voice_channel_id == 77
    assert env.author_voice_channel_name == 'music'
    assert env.author_voice_guild_id == 88


# ---------------------------------------------------------------------------
# build_commands_client / forward_command: hand a command to the commands pod
# ---------------------------------------------------------------------------

def test_build_commands_client_reads_the_url():
    client = build_commands_client({'general': {'commands_http_url': 'http://commands:8086/'}})
    assert isinstance(client, HttpCommandsClient)
    assert client._base_url == 'http://commands:8086'  #pylint:disable=protected-access


def test_build_commands_client_requires_the_url():
    with pytest.raises(DiscordBotException, match='commands_http_url'):
        build_commands_client({'general': {}})


@pytest.mark.asyncio
async def test_forward_command_sends_envelope_and_args(fake_context):  #pylint:disable=redefined-outer-name
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.commands_client = AsyncMock()
    ctx = fake_context['context']
    await cog.forward_command(ctx, 'roll', input_value='2d6')
    cog.commands_client.forward.assert_awaited_once()
    command, envelope, args = cog.commands_client.forward.await_args.args
    assert (command, args) == ('roll', {'input_value': '2d6'})
    assert envelope == envelope_from_ctx(ctx)
    assert 'span_context' in cog.commands_client.forward.await_args.kwargs
    assert not fake_context['channel'].messages_sent


@pytest.mark.asyncio
async def test_forward_command_tells_the_user_when_the_pod_is_unavailable(fake_context):  #pylint:disable=redefined-outer-name
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.commands_client = AsyncMock()
    cog.commands_client.forward.side_effect = CommandsUnavailable('breaker is open')
    await cog.forward_command(fake_context['context'], 'hello')
    assert fake_context['channel'].messages_sent == [COMMANDS_UNAVAILABLE_MESSAGE]
    cog.commands_client.forward.assert_awaited_once()  # not retried


@pytest.mark.asyncio
async def test_forward_command_lets_a_rejected_command_propagate(fake_context):  #pylint:disable=redefined-outer-name
    cog = CogHelperBase(fake_context['bot'], {}, FakeMessageDispatcher(fake_context['bot']))
    cog.commands_client = AsyncMock()
    cog.commands_client.forward.side_effect = ValueError('bad args')
    with pytest.raises(ValueError):
        await cog.forward_command(fake_context['context'], 'hello')
    assert not fake_context['channel'].messages_sent
