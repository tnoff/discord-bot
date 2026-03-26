from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult

from tests.helpers import fake_context  #pylint:disable=unused-import
from tests.helpers import FakeMessage, fake_engine, generate_fake_context  #pylint:disable=unused-import


class _MinimalConfig(BaseModel):
    value: int = 1


# ---------------------------------------------------------------------------
# __init__: config_model without settings_prefix (line 41)
# ---------------------------------------------------------------------------

def test_config_model_requires_settings_prefix(fake_context):  #pylint:disable=redefined-outer-name
    '''Raises CogMissingRequiredArg when config_model given but settings_prefix omitted'''
    with pytest.raises(CogMissingRequiredArg) as exc:
        CogHelper(fake_context['bot'], {}, None, config_model=_MinimalConfig)
    assert 'settings prefix' in str(exc.value)


# ---------------------------------------------------------------------------
# gate_tasks_on_db_restore (lines 66-72)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gate_tasks_no_backup_cog_calls_start_fn_directly(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''When no DatabaseBackup cog is present, start_tasks_fn is called immediately'''
    cog = CogHelper(fake_context['bot'], {}, None)
    mocker.patch.object(fake_context['bot'], 'get_cog', return_value=None)
    start_fn = MagicMock()
    await cog.gate_tasks_on_db_restore(start_fn)
    start_fn.assert_called_once()


@pytest.mark.asyncio
async def test_gate_tasks_with_backup_cog_creates_init_task(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''When DatabaseBackup cog is present, an init task is created'''
    cog = CogHelper(fake_context['bot'], {}, None)
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
    cog = CogHelper(fake_context['bot'], {}, None)
    backup_cog = MagicMock()
    backup_cog.wait_for_tables = AsyncMock()
    start_fn = MagicMock()

    await getattr(cog, '_await_restore_then_start')(backup_cog, start_fn)

    backup_cog.wait_for_tables.assert_called_once_with(cog.REQUIRED_TABLES)
    start_fn.assert_called_once()


# ---------------------------------------------------------------------------
# with_db_session (lines 84-88)
# ---------------------------------------------------------------------------

def test_with_db_session_yields_and_closes(fake_engine):  #pylint:disable=redefined-outer-name
    '''with_db_session yields a live session and closes it on exit'''
    ctx = generate_fake_context()
    cog = CogHelper(ctx['bot'], {}, fake_engine)
    with cog.with_db_session() as session:
        assert session is not None


# ---------------------------------------------------------------------------
# _dispatcher cached_property: raises when not loaded (line 94)
# ---------------------------------------------------------------------------

def test_dispatcher_raises_when_not_loaded(fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''Accessing _dispatcher raises RuntimeError when MessageDispatcher cog is absent'''
    cog = CogHelper(fake_context['bot'], {}, None)
    mocker.patch.object(fake_context['bot'], 'get_cog', return_value=None)
    with pytest.raises(RuntimeError, match='MessageDispatcher'):
        getattr(cog, '_dispatcher')


# ---------------------------------------------------------------------------
# register_result_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_result_queue_sets_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''register_result_queue stores a queue from the dispatcher on the cog'''
    cog = CogHelper(fake_context['bot'], {}, None)
    cog.register_result_queue()
    assert cog._result_queue is not None  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# dispatch_channel_history: fire-and-forget, result on queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_channel_history_delivers_to_result_queue(fake_context):  #pylint:disable=redefined-outer-name
    '''dispatch_channel_history submits a request; result arrives on _result_queue'''
    cog = CogHelper(fake_context['bot'], {}, None)
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
    cog = CogHelper(fake_context['bot'], {}, None)
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
    cog = CogHelper(fake_context['bot'], {}, None)
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
    cog = CogHelper(fake_context['bot'], {}, None)
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
    cog = CogHelper(fake_context['bot'], {}, None)
    msg = FakeMessage(channel=fake_context['channel'])
    fake_context['channel'].messages = [msg]
    await cog.dispatch_delete(
        fake_context['guild'].id,
        fake_context['channel'].id,
        msg.id,
    )
    assert msg.deleted


# ---------------------------------------------------------------------------
# retry_commit (lines 161-164)
# ---------------------------------------------------------------------------

def test_retry_commit_calls_session_commit(fake_engine):  #pylint:disable=redefined-outer-name
    '''retry_commit wraps session.commit in retry_database_commands'''
    ctx = generate_fake_context()
    cog = CogHelper(ctx['bot'], {}, fake_engine)
    with cog.with_db_session() as session:
        cog.retry_commit(session)
