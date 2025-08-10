from functools import partial

from discord.errors import NotFound
import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.message_queue import SourceLifecycleStage

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_source_dict
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import FakeResponse


@pytest.mark.asyncio
async def test_message_loop(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    assert await cog.send_messages() is True

@pytest.mark.asyncio
async def test_message_loop_bot_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    cog.bot_shutdown = True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    with pytest.raises(ExitEarlyException) as exc:
        await cog.send_messages()
    assert 'Bot in shutdown and i dont have any more messages, exiting early' in str(exc.value)

@pytest.mark.asyncio
async def test_message_loop_send_single_message(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_single_message([partial(fake_context['channel'].send, 'test message')])
    await cog.send_messages()
    assert fake_context['channel'].messages[0].content == 'test message'

@pytest.mark.asyncio
async def test_message_play_order(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_play_order(fake_context['guild'].id)
    result = await cog.send_messages()
    assert result is True

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    x = fake_source_dict(fake_context)
    cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, fake_context['channel'].send, 'Original message')
    await cog.send_messages()
    assert x.message.content == 'Original message'

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle_delete(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    def delete_message_raise(*args, **kwargs):
        raise NotFound(FakeResponse(), 'Message not found')

    x = fake_source_dict(fake_context)
    cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, delete_message_raise, '')
    assert not await cog.send_messages()
