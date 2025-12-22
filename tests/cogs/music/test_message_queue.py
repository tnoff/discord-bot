from functools import partial

import pytest

from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.common import MultipleMutableType
from discord_bot.cogs.music_helpers.message_context import MessageContext

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_context #pylint:disable=unused-import


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
    mc = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    mc.function = partial(fake_context['channel'].send, 'test message')
    cog.message_queue.send_single_immutable([mc])
    await cog.send_messages()
    assert fake_context['channel'].messages[0].content == 'test message'

@pytest.mark.asyncio
async def test_message_play_order(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.update_multiple_mutable(
        f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}',
        fake_context["channel"]
    )
    result = await cog.send_messages()
    assert result is True

@pytest.mark.asyncio
async def test_message_content_length_validation_under_limit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that messages under 2000 characters pass through unchanged"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a message with content under 2000 characters
    short_content = "A" * 1500
    index_name = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    cog.message_queue.update_multiple_mutable(index_name, fake_context["channel"])

    # Mock the message content to return our test content
    mocker.patch.object(cog.message_queue, 'update_mutable_bundle_content',
                       return_value=[partial(fake_context['channel'].send, content=short_content)])

    # Send the message
    await cog.send_messages()

    # Verify the message was sent with original content
    assert len(fake_context['channel'].messages) == 1
    assert fake_context['channel'].messages[0].content == short_content

@pytest.mark.asyncio
async def test_message_content_length_validation_over_limit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that messages over 2000 characters are truncated to 1900"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a message with content over 2000 characters
    long_content = "B" * 2500
    index_name = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    cog.message_queue.update_multiple_mutable(index_name, fake_context["channel"])

    # Create a mock send function to verify truncation
    async def mock_send(**kwargs):
        return await fake_context['channel'].send(**kwargs)

    # Mock the message content to return our test content
    mocker.patch.object(cog.message_queue, 'update_mutable_bundle_content',
                       return_value=[partial(mock_send, content=long_content)])

    # Send the message
    await cog.send_messages()

    # Verify the message was sent with truncated content (1900 chars)
    assert len(fake_context['channel'].messages) == 1
    assert len(fake_context['channel'].messages[0].content) == 1900
    assert fake_context['channel'].messages[0].content == "B" * 1900

@pytest.mark.asyncio
async def test_message_content_length_validation_logs_warning(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a warning is logged when content exceeds 2000 characters"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a message with content over 2000 characters
    long_content = "C" * 2100
    index_name = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    cog.message_queue.update_multiple_mutable(index_name, fake_context["channel"])

    # Create a mock send function
    async def mock_send(**kwargs):
        return await fake_context['channel'].send(**kwargs)

    # Mock the message content to return our test content
    mocker.patch.object(cog.message_queue, 'update_mutable_bundle_content',
                       return_value=[partial(mock_send, content=long_content)])

    # Mock the logger to capture warnings
    logger_spy = mocker.spy(cog.logger, 'warning')

    # Send the message
    await cog.send_messages()

    # Verify that logger.warning was called
    assert logger_spy.call_count == 2  # Once for the summary, once for the full content

    # Verify the warning message contains expected information
    warning_calls = [call.args[0] for call in logger_spy.call_args_list]
    assert any('Message content exceeds 2000 chars (length: 2100)' in call for call in warning_calls)
    assert any('Full content message:' in call for call in warning_calls)

@pytest.mark.asyncio
async def test_message_content_length_validation_exactly_2000(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that messages exactly at 2000 characters pass through unchanged"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a message with exactly 2000 characters (Discord's actual limit)
    exact_content = "D" * 2000
    index_name = f'{MultipleMutableType.PLAY_ORDER.value}-{fake_context["guild"].id}'
    cog.message_queue.update_multiple_mutable(index_name, fake_context["channel"])

    # Create a mock send function
    async def mock_send(**kwargs):
        return await fake_context['channel'].send(**kwargs)

    # Mock the message content to return our test content
    mocker.patch.object(cog.message_queue, 'update_mutable_bundle_content',
                       return_value=[partial(mock_send, content=exact_content)])

    # Send the message
    await cog.send_messages()

    # Verify the message was sent without truncation (2000 is the exact limit)
    assert len(fake_context['channel'].messages) == 1
    # Content should NOT be truncated since 2000 is the Discord limit
    assert len(fake_context['channel'].messages[0].content) == 2000
    assert fake_context['channel'].messages[0].content == exact_content
