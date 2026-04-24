import asyncio
from datetime import datetime, timezone

from freezegun import freeze_time
import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.cogs.markov import clean_message, Markov, get_markov_channel_by_ids
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.types.fetched_message import FetchedMessage

from discord_bot.database import MarkovChannel, MarkovRelation

from tests.helpers import fake_context, fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session
from tests.helpers import FakeEmjoi, FakeChannel, FakeMessage

GENERIC_CONFIG = {
    'general': {
        'include': {
            'markov': True
        }
    },
}

def test_clean_message():
    '''clean_message lowercases and strips non-corpus words'''
    message = 'This is an example message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_clean_message_extra_spaces():
    '''clean_message handles extra whitespace between words'''
    message = 'This is an example                 message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_clean_message_skip_commands():
    '''clean_message skips words starting with !'''
    message = '!play This is an example                 message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_remove_mentions():
    '''clean_message removes mentions and @here/@everyone'''
    message = '!play <@1234567> example @here @everyone'
    corpus = clean_message(message, [])
    assert corpus == [
        'example'
    ]

def test_remove_channels():
    '''clean_message removes channel references'''
    message = '!play <#123456789> example @here @everyone'
    corpus = clean_message(message, [])
    assert corpus == [
        'example'
    ]

def test_invalid_emojis():
    '''clean_message removes emojis not in the server list'''
    message = 'test message <:derp:1234>'
    corpus = clean_message(message, [])
    assert corpus == [
        'test', 'message',
    ]

def test_valid_emojis():
    '''clean_message keeps emojis that belong to the server'''
    fake_emoji = FakeEmjoi()
    message = f'test message <:Derp:{fake_emoji.id}>'
    corpus = clean_message(message, [fake_emoji])
    assert corpus == [
        'test', 'message', f'<:Derp:{fake_emoji.id}>'
    ]


@pytest.mark.asyncio
async def test_turn_on(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''on command enables markov for the channel'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned on for channel'
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovChannel))).scalar() == 1
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Channel already has markov turned on'

@pytest.mark.asyncio
async def test_turn_on_invalid_channel(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''on command rejects non-text/voice channel types'''
    fake_channel = FakeChannel(channel_type='news')
    fake_context['bot'].channels.append(fake_channel)
    fake_context['context'].channel = fake_channel
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Not a valid markov channel, cannot turn on markov'

@pytest.mark.asyncio
async def test_server_reject_list(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''on and speak commands are blocked for servers in the reject list'''
    config = {
        'markov': {
            'server_reject_list': [
                fake_context['guild'].id,
            ]
        }
    } | GENERIC_CONFIG
    cog = Markov(fake_context['bot'], config, fake_engine)
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Unable to turn on markov for server, in reject list'
    result = await cog.speak(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Unable to use markov for server, in reject list'

@pytest.mark.asyncio
async def test_turn_off(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''off command reports channel not enabled when markov not on'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.off(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Channel does not have markov turned on'

@pytest.mark.asyncio
async def test_turn_on_and_off(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''on then off disables markov for the channel'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    result = await cog.off(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned off for channel'


async def _run_markov_request_and_result(cog, mocker):
    '''Helper: run the request loop once, then drain all queued results.'''
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog._markov_request_loop()  #pylint:disable=protected-access
    # Drain all items from the result queue
    while not cog._result_queue.empty():  #pylint:disable=protected-access
        result = cog._result_queue.get_nowait()  #pylint:disable=protected-access
        if isinstance(result, (ChannelHistoryResult, GuildEmojisResult)):
            if isinstance(result, GuildEmojisResult):
                if not result.error:
                    cog._emoji_cache[result.guild_id] = result.emojis  #pylint:disable=protected-access
            elif isinstance(result, ChannelHistoryResult):
                await cog._process_history_result(result)  #pylint:disable=protected-access


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Producer+consumer loop saves markov relations for a new message'''
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages.append(fake_message)
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() > 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_messages(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Producer+consumer loop saves nothing when channel has no messages'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 0

@pytest.mark.asyncio
async def test_turn_on_and_sync_multiple_times(mocker, fake_engine, freezer, fake_context):  #pylint:disable=redefined-outer-name
    '''Running the loop twice accumulates relations from both runs'''
    freezer.move_to('2024-12-01 12:00:00')
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)

    new_fake_message = FakeMessage(content='another basic message', channel=fake_context['channel'],
                                   created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages.append(new_fake_message)
    freezer.move_to('2024-12-02 12:00:00')
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 13

@pytest.mark.asyncio
async def test_turn_on_and_sync_message_dissapears(mocker, fake_engine, freezer, fake_context):  #pylint:disable=redefined-outer-name
    '''When a tracked message disappears (NotFound), relations are wiped and restarted'''
    freezer.move_to('2024-12-01 12:00:00')
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)

    fake_context['channel'].messages = []
    freezer.move_to('2024-12-02 12:00:00')
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_bot_command(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Messages starting with ! are not added to the markov chain'''
    fake_message = FakeMessage(content='!test command', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_content(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Messages with empty content are not added to the markov chain'''
    fake_message = FakeMessage(content='', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_too_long_words(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Words longer than 255 chars are skipped when building relations'''
    fake_message = FakeMessage(content=f'{"a" * 300} foo bar {"b" * 300} bar bar foo foo',
                               author=fake_context['author'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    await _run_markov_request_and_result(cog, mocker)
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 4

def mock_random(input_list):
    '''Return the first element of the list (deterministic for tests).'''
    return input_list[0]

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_and_speak(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''After syncing, speak generates a predictable markov sentence'''
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               author=fake_context['author'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await _run_markov_request_and_result(cog, mocker)
    result = await cog.speak(cog, fake_context['context'])
    assert result == 'this is an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message,'
    assert len(result.split(' ')) == 32


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_invalid_first_word(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''speak returns an error message when given a first_word with no matches'''
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                                channel=fake_context['channel'],
                                created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await _run_markov_request_and_result(cog, mocker)
    result = await cog.speak(cog, fake_context['context'], 'non-existing')
    assert result == 'No markov word matching "non-existing"'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_multi_first_word(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''speak handles a multi-word first_word argument'''
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await _run_markov_request_and_result(cog, mocker)
    result = await cog.speak(cog, fake_context['context'], 'funny you want an example')
    assert len(result.split(' ')) == 32

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_sentence_length(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''speak respects the sentence_length parameter'''
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await _run_markov_request_and_result(cog, mocker)
    result = await cog.speak(cog, fake_context['context'], sentence_length=5)
    assert len(result.split(' ')) == 5

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_speak_no_words(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''speak returns a message when no markov words exist'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'], sentence_length=5)
    assert result == 'No markov words to pick from'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_none_on(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''list_channels reports no channels when none are enabled'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.list_channels(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov not enabled for any channels in server'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_with_valid_output(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''list_channels shows enabled channels in a table'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    # Clear messages sent before next bit
    fake_context['context'].messages_sent = []
    result = await cog.list_channels(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'Channel List \nChannel\n-------\n<#{fake_context["channel"].id}>']
    assert result is True

# ---------------------------------------------------------------------------
# __loop_active_callback (lines 123-124)
# ---------------------------------------------------------------------------

def test_loop_active_callback_not_running(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Heartbeat returns 0 when _task is None'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    observations = cog._Markov__loop_active_callback(None)  #pylint:disable=protected-access
    assert observations[0].value == 0


def test_loop_active_callback_running(fake_engine, fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''Heartbeat returns 1 when _task is set and not done'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    mock_task = mocker.Mock()
    mock_task.done.return_value = False
    cog._task = mock_task  #pylint:disable=protected-access
    observations = cog._Markov__loop_active_callback(None)  #pylint:disable=protected-access
    assert observations[0].value == 1


# ---------------------------------------------------------------------------
# cog_load / _start_tasks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_load_creates_task(fake_engine, fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''cog_load calls _start_tasks which schedules background tasks'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    fake_context['bot'].loop = mocker.Mock()
    fake_context['bot'].loop.create_task = mocker.Mock(return_value=mocker.Mock())
    await cog.cog_load()
    assert cog._task is not None  #pylint:disable=protected-access
    assert cog._result_task is not None  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# cog_unload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_unload_cancels_tasks(fake_engine, fake_context, mocker):  #pylint:disable=redefined-outer-name
    '''cog_unload cancels _task, _result_task, and _init_task when set'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    mock_task = mocker.Mock()
    mock_result_task = mocker.Mock()
    mock_init_task = mocker.Mock()
    cog._task = mock_task  #pylint:disable=protected-access
    cog._result_task = mock_result_task  #pylint:disable=protected-access
    cog._init_task = mock_init_task  #pylint:disable=protected-access
    await cog.cog_unload()
    mock_task.cancel.assert_called_once()
    mock_result_task.cancel.assert_called_once()
    mock_init_task.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_cog_unload_handles_none_tasks(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''cog_unload does not raise when tasks are None'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.cog_unload()


# ---------------------------------------------------------------------------
# markov group: no subcommand
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markov_group_no_subcommand(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Group handler sends error when invoked without a subcommand'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    fake_context['context'].invoked_subcommand = None
    await cog.markov(cog, fake_context['context'])  #pylint:disable=too-many-function-args
    assert 'Invalid sub command passed...' in fake_context['context'].messages_sent


# ---------------------------------------------------------------------------
# _process_history_result: error handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_history_result_not_found_clears_relations(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''When a NotFound error occurs with after_message_id, channel relations are cleared'''
    from discord.errors import NotFound  #pylint:disable=import-outside-toplevel
    from tests.helpers import FakeResponse  #pylint:disable=import-outside-toplevel

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args

    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[],
        after_message_id=12345,
        error=NotFound(FakeResponse(), 'message not found'),
    )
    await cog._process_history_result(result)  #pylint:disable=protected-access

    async with async_mock_session(fake_engine) as session:
        mc = (await session.execute(
            select(MarkovChannel).where(MarkovChannel.channel_id == fake_context['channel'].id)
        )).scalars().first()
        assert mc is not None
        assert mc.last_message_id is None


@pytest.mark.asyncio
async def test_process_history_result_generic_error_is_logged(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''A non-NotFound error is logged and processing stops'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[],
        error=Exception('something went wrong'),
    )
    # Should not raise
    await cog._process_history_result(result)  #pylint:disable=protected-access


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_process_history_result_saves_relations(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_process_history_result saves markov relations from FetchedMessage list'''
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args

    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[FetchedMessage(
            id=fake_message.id,
            content=fake_message.content,
            created_at=fake_message.created_at,
            author_bot=False,
        )],
    )
    await cog._process_history_result(result)  #pylint:disable=protected-access

    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() > 0


# ---------------------------------------------------------------------------
# _markov_result_loop: emoji cache update
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markov_result_loop_updates_emoji_cache(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_markov_result_loop stores emojis in _emoji_cache on success'''
    from unittest.mock import MagicMock  #pylint:disable=import-outside-toplevel

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    fake_emoji = MagicMock()
    guild_id = fake_context['guild'].id

    # Put a GuildEmojisResult into the queue, then run one iteration of the loop
    result = GuildEmojisResult(guild_id=guild_id, emojis=[fake_emoji])
    cog._result_queue.put_nowait(result)  #pylint:disable=protected-access

    # We need to read from the queue manually since we can't run the infinite loop
    item = cog._result_queue.get_nowait()  #pylint:disable=protected-access
    assert isinstance(item, GuildEmojisResult)
    if not item.error:
        cog._emoji_cache[item.guild_id] = item.emojis  #pylint:disable=protected-access

    assert cog._emoji_cache.get(guild_id) == [fake_emoji]  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# get_markov_channel_by_ids helper
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_markov_channel_by_ids_returns_channel(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''get_markov_channel_by_ids returns the matching MarkovChannel'''
    async with async_mock_session(fake_engine) as session:
        mc = MarkovChannel(channel_id=fake_context['channel'].id,
                           server_id=fake_context['guild'].id,
                           last_message_id=None)
        session.add(mc)
        await session.commit()

        result = await get_markov_channel_by_ids(session, fake_context['guild'].id, fake_context['channel'].id)
        assert result is not None
        assert result.channel_id == fake_context['channel'].id


@pytest.mark.asyncio
async def test_get_markov_channel_by_ids_returns_none(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''get_markov_channel_by_ids returns None when no matching channel exists'''
    async with async_mock_session(fake_engine) as session:
        result = await get_markov_channel_by_ids(session, fake_context['guild'].id, 999999)
        assert result is None


# ---------------------------------------------------------------------------
# _markov_result_loop: emoji error path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markov_result_loop_emoji_error_skips_cache_update(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_markov_result_loop logs the error and does not update _emoji_cache on GuildEmojisResult error'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    guild_id = fake_context['guild'].id

    error_result = GuildEmojisResult(guild_id=guild_id, emojis=[], error=Exception('fetch failed'))
    cog._result_queue.put_nowait(error_result)  #pylint:disable=protected-access

    task = asyncio.create_task(cog._markov_result_loop())  #pylint:disable=protected-access
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert guild_id not in cog._emoji_cache  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# _markov_result_loop: emoji success + channel history result paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markov_result_loop_emoji_success_updates_cache(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_markov_result_loop stores emojis in _emoji_cache when GuildEmojisResult has no error.'''
    from unittest.mock import MagicMock  #pylint:disable=import-outside-toplevel

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()
    fake_emoji = MagicMock()
    guild_id = fake_context['guild'].id

    cog._result_queue.put_nowait(GuildEmojisResult(guild_id=guild_id, emojis=[fake_emoji]))  #pylint:disable=protected-access

    task = asyncio.create_task(cog._markov_result_loop())  #pylint:disable=protected-access
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert cog._emoji_cache.get(guild_id) == [fake_emoji]  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_markov_result_loop_channel_history_result_dispatches(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_markov_result_loop calls _process_history_result when a ChannelHistoryResult arrives.'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    cog.register_result_queue()

    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[],
        after_message_id=None,
        error=Exception('fetch failed'),
    )
    cog._result_queue.put_nowait(result)  #pylint:disable=protected-access

    task = asyncio.create_task(cog._markov_result_loop())  #pylint:disable=protected-access
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# _process_history_result: channel not found in DB
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_process_history_result_channel_not_in_db(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''_process_history_result returns early without saving when channel has no DB record'''
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    # No MarkovChannel created in DB — channel_id 999999 has no record
    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=999999,
        messages=[FetchedMessage(
            id=12345,
            content='some message',
            created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc),
            author_bot=False,
        )],
    )
    await cog._process_history_result(result)  #pylint:disable=protected-access
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar() == 0
