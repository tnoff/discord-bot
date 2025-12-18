from datetime import datetime, timezone

from freezegun import freeze_time
import pytest

from discord_bot.cogs.markov import clean_message, Markov

from discord_bot.database import MarkovChannel, MarkovRelation

from tests.helpers import fake_context, fake_engine #pylint:disable=unused-import
from tests.helpers import mock_session
from tests.helpers import FakeEmjoi, FakeChannel, FakeMessage

GENERIC_CONFIG = {
    'general': {
        'include': {
            'markov': True
        }
    },
}

def test_clean_message():
    message = 'This is an example message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_clean_message_extra_spaces():
    message = 'This is an example                 message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_clean_message_skip_commands():
    message = '!play This is an example                 message'
    corpus = clean_message(message, [])
    assert corpus == [
        'this', 'is', 'an', 'example', 'message'
    ]

def test_remove_mentions():
    message = '!play <@1234567> example @here @everyone'
    corpus = clean_message(message, [])
    assert corpus == [
        'example'
    ]

def test_remove_channels():
    message = '!play <#123456789> example @here @everyone'
    corpus = clean_message(message, [])
    assert corpus == [
        'example'
    ]

def test_invalid_emojis():
    message = 'test message <:derp:1234>'
    corpus = clean_message(message, [])
    assert corpus == [
        'test', 'message',
    ]

def test_valid_emojis():
    fake_emoji = FakeEmjoi()
    message = f'test message <:Derp:{fake_emoji.id}>'
    corpus = clean_message(message, [fake_emoji])
    assert corpus == [
        'test', 'message', f'<:Derp:{fake_emoji.id}>'
    ]


@pytest.mark.asyncio
async def test_turn_on(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned on for channel'
    with mock_session(fake_engine) as session:
        assert session.query(MarkovChannel).count() == 1
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Channel already has markov turned on'

@pytest.mark.asyncio
async def test_turn_on_invalid_channel(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_channel = FakeChannel(channel_type='news')
    fake_context['bot'].channels.append(fake_channel)
    fake_context['context'].channel = fake_channel
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Not a valid markov channel, cannot turn on markov'

@pytest.mark.asyncio
async def test_server_reject_list(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
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
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.off(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Channel does not have markov turned on'

@pytest.mark.asyncio
async def test_turn_on_and_off(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    result = await cog.off(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned off for channel'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages.append(fake_message)
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() > 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_messages(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
async def test_turn_on_and_sync_multiple_times(mocker, fake_engine, freezer, fake_context):  #pylint:disable=redefined-outer-name
    freezer.move_to('2024-12-01 12:00:00')
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]

    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args

    new_fake_message = FakeMessage(content='another basic message', channel=fake_context['channel'],
                                   created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages.append(new_fake_message)
    freezer.move_to('2024-12-02 12:00:00')
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 13

@pytest.mark.asyncio
async def test_turn_on_and_sync_message_dissapears(mocker, fake_engine, freezer, fake_context):  #pylint:disable=redefined-outer-name
    freezer.move_to('2024-12-01 12:00:00')
    fake_message = FakeMessage(content='this is a basic test', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args

    fake_context['channel'].messages = []
    freezer.move_to('2024-12-02 12:00:00')
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_bot_command(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='!test command', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_content(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='', channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_too_long_words(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content=f'{"a" * 300} foo bar {"b" * 300} bar bar foo foo',
                               author=fake_context['author'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    with mock_session(fake_engine) as session:
        assert session.query(MarkovRelation).count() == 4

def mock_random(input_list):
    return input_list[0]

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_and_speak(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               author=fake_context['author'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'])
    assert result == 'this is an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message,'
    assert len(result.split(' ')) == 32


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_invalid_first_word(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                                channel=fake_context['channel'],
                                created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'], 'non-existing')
    assert result == 'No markov word matching "non-existing"'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_multi_first_word(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'], 'funny you want an example')
    assert len(result.split(' ')) == 32

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_sentence_length(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human',
                               channel=fake_context['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'], sentence_length=5)
    assert len(result.split(' ')) == 5

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_speak_no_words(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    result = await cog.speak(cog, fake_context['context'], sentence_length=5)
    assert result == 'No markov words to pick from'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_none_on(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    result = await cog.list_channels(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov not enabled for any channels in server'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_with_valid_output(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], GENERIC_CONFIG, fake_engine)
    await cog.on(cog, fake_context['context']) #pylint: disable=too-many-function-args
    # Clear messages sent before next bit
    fake_context['context'].messages_sent = []
    result = await cog.list_channels(cog, fake_context['context']) #pylint: disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'Channel List \nChannel\n-------\n<#{fake_context["channel"].id}>']
    assert result is True
