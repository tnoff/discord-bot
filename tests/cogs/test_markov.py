from datetime import datetime, timezone
from tempfile import NamedTemporaryFile

from discord import ChannelType
from freezegun import freeze_time
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.cogs.markov import clean_message, Markov
from discord_bot.database import BASE, MarkovChannel, MarkovRelation

from tests.helpers import fake_bot_yielder, generate_fake_context, fake_engine
from tests.helpers import FakeContext, FakeGuild, FakeEmjoi, FakeChannel, FakeMessage

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
@pytest.fixture(scope="function")
async def test_turn_on(fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    cog = Markov(fakes['bot'], config, fake_engine)
    result = await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned on for channel'
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovChannel).count() == 1
    result = await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Channel already has markov turned on'

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_turn_on_invalid_channel(fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    cog = Markov(fakes['bot'], config, fake_engine)
    result = await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Not a valid markov channel, cannot turn on markov'

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_server_reject_list(fake_engine):
    fakes = generate_fake_context()
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
        'markov': {
            'server_reject_list': [
                fakes['guild'].id,
            ]
        }
    }
    cog = Markov(fakes['bot'], config, fake_engine)
    result = await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Unable to turn on markov for server, in reject list'
    result = await cog.speak(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Unable to use markov for server, in reject list'

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_turn_off(fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
    cog = Markov(fake_bot, config, fake_engine)
    result = await cog.off(cog, FakeContext()) #pylint: disable=too-many-function-args
    assert result == 'Channel does not have markov turned on'

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_turn_on_and_off(fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    result = await cog.off(cog, fakes['context']) #pylint: disable=too-many-function-args
    assert result == 'Markov turned off for channel'

@pytest.mark.asyncio
@pytest.fixture(scope="function")
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync(mocker, fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() > 0

@pytest.mark.asyncio
@pytest.fixture(scope="function")
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_messages(mocker, fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_turn_on_and_sync_multiple_times(mocker, fake_engine, freezer):
    freezer.move_to('2024-12-01 12:00:00')
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    fake_message = FakeMessage(content='this is a basic test', channel=fakes['channel'],
                               created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]

    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args

    new_fake_message = FakeMessage(content='another basic message', channel=fakes['channel'],
                                   created_at=datetime(2024, 11, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages.append(new_fake_message)
    freezer.move_to('2024-12-02 12:00:00')
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 13

@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def test_turn_on_and_sync_message_dissapears(mocker, fake_engine, freezer):
    freezer.move_to('2024-12-01 12:00:00')
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    fake_message = FakeMessage(content='this is a basic test', channel=fakes['channel'],
                               created_at=datetime(2024, 11, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args

    fakes['channel'].messages = []
    freezer.move_to('2024-12-02 12:00:00')
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@pytest.fixture(scope="function")
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_bot_command(mocker, fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }

    fakes = generate_fake_context()
    fake_message = FakeMessage(content='!test command', channel=fakes['channel'],
                               created_at=datetime(2024, 11, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@pytest.fixture(scope="function")
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_content(mocker, fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    fake_message = FakeMessage(content='', channel=fakes['channel'],
                               created_at=datetime(2024, 11, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@pytest.fixture(scope="function")
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_too_long_words(mocker, fake_engine):
    config = {
        'general': {
            'include': {
                'markov': True
            }
        },
    }
    fakes = generate_fake_context()
    fake_message = FakeMessage(content=f'{"a" * 300} foo bar {"b" * 300} bar bar foo foo',
                               author=fakes['channel'],
                               created_at=datetime(2024, 11, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
    cog = Markov(fakes['bot'], config, fake_engine)
    await cog.on(cog, fakes['context']) #pylint: disable=too-many-function-args
    mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
    await cog.markov_message_check() #pylint: disable=too-many-function-args
    session = sessionmaker(bind=fake_engine)()
    assert session.query(MarkovRelation).count() == 4

def mock_random(input_list):
    return input_list[0]

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_and_speak(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        result = await cog.speak(cog, FakeContext())
        assert result == 'this is an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message, an example message,'
        assert len(result.split(' ')) == 32


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_invalid_first_word(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        result = await cog.speak(cog, FakeContext(), 'non-existing')
        assert result == 'No markov word matching "non-existing"'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_multi_first_word(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        result = await cog.speak(cog, FakeContext(), 'funny you want an example')
        assert len(result.split(' ')) == 32

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_sync_speak_sentence_length(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_message = FakeMessage(content='this is an example message, an example of what you can say, if you were a real human')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        mocker.patch('discord_bot.cogs.markov.choice', side_effect=mock_random)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        result = await cog.speak(cog, FakeContext(), sentence_length=5)
        assert len(result.split(' ')) == 5

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_speak_no_words():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        result = await cog.speak(cog, FakeContext(), sentence_length=5)
        assert result == 'No markov words to pick from'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_none_on():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_bot = fake_bot_yielder(guilds=[fake_guild])()
        cog = Markov(fake_bot, config, engine)
        result = await cog.list_channels(cog, FakeContext()) #pylint: disable=too-many-function-args
        assert result == 'Markov not enabled for any channels in server'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_list_channels_with_valid_output():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
        }
        fake_emoji = FakeEmjoi()
        fake_guild = FakeGuild(emojis=[fake_emoji])
        fake_context = FakeContext()
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, config, engine)
        await cog.on(cog, fake_context) #pylint: disable=too-many-function-args
        # Clear messages sent before next bit
        fake_context.messages_sent = []
        result = await cog.list_channels(cog, fake_context) #pylint: disable=too-many-function-args
        assert fake_context.messages_sent == ['Channel\n----------------------------------------------------------------\n<#fake-channel-id-123>']
        assert result is True
