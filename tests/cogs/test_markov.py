import logging
from tempfile import NamedTemporaryFile

from freezegun import freeze_time
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.cogs.markov import clean_message, Markov
from discord_bot.database import BASE, MarkovChannel, MarkovRelation

from tests.helpers import fake_bot_yielder, FakeContext, FakeGuild, FakeEmjoi, FakeChannel, FakeMessage

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
async def test_turn_on():
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
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        result = await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        assert result == 'Markov turned on for channel'
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovChannel).count() == 1
        result = await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        assert result == 'Channel already has markov turned on'

@pytest.mark.asyncio
async def test_server_reject_list():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine

        fake_guild = FakeGuild()
        config = {
            'general': {
                'include': {
                    'markov': True
                }
            },
            'markov': {
                'server_reject_list': [
                    fake_guild.id,
                ]
            }
        }
        fake_bot = fake_bot_yielder()()
        cog = Markov(fake_bot, logging, config, engine)
        result = await cog.on(cog, FakeContext(fake_guild=fake_guild)) #pylint: disable=too-many-function-args
        assert result == 'Unable to turn on markov for server, in reject list'
        result = await cog.speak(cog, FakeContext(fake_guild=fake_guild)) #pylint: disable=too-many-function-args
        assert result == 'Unable to use markov for server, in reject list'

@pytest.mark.asyncio
async def test_turn_off():
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
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        result = await cog.off(cog, FakeContext()) #pylint: disable=too-many-function-args
        assert result == 'Channel does not have markov turned on'

@pytest.mark.asyncio
async def test_turn_on_and_off():
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
        fake_channel = FakeChannel()
        fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        result = await cog.off(cog, FakeContext()) #pylint: disable=too-many-function-args
        assert result == 'Markov turned off for channel'

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync(mocker):
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
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() > 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_messages(mocker):
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
        fake_channel = FakeChannel(no_messages=True)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_multiple_times(mocker):
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
        fake_message = FakeMessage(content='this is a basic test')
        new_fake_message = FakeMessage(content='another basic message')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        fake_channel.messages.append(new_fake_message)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() == 13


@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_message_dissapears(mocker):
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
        fake_message = FakeMessage(content='this is a basic test')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args

        fake_channel.messages = []
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_bot_command(mocker):
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
        fake_message = FakeMessage(content='!test command')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_no_content(mocker):
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
        fake_message = FakeMessage(content='')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() == 0

@pytest.mark.asyncio
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_turn_on_and_sync_too_long_words(mocker):
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
        fake_message = FakeMessage(content=f'{"a" * 300} foo bar {"b" * 300} bar bar foo foo')
        fake_channel = FakeChannel(fake_message=fake_message)
        fake_bot = fake_bot_yielder(guilds=[fake_guild], fake_channel=fake_channel)()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext()) #pylint: disable=too-many-function-args
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check() #pylint: disable=too-many-function-args
        session = sessionmaker(bind=engine)()
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
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
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, fake_context) #pylint: disable=too-many-function-args
        # Clear messages sent before next bit
        fake_context.messages_sent = []
        result = await cog.list_channels(cog, fake_context) #pylint: disable=too-many-function-args
        assert fake_context.messages_sent == ['Channel\n----------------------------------------------------------------\n<#fake-channel-id-123>']
        assert result is True
