import logging
from tempfile import NamedTemporaryFile

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.cogs.markov import clean_message, Markov
from discord_bot.database import BASE, MarkovChannel, MarkovRelation

from tests.helpers import fake_bot_yielder, FakeContext, FakeGuild, FakeEmjoi, FakeChannel

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
        fake_bot = fake_bot_yielder()()
        cog = Markov(fake_bot, logging, config, engine)
        result = await cog.on(cog, FakeContext())
        assert result == 'Markov turned on for channel'
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovChannel).count() == 1
        result = await cog.on(cog, FakeContext())
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
        result = await cog.on(cog, FakeContext(fake_guild=fake_guild))
        assert result == 'Unable to turn on markov for server, in reject list'
        result = await cog.speak(cog, FakeContext(fake_guild=fake_guild))
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
        fake_bot = fake_bot_yielder()()
        cog = Markov(fake_bot, logging, config, engine)
        result = await cog.off(cog, FakeContext())
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
        fake_bot = fake_bot_yielder()()
        cog = Markov(fake_bot, logging, config, engine)
        await cog.on(cog, FakeContext())
        result = await cog.off(cog, FakeContext())
        assert result == 'Markov turned off for channel'

@pytest.mark.asyncio
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
        await cog.on(cog, FakeContext())
        mocker.patch('discord_bot.cogs.markov.sleep', return_value=True)
        await cog.markov_message_check()
        session = sessionmaker(bind=engine)()
        assert session.query(MarkovRelation).count() > 0