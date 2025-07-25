import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from random import choice
from string import digits, ascii_lowercase

from discord import ChannelType
from discord.errors import NotFound
import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from discord_bot.database import BASE

def random_id(length=12):
    '''
    Generate a random id of the given length
    '''
    return ''.join(choice(digits) for _ in range(length))

def random_string(length=12):
    '''
    Generate string of given length
    '''
    return ''.join(choice(ascii_lowercase) for _ in range(length))

def generate_fake_context(bot=None):
    '''
    Generate Fake Context
    '''
    fake_bot_user = FakeBotUser()
    fake_guild = FakeGuild()
    fake_author = FakeAuthor()
    fake_channel = FakeChannel(members=[fake_bot_user, fake_author], guild=fake_guild)
    fake_context = FakeContext(author=fake_author, guild=fake_guild, channel=fake_channel)

    if bot is None:
        bot = fake_bot_yielder(guilds=[fake_guild], channel=fake_channel, user=fake_bot_user)()
    return {
        'bot': bot,
        'guild': fake_guild,
        'author': fake_author,
        'channel': fake_channel,
        'context': fake_context,
    }

@pytest.fixture(scope="function")
def fake_engine():
    engine = create_engine(f'sqlite+pysqlite:///:memory:')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine

    try:
        yield engine
    finally:
        BASE.metadata.drop_all(engine)

@contextmanager
def mock_session(engine):
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()

class AsyncIterator():
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item

class FakeResponse():
    def __init__(self):
        self.status = 404
        self.reason = 'Cant find nothing'

class FakeEmjoi():
    def __init__(self):
        self.id = 1234

class FakeMessage():
    def __init__(self, id=None, content=None, channel=None, author=None, created_at=None):
        self.id = id or random_id()
        self.created_at = created_at or datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
        self.deleted = False
        self.content = content
        self.channel = channel or FakeChannel()
        if content is None:
            self.content = 'fake message content that was typed by a real human'
        self.author = author or FakeAuthor()
        self.delete_after = None

    async def delete(self):
        self.deleted = True
        return True

    async def edit(self, content, delete_after=None):
        self.content = content
        self.delete_after = delete_after
        return None

class FakeRole():
    def __init__(self, id=None, name=None):
        self.id = id or random_id()
        self.name = name or random_string()
        self.members = []

class FakeBotUser():
    def __init__(self):
        self.id = random_id()

    def __str__(self):
        return f'{self.id}'

class FakeGuild():
    def __init__(self, members=None, roles=None, voice=None):
        self.id = random_id()
        self.name = random_string()
        self.emojis = []
        self.left_guild = False
        self.members = members or []
        self.roles = roles or []
        self.voice_client = voice

    async def leave(self):
        self.left_guild = True

    async def fetch_emojis(self, **_kwargs):
        return self.emojis

    async def fetch_member(self, member_id):
        for member in self.members:
            if member_id == member.id:
                return member
        raise NotFound(FakeResponse(), 'Unable to find user')

    def get_role(self, role_id):
        for role in self.roles:
            if role.id == role_id:
                return role
        raise NotFound(FakeResponse(), 'Unable to find role')

class FakeAuthor():
    def __init__(self, id=None, roles=None, bot=False, voice=None):
        self.id = id or random_id()
        self.name = random_string()
        self.display_name = random_string()
        self.bot = bot
        self.roles = roles or []
        self.voice = voice

    async def add_roles(self, role):
        self.roles.append(role)

    async def remove_roles(self, role):
        self.roles.remove(role)

class FakeChannel():
    def __init__(self, id=None, channel_type=ChannelType.text, members=None, guild=None):
        self.id = id or random_id()
        self.messages = []
        self.type = channel_type
        self.members = members
        self.guild = guild or FakeGuild()

    def history(self, **_kwargs):
        return AsyncIterator(self.messages)

    async def fetch_message(self, message_id):
        for message in self.messages:
            if message.id == message_id:
                return message
        raise NotFound(FakeResponse(), 'Unable to find message')

    async def connect(self, reconnect=False): #pylint:disable=unused-argument
        return True

    async def send(self, message_content, **_kwargs):
        message = FakeMessage(content=message_content)
        self.messages.append(message)
        return message


class FakeIntents():
    def __init__(self):
        self.members = True


def fake_bot_yielder(start_sleep=0, user=None, guilds=None, channel=None):
    class FakeBot():
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.user = user or FakeBotUser()
            self.cogs = []
            self.guilds = guilds or []
            self.token = None
            self.channel = channel
            if guilds:
                self.guild = guilds[0]
            self.intents = FakeIntents()
            self.bot_closed = False
            self.loop = None

        async def fetch_channel(self, _channel_id):
            return self.channel

        async def fetch_guild(self, guild_id):
            for guild in self.guilds:
                if guild.id == guild_id:
                    return guild
            return None

        def fetch_guilds(self, **_kwargs):
            return AsyncIterator(guilds)

        def event(self, func):
            self.startup_functions.append(func)

        def is_closed(self):
            return self.bot_closed

        async def start(self, token):
            self.token = token
            for func in self.startup_functions:
                await func()
            await asyncio.sleep(start_sleep)

        async def __aenter__(self):
            pass

        async def __aexit__(self, *args):
            pass

        async def add_cog(self, cog):
            self.cogs.append(cog)

        async def wait_until_ready(self):
            return True

    return FakeBot

class FakeVoiceClient():
    def __init__(self):
        self.channel = None

    def play(self, *_args, after=None, **_kwargs):
        if after:
            after()
        return True

    def is_playing(self):
        return True

    def stop(self):
        return True

    async def move_to(self, channel):
        self.channel = channel
        return True

class FakeContext():
    def __init__(self, bot=None, guild=None, author=None, voice_client=None, channel=None):
        self.author = author or FakeAuthor()
        self.guild = guild or FakeGuild()
        self.channel = channel or FakeChannel()
        self.messages_sent = []
        self.bot = bot
        self.voice_client = voice_client or FakeVoiceClient()

    async def send(self, message):
        self.messages_sent.append(message)
        return message
