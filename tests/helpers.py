import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from random import choice
from pathlib import Path
from string import digits, ascii_lowercase
from tempfile import NamedTemporaryFile

from discord import ChannelType
from discord.errors import NotFound
import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from discord_bot.database import BASE
from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.media_request import MediaRequest
from discord_bot.cogs.music_helpers.media_download import MediaDownload

class HelperException(Exception):
    '''
    Test helper exception
    '''

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
    context = FakeContext(author=fake_author, guild=fake_guild, channel=fake_channel)
    # Setup some other bits
    fake_guild.members = [fake_author]
    fake_guild.voice_client = None
    fake_role = FakeRole()
    fake_author.roles = [fake_role]
    fake_guild.roles = [fake_role]
    fake_role.members = [fake_author]


    if bot is None:
        bot = fake_bot_yielder(guilds=[fake_guild], channels=[fake_channel], user=fake_bot_user)()
    context.bot = bot
    return {
        'bot': bot,
        'guild': fake_guild,
        'author': fake_author,
        'channel': fake_channel,
        'context': context,
    }

def fake_source_dict(fakes, download_file=True, is_direct_search=False):
    '''
    Assumes fakes from fake_context
    '''
    search_type = SearchType.SEARCH
    search_string = random_string()
    message_context = MessageContext(fakes['guild'].id, fakes['channel'].id)
    if is_direct_search:
        search_type = SearchType.DIRECT
        search_string = f'https://foo.example/{random_string()}'
    mr = MediaRequest(fakes['guild'].id, fakes['channel'].id, fakes['author'].display_name, fakes['author'].id, search_string, search_string, search_type, download_file=download_file)
    mr.message_context = message_context
    return mr

@contextmanager
def fake_media_download(file_dir, media_request=None, fake_context=None, extractor='youtube', download_file=True, is_direct_search=False):  #pylint:disable=redefined-outer-name
    '''
    Assumes you pass it a random file path for now
    '''
    if media_request is None and fake_context is None:
        raise HelperException('Source dict or fake context must be provided')
    if media_request is None:
        media_request = fake_source_dict(fake_context, download_file=download_file, is_direct_search=is_direct_search)
    with NamedTemporaryFile(dir=file_dir, suffix='.mp3', delete=False) as tmp_file:
        file_path = Path(tmp_file.name)
        file_path.write_text('testing', encoding='utf-8')
        webpage_url = f'https://foo.example/{random_string()}'
        if media_request.search_type == SearchType.DIRECT:
            webpage_url = media_request.search_string
        media_download = MediaDownload(file_path, {
            'duration': 120,
            'webpage_url': webpage_url,
            'title': random_string(),
            'id': random_string(),
            'uploader': random_string(),
            'extractor': extractor,
            },
        media_request)
        yield media_download

@pytest.fixture(scope="function")
def fake_engine():
    engine = create_engine('sqlite+pysqlite:///:memory:')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine

    try:
        yield engine
    finally:
        BASE.metadata.drop_all(engine)
        engine.dispose()

@pytest.fixture(scope="function")
def fake_context():
    yield generate_fake_context()

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
        # Remove this message from the channel's message list
        if self.channel and hasattr(self.channel, 'messages'):
            try:
                self.channel.messages.remove(self)
            except ValueError:
                pass  # Message wasn't in the list
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
        self.name = random_string()
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
        self.guild.voice_client = FakeVoiceClient()
        await self.guild.voice_client.move_to(self)
        return True

    async def send(self, message_content=None, **_kwargs):
        message = FakeMessage(content=message_content, channel=self)
        self.messages.append(message)
        return message


class FakeIntents():
    def __init__(self):
        self.members = True


def fake_bot_yielder(start_sleep=0, user=None, guilds=None, channels=None):
    class FakeBot():
        def __init__(self, *_args, **_kwargs):
            self.startup_functions = []
            self.user = user or FakeBotUser()
            self.cogs = []
            self.guilds = guilds or []
            self.token = None
            self.channels = channels or []
            if guilds:
                self.guild = guilds[0]
            self.intents = FakeIntents()
            self.bot_closed = False
            self.loop = None

        async def fetch_channel(self, channel_id):
            for channel in self.channels:
                if channel.id == channel_id:
                    return channel
            return None

        def get_channel(self, channel_id):
            for channel in self.channels:
                if channel.id == channel_id:
                    return channel
            return None

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

    def cleanup(self):
        """Mock cleanup method for VoiceClient"""
        return True

    async def disconnect(self):
        """Mock disconnect method for VoiceClient"""
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
