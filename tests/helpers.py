import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from random import choice
from pathlib import Path
from string import digits, ascii_lowercase
from tempfile import NamedTemporaryFile
from typing import Any, Generator, Optional
from collections.abc import Callable

from discord import ChannelType
from discord.errors import NotFound
import pytest
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from discord_bot.database import BASE
from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.media_request import MediaRequest
from discord_bot.cogs.music_helpers.media_download import MediaDownload

class HelperException(Exception):
    '''
    Test helper exception
    '''

def random_id(length: int = 12) -> int:
    '''
    Generate a random Discord ID (integer)
    '''
    return int(''.join(choice(digits) for _ in range(length)))

def random_string(length: int = 12) -> str:
    '''
    Generate string of given length
    '''
    return ''.join(choice(ascii_lowercase) for _ in range(length))

def generate_fake_context(bot: Optional[Any] = None) -> dict[str, Any]:
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

def fake_source_dict(fakes: dict[str, Any], download_file: bool = True, is_direct_search: bool = False) -> MediaRequest:
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
def fake_media_download(file_dir: Path, media_request: Optional[MediaRequest] = None, fake_context: Optional[dict[str, Any]] = None, extractor: str = 'youtube', download_file: bool = True, is_direct_search: bool = False) -> Generator[MediaDownload, None, None]:  #pylint:disable=redefined-outer-name
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
def fake_engine() -> Generator[Engine, None, None]:
    engine = create_engine('sqlite+pysqlite:///:memory:')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine

    try:
        yield engine
    finally:
        BASE.metadata.drop_all(engine)
        engine.dispose()

@pytest.fixture(scope="function")
def fake_context() -> Generator[dict[str, Any], None, None]:
    yield generate_fake_context()

@contextmanager
def mock_session(engine: Engine) -> Generator[Session, None, None]:
    session = sessionmaker(bind=engine)()
    try:
        yield session
    finally:
        session.close()

class AsyncIterator():
    def __init__(self, items: list[Any]) -> None:
        self.items = items

    async def __aiter__(self) -> Any:
        for item in self.items:
            yield item

class FakeResponse():
    def __init__(self) -> None:
        self.status = 404
        self.reason = 'Cant find nothing'

class FakeEmjoi():
    def __init__(self) -> None:
        self.id = 1234

class FakeMessage():
    def __init__(self, id: Optional[int] = None, content: Optional[str] = None, channel: Optional[Any] = None, author: Optional[Any] = None, created_at: Optional[datetime] = None) -> None:
        self.id = id or random_id()
        self.created_at = created_at or datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
        self.deleted = False
        self.content = content
        self.channel = channel or FakeChannel()
        if content is None:
            self.content = 'fake message content that was typed by a real human'
        self.author = author or FakeAuthor()
        self.delete_after = None

    async def delete(self) -> bool:
        self.deleted = True
        # Remove this message from the channel's message list
        if self.channel and hasattr(self.channel, 'messages'):
            try:
                self.channel.messages.remove(self)
            except ValueError:
                pass  # Message wasn't in the list
        return True

    async def edit(self, content: str, delete_after: Optional[int] = None) -> None:
        self.content = content
        self.delete_after = delete_after
        return None

class FakeRole():
    def __init__(self, id: Optional[int] = None, name: Optional[str] = None) -> None:
        self.id = id or random_id()
        self.name = name or random_string()
        self.members = []

class FakeBotUser():
    def __init__(self) -> None:
        self.id = random_id()

    def __str__(self) -> str:
        return f'{self.id}'

class FakeGuild():
    def __init__(self, members: Optional[list[Any]] = None, roles: Optional[list[Any]] = None, voice: Optional[Any] = None) -> None:
        self.id = random_id()
        self.name = random_string()
        self.emojis = []
        self.left_guild = False
        self.members = members or []
        self.roles = roles or []
        self.voice_client = voice

    async def leave(self) -> None:
        self.left_guild = True

    async def fetch_emojis(self, **_kwargs: Any) -> list[Any]:
        return self.emojis

    async def fetch_member(self, member_id: int) -> Any:
        for member in self.members:
            if member_id == member.id:
                return member
        raise NotFound(FakeResponse(), 'Unable to find user')

    def get_role(self, role_id: int) -> Any:
        for role in self.roles:
            if role.id == role_id:
                return role
        raise NotFound(FakeResponse(), 'Unable to find role')

class FakeAuthor():
    def __init__(self, id: Optional[int] = None, roles: Optional[list[Any]] = None, bot: bool = False, voice: Optional[Any] = None) -> None:
        self.id = id or random_id()
        self.name = random_string()
        self.display_name = random_string()
        self.bot = bot
        self.roles = roles or []
        self.voice = voice

    async def add_roles(self, role: Any) -> None:
        self.roles.append(role)

    async def remove_roles(self, role: Any) -> None:
        self.roles.remove(role)

class FakeChannel():
    def __init__(self, id: Optional[int] = None, channel_type: ChannelType = ChannelType.text, members: Optional[list[Any]] = None, guild: Optional[Any] = None) -> None:
        self.id = id or random_id()
        self.name = random_string()
        self.messages = []
        self.type = channel_type
        self.members = members
        self.guild = guild or FakeGuild()

    def history(self, **_kwargs: Any) -> AsyncIterator:
        return AsyncIterator(self.messages)

    async def fetch_message(self, message_id: int) -> Any:
        for message in self.messages:
            if message.id == message_id:
                return message
        raise NotFound(FakeResponse(), 'Unable to find message')

    async def connect(self, reconnect: bool = False) -> bool: #pylint:disable=unused-argument
        self.guild.voice_client = FakeVoiceClient()
        await self.guild.voice_client.move_to(self)
        return True

    async def send(self, content: Optional[str] = None, message_content: Optional[str] = None, **_kwargs: Any) -> Any:
        # Support both 'content' (real Discord API) and 'message_content' (legacy) for backwards compatibility
        msg_content = content if content is not None else message_content
        message = FakeMessage(content=msg_content, channel=self)
        self.messages.append(message)
        return message


class FakeIntents():
    def __init__(self) -> None:
        self.members = True


def fake_bot_yielder(start_sleep: int = 0, user: Optional[Any] = None, guilds: Optional[list[Any]] = None, channels: Optional[list[Any]] = None) -> type:
    class FakeBot():
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
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

        async def fetch_channel(self, channel_id: int) -> Optional[Any]:
            for channel in self.channels:
                if channel.id == channel_id:
                    return channel
            return None

        def get_channel(self, channel_id: int) -> Optional[Any]:
            for channel in self.channels:
                if channel.id == channel_id:
                    return channel
            return None

        async def fetch_guild(self, guild_id: int) -> Optional[Any]:
            for guild in self.guilds:
                if guild.id == guild_id:
                    return guild
            return None

        def fetch_guilds(self, **_kwargs: Any) -> AsyncIterator:
            return AsyncIterator(guilds)

        def event(self, func: Callable) -> None:
            self.startup_functions.append(func)

        def is_closed(self) -> bool:
            return self.bot_closed

        async def start(self, token: str) -> None:
            self.token = token
            for func in self.startup_functions:
                await func()
            await asyncio.sleep(start_sleep)

        async def __aenter__(self) -> None:
            pass

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def add_cog(self, cog: Any) -> None:
            self.cogs.append(cog)

        async def wait_until_ready(self) -> bool:
            return True

    return FakeBot

class FakeVoiceClient():
    def __init__(self) -> None:
        self.channel = None

    def play(self, *_args: Any, after: Optional[Callable] = None, **_kwargs: Any) -> bool:
        if after:
            after()
        return True

    def is_playing(self) -> bool:
        return True

    def stop(self) -> bool:
        return True

    async def move_to(self, channel: Any) -> bool:
        self.channel = channel
        return True

    def cleanup(self) -> bool:
        """Mock cleanup method for VoiceClient"""
        return True

    async def disconnect(self) -> bool:
        """Mock disconnect method for VoiceClient"""
        return True

class FakeContext():
    def __init__(self, bot: Optional[Any] = None, guild: Optional[Any] = None, author: Optional[Any] = None, voice_client: Optional[Any] = None, channel: Optional[Any] = None) -> None:
        self.author = author or FakeAuthor()
        self.guild = guild or FakeGuild()
        self.channel = channel or FakeChannel()
        self.messages_sent = []
        self.bot = bot
        self.voice_client = voice_client or FakeVoiceClient()

    async def send(self, message: str) -> str:
        self.messages_sent.append(message)
        return message
