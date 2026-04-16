import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from random import choice
from pathlib import Path
from string import digits, ascii_lowercase
import os
import tempfile
from tempfile import NamedTemporaryFile
from typing import Any, AsyncGenerator, Generator, Optional
from collections.abc import Callable

from unittest.mock import AsyncMock
from discord import ChannelType
from discord.errors import NotFound
import pytest
import pytest_asyncio
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker, AsyncEngine

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.database import BASE
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult

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

def fake_source_dict(fakes: dict[str, Any], is_direct_search: bool = False) -> MediaRequest:
    '''
    Assumes fakes from fake_context
    '''
    search_type = SearchType.SEARCH
    search_string = random_string()
    if is_direct_search:
        search_type = SearchType.DIRECT
        search_string = f'https://foo.example/{random_string()}'
    search_result = SearchResult(search_type=search_type, raw_search_string=search_string)
    mr = MediaRequest(guild_id=fakes['guild'].id, channel_id=fakes['channel'].id, requester_name=fakes['author'].display_name, requester_id=fakes['author'].id, search_result=search_result)
    return mr

@contextmanager
def fake_media_download(file_dir: Path, media_request: Optional[MediaRequest] = None, fake_context: Optional[dict[str, Any]] = None, extractor: str = 'youtube', is_direct_search: bool = False) -> Generator[MediaDownload, None, None]:  #pylint:disable=redefined-outer-name
    '''
    Assumes you pass it a random file path for now
    '''
    if media_request is None and fake_context is None:
        raise HelperException('Source dict or fake context must be provided')
    if media_request is None:
        media_request = fake_source_dict(fake_context, is_direct_search=is_direct_search)
    with NamedTemporaryFile(dir=file_dir, suffix='.mp3', delete=False) as tmp_file:
        file_path = Path(tmp_file.name)
        file_path.write_text('testing', encoding='utf-8')
        webpage_url = f'https://foo.example/{random_string()}'
        if media_request.search_result.search_type == SearchType.DIRECT:
            webpage_url = media_request.search_result.resolved_search_string
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

@pytest_asyncio.fixture(scope="function")
async def fake_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture(scope="function")
async def fake_async_file_engine() -> AsyncGenerator[AsyncEngine, None]:
    '''File-based async SQLite engine — required when _create_sqlite_snapshot must work.'''
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    engine = create_async_engine(f'sqlite+aiosqlite:///{db_path}', poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()
        os.unlink(db_path)

@pytest.fixture(scope="function")
def fake_sync_engine() -> Generator[Engine, None, None]:
    fd, db_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    engine = create_engine(f'sqlite:///{db_path}', poolclass=NullPool)
    BASE.metadata.create_all(engine)
    try:
        yield engine
    finally:
        BASE.metadata.drop_all(engine)
        engine.dispose()
        os.unlink(db_path)

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

@asynccontextmanager
async def async_mock_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

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
        self.messages_sent = []
        self.type = channel_type
        self.members = members
        self.guild = guild or FakeGuild()

    def history(self, **_kwargs: Any) -> AsyncIterator:
        return AsyncIterator(self.messages)

    def get_partial_message(self, message_id: int) -> Any:
        for message in self.messages:
            if message.id == message_id:
                return message
        return None

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


class FakePartialMessageable():
    '''Minimal stand-in for discord.PartialMessageable (no gateway cache required).'''
    def __init__(self, channel_id: int) -> None:
        self.id = channel_id

    async def send(self, **_kwargs: Any) -> 'FakeMessage':
        return FakeMessage(content=_kwargs.get('content', ''), channel=self)

    def history(self, **_kwargs: Any) -> 'AsyncIterator':
        return AsyncIterator([])

    def get_partial_message(self, message_id: int) -> Any:
        msg = AsyncMock()
        msg.id = message_id
        return msg


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

        def get_partial_messageable(self, channel_id: int) -> Any:
            for channel in self.channels:
                if channel.id == channel_id:
                    return channel
            return FakePartialMessageable(channel_id)

        async def fetch_guild(self, guild_id: int) -> Optional[Any]:
            for guild in self.guilds:
                if guild.id == guild_id:
                    return guild
            return None

        def fetch_guilds(self, **_kwargs: Any) -> AsyncIterator:
            return AsyncIterator(guilds)

        def event(self, func: Callable) -> None:
            self.startup_functions.append(func)

        def get_cog(self, name: str) -> Optional[Any]:
            if name == 'MessageDispatcher':
                return FakeMessageDispatcher(self)
            return None

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

class FakeMessageDispatcher():
    '''Synchronous fake dispatcher for tests — processes requests inline.'''
    def __init__(self, bot: Any) -> None:
        self.bot = bot
        self._cog_result_queues: dict = {}

    def register_cog_queue(self, cog_name: str) -> asyncio.Queue:
        '''Create and return a result queue for the named cog.'''
        q: asyncio.Queue = asyncio.Queue()
        self._cog_result_queues[cog_name] = q
        return q

    async def submit_request(self, request: Any) -> None:
        '''Process a typed request inline (synchronous for test predictability).'''
        if isinstance(request, FetchChannelHistoryRequest):
            try:
                channel = await self.bot.fetch_channel(request.channel_id)
                if channel is None:
                    raise Exception(f'Channel {request.channel_id} not found')  # pylint: disable=broad-exception-raised
                after_obj = request.after
                if request.after_message_id is not None:
                    after_obj = await channel.fetch_message(request.after_message_id)
                messages = [m async for m in channel.history(
                    limit=request.limit, after=after_obj, oldest_first=request.oldest_first,
                )]
                result_msgs = [
                    FetchedMessage(id=m.id, content=m.content, created_at=m.created_at, author_bot=m.author.bot)
                    for m in messages
                ]
                result: Any = ChannelHistoryResult(
                    guild_id=request.guild_id,
                    channel_id=request.channel_id,
                    messages=result_msgs,
                    after_message_id=request.after_message_id,
                )
            except Exception as exc:  # pylint: disable=broad-except
                result = ChannelHistoryResult(
                    guild_id=request.guild_id,
                    channel_id=request.channel_id,
                    messages=[],
                    after_message_id=request.after_message_id,
                    error=exc,
                )
            q = self._cog_result_queues.get(request.cog_name)
            if q:
                await q.put(result)
        elif isinstance(request, FetchGuildEmojisRequest):
            try:
                guild = await self.bot.fetch_guild(request.guild_id)
                emojis = await guild.fetch_emojis()
                emoji_result: Any = GuildEmojisResult(guild_id=request.guild_id, emojis=emojis)
            except Exception as exc:  # pylint: disable=broad-except
                emoji_result = GuildEmojisResult(guild_id=request.guild_id, emojis=[], error=exc)
            q = self._cog_result_queues.get(request.cog_name)
            if q:
                await q.put(emoji_result)
        elif isinstance(request, SendRequest):
            self.send_message(request.guild_id, request.channel_id, request.content,
                              delete_after=request.delete_after)
        elif isinstance(request, DeleteRequest):
            self.delete_message(request.guild_id, request.channel_id, request.message_id)

    def send_message(self, _guild_id: int, channel_id: int, content: str, **_kwargs: Any) -> None:
        '''Add content to channel.messages_sent.'''
        channel = self.bot.get_channel(channel_id)
        if channel is not None:
            channel.messages_sent.append(content)

    def delete_message(self, _guild_id: int, channel_id: int, message_id: int) -> None:
        '''Mark and remove message from channel.messages.'''
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            return
        for message in list(getattr(channel, 'messages', [])):
            if message.id == message_id:
                message.deleted = True
                try:
                    channel.messages.remove(message)
                except ValueError:
                    pass
                return

    async def fetch_object(self, _guild_id: int, func: Callable, **_retry_kwargs: Any) -> Any:
        '''Call func and return its result.'''
        return await func()


class FakeDispatchServer:
    '''Minimal dispatcher stand-in for DispatchHttpServer tests that records method calls.'''

    def __init__(self, result_store: dict | None = None):
        self.calls: list = []
        self._result_store = result_store if result_store is not None else {}

    def send_message(self, guild_id, channel_id, content, **_):
        self.calls.append(('send_message', guild_id, channel_id, content))

    def delete_message(self, guild_id, channel_id, message_id, **_):
        self.calls.append(('delete_message', guild_id, channel_id, message_id))

    def update_mutable(self, key, guild_id, content, channel_id, **_):
        self.calls.append(('update_mutable', key, guild_id, content, channel_id))

    def remove_mutable(self, key):
        self.calls.append(('remove_mutable', key))

    def update_mutable_channel(self, key, _guild_id, _new_channel_id):
        self.calls.append(('update_mutable_channel', key))

    async def enqueue_fetch_history(self, request_id, guild_id, channel_id,
                                    after_message_id=None, **_):
        self.calls.append(('enqueue_fetch_history', request_id, guild_id, channel_id))
        self._result_store[request_id] = {
            'guild_id': guild_id, 'channel_id': channel_id,
            'after_message_id': after_message_id, 'messages': [],
        }

    async def enqueue_fetch_emojis(self, request_id, guild_id, **_):
        self.calls.append(('enqueue_fetch_emojis', request_id, guild_id))
        self._result_store[request_id] = {'guild_id': guild_id, 'emojis': []}


class FakeRedisDispatchQueue:
    '''Minimal RedisDispatchQueue stand-in backed by a plain dict.'''

    def __init__(self, result_store: dict | None = None):
        self._results = result_store if result_store is not None else {}

    async def get_result(self, request_id: str) -> dict | None:
        return self._results.get(request_id)


class FakeContext():
    def __init__(self, bot: Optional[Any] = None, guild: Optional[Any] = None, author: Optional[Any] = None, voice_client: Optional[Any] = None, channel: Optional[Any] = None) -> None:
        self.author = author or FakeAuthor()
        self.guild = guild or FakeGuild()
        self.channel = channel or FakeChannel()
        self.bot = bot
        self.voice_client = voice_client or FakeVoiceClient()

    @property
    def messages_sent(self) -> list:
        return self.channel.messages_sent

    @messages_sent.setter
    def messages_sent(self, value: list) -> None:
        self.channel.messages_sent = value

    async def send(self, message: str) -> str:
        self.messages_sent.append(message)
        return message
