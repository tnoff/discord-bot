import asyncio
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from datetime import datetime, timezone
from random import choice
from pathlib import Path
from string import digits, ascii_lowercase
from tempfile import NamedTemporaryFile
from typing import Any, AsyncGenerator, Generator, Optional
from collections.abc import Callable

from unittest.mock import AsyncMock
from discord import ChannelType
from discord.errors import NotFound
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker, AsyncEngine

from discord_bot.clients.broker_client import InMemoryBrokerClient
from discord_bot.clients.database_stores import DatabaseStores
from discord_bot.clients.guild_analytics_client import GuildAnalyticsClient
from discord_bot.clients.markov_client import MarkovClient
from discord_bot.clients.playlist_client import PlaylistClient
from discord_bot.clients.download_client import InMemoryDownloadClient
from discord_bot.clients.youtube_music_search_client import InMemoryYoutubeMusicSearchClient
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.database import BASE
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)
from discord_bot.clients.dispatch_client_base import DispatchRemoteError
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, encode_error
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.utils.integrations import youtube_music
from discord_bot.workers.asyncio_broker import AsyncioBroker
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker
from discord_bot.workers.asyncio_youtube_music_search_worker import AsyncioYoutubeMusicSearchWorker
from discord_bot.workers.youtube_music_search_driver import YoutubeMusicSearchDriver

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
        'dispatcher': FakeMessageDispatcher(bot),
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

_TRUNCATE_TABLES = ', '.join(f'"{t.name}"' for t in BASE.metadata.sorted_tables)


@pytest_asyncio.fixture(scope="function")
async def fake_engine(pg_test_db_url) -> AsyncGenerator[AsyncEngine, None]:
    '''Async postgres engine with the bot schema, wiped clean before each test.'''
    engine = create_async_engine(pg_test_db_url, poolclass=NullPool)
    if _TRUNCATE_TABLES:
        async with engine.begin() as conn:
            await conn.execute(text(f'TRUNCATE {_TRUNCATE_TABLES} RESTART IDENTITY CASCADE'))
    try:
        yield engine
    finally:
        await engine.dispose()

@pytest.fixture(scope="function")
def fake_context() -> Generator[dict[str, Any], None, None]:
    yield generate_fake_context()


@pytest_asyncio.fixture(scope="function")
async def fake_stores(fake_engine) -> AsyncGenerator[DatabaseStores, None]:  #pylint:disable=redefined-outer-name
    '''
    A DatabaseStores bundle backed by the local test engine.

    Production builds this bundle out of the HTTP stores and points them at the
    db pod. Tests build it out of the in-process clients over `fake_engine`, so
    a cog exercises real queries against real postgres rather than a mock of
    what the db pod might answer. That substitution is the point of the
    Protocols: the cog cannot tell the two apart, and a test that asserts on
    rows afterwards is asserting on the same engine the cog wrote through.

    Request `fake_engine` alongside this fixture to read those rows back.
    '''
    session_generator = partial(async_mock_session, fake_engine)
    yield DatabaseStores(
        playlist=PlaylistClient(session_generator),
        markov=MarkovClient(session_generator),
        guild_analytics=GuildAnalyticsClient(session_generator),
    )

@asynccontextmanager
async def async_mock_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session



def attach_in_process_broker(cog: Any, video_cache: Optional[Any] = None,
                            db_engine: Optional[AsyncEngine] = None) -> AsyncioBroker:
    '''
    Rebuild the in-process broker stack the cog no longer builds itself.

    The cog is an HTTP client only since the broker dual path was collapsed — the
    registry, the bundle state, the video cache and the S3 checkout all live in
    the broker pod. AsyncioBroker, VideoCacheClient and InMemoryBrokerClient
    survive as test doubles (projects/discord-bot-ha-only), and this wires them
    exactly as the cog used to, so tests keep driving real broker behaviour
    through `cog.broker_client` instead of standing up a broker pod behind an
    aiohttp server.

    Returns the engine, which is what tests reach for when they need to assert on
    registry state directly (the cog's old `cog.media_broker`).

    Call this BEFORE attach_in_process_download / attach_in_process_search: both
    read `cog.broker_client` to wire their worker, so attaching them first leaves
    them holding the HttpBrokerClient this replaces.

    video_cache overrides the client built from cog config — pass one to exercise
    the cache path without an enable_cache_files config, or a fake to assert on
    calls.

    db_engine is what the cache gets built over. It has to be passed in now: the
    cog stopped carrying a database handle when persistence moved behind HTTP,
    and the video cache was never the bot's to begin with — it is broker state,
    and this function is standing up the broker. Without an engine the config's
    cache settings are inert, which is exactly what a bot process sees.
    '''
    bucket_name = cog.config.download.storage.bucket_name if cog.config.download.storage else None
    if video_cache is None and cog.config.download.cache.enable_cache_files and db_engine and bucket_name:
        max_mb = cog.config.download.cache.max_cache_size_mb
        video_cache = VideoCacheClient(
            cog.config.download.cache.max_cache_files,
            partial(async_mock_session, db_engine),
            max_cache_size_bytes=(max_mb * 1024 * 1024 if max_mb else None),
            storage_type='s3',
        )
    broker = AsyncioBroker(
        video_cache=video_cache,
        bucket_name=bucket_name,
        dispatcher=cog.dispatcher,
        download_max_retries=cog.config.download.max_download_retries,
        search_max_retries=cog.config.download.max_youtube_music_search_retries,
        message_delete_after=cog.config.general.message_delete_after,
    )
    # Only broker_client is set on the cog: it is real production state, just a
    # different implementation. The engine and the cache are NOT re-attached as
    # cog.media_broker / cog.video_cache — the cog has no such attributes any
    # more, and re-adding them would let a test assert against a shape production
    # cannot have. Tests that need the engine use the returned handle.
    cog.broker_client = InMemoryBrokerClient(broker)
    return broker

def attach_in_process_download(cog: Any, worker_cls: Optional[type] = None) -> InMemoryDownloadClient:
    '''
    Rebuild the in-process download stack the cog no longer builds itself.

    The cog is an HTTP client only since the download dual path was collapsed —
    the consumer loop runs in the downloader pod. AsyncioDownloadWorker and
    InMemoryDownloadClient survive as test doubles (projects/discord-bot-ha-only),
    and this wires them exactly as the cog used to, so tests keep driving the real
    worker with `await cog.download_client.run(...)` instead of standing up a
    downloader pod.

    worker_cls swaps in a fake worker subclass — what the tests used to get by
    patching discord_bot.cogs.music.AsyncioDownloadWorker, which no longer exists
    to patch.
    '''
    bucket_name = cog.config.download.storage.bucket_name if cog.config.download.storage else None
    worker = (worker_cls or AsyncioDownloadWorker)(
        cog.logging_config,
        cog.download_dir,
        queue_max_size=cog.config.player.queue_max_size,
        extra_ytdlp_options=cog.config.download.extra_ytdlp_options,
        max_video_length=cog.config.download.max_video_length,
        banned_video_list=cog.config.download.banned_videos_list,
        failure_queue=FailureQueue(
            max_size=cog.config.download.failure_tracking_max_size,
            max_age_seconds=cog.config.download.failure_tracking_max_age_seconds,
        ),
        wait_period_minimum=cog.config.download.youtube_wait_period_minimum,
        wait_period_max_variance=cog.config.download.youtube_wait_period_max_variance,
        bucket_name=bucket_name,
        normalize_audio=cog.config.download.normalize_audio,
        broker=cog.broker_client,
        max_retries=cog.config.download.max_download_retries,
        retry_backoff_seconds_minimum=cog.config.download.retry_backoff_seconds_minimum,
    )
    cog.download_client = InMemoryDownloadClient(worker)
    return cog.download_client

def attach_in_process_search(cog: Any, client: Optional[Any] = None) -> YoutubeMusicSearchDriver:
    '''
    Rebuild the in-process search stack the cog no longer builds itself.

    The cog is an HTTP client only since the dual-path collapse — the search loop
    runs in the standalone search pod. These in-memory implementations survive as
    test doubles (projects/discord-bot-ha-only): driving the driver against them
    keeps its behaviour under test without standing up an aiohttp server, which is
    what the alternative would cost.

    Swaps the cog's HttpYoutubeMusicSearchClient for an InMemory one over a real
    AsyncioYoutubeMusicSearchWorker, wired exactly as the cog used to wire it, and
    returns the driver that used to be cog.youtube_music_search_driver. Callers
    drive one iteration with `await driver.run_once(cog.bot_shutdown_event)`.
    '''
    worker = AsyncioYoutubeMusicSearchWorker(
        cog.logging_config,
        client or youtube_music.YoutubeMusicClient(),
        FailureQueue(
            max_size=cog.config.download.failure_tracking_max_size,
            max_age_seconds=cog.config.download.failure_tracking_max_age_seconds,
        ),
        cog.config.download.youtube_wait_period_minimum,
        cog.config.download.youtube_wait_period_max_variance,
        queue_max_size=cog.config.player.queue_max_size * 2,
    )
    cog.youtube_music_search_client = InMemoryYoutubeMusicSearchClient(worker)
    return YoutubeMusicSearchDriver(
        cog.youtube_music_search_client,
        cog.broker_client,
        cog.logger,
        max_retries=cog.config.download.max_youtube_music_search_retries,
        queue_priority=cog.server_queue_priority,
    )

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
    def __init__(self, members: Optional[list[Any]] = None, roles: Optional[list[Any]] = None, voice: Optional[Any] = None,
                 channels: Optional[list[Any]] = None) -> None:
        self.id = random_id()
        self.name = random_string()
        self.emojis = []
        self.left_guild = False
        self.members = members or []
        self.roles = roles or []
        self.voice_client = voice
        self.channels = channels or []

    def get_channel(self, channel_id: int) -> Optional[Any]:
        for channel in self.channels:
            if channel.id == channel_id:
                return channel
        return None

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
            self.voice_clients = []

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

        def get_guild(self, guild_id: int) -> Optional[Any]:
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
    def __init__(self, guild: Optional[Any] = None) -> None:
        self.channel = None
        self.guild = guild

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

def _as_remote_error(exc: Exception) -> DispatchRemoteError:
    '''
    Flatten *exc* the way the real dispatcher transports it back to a cog.

    Both the HTTP and in-process dispatchers serialize the exception to JSON and
    rebuild it as a DispatchRemoteError, so a cog never receives the original
    discord exception object. The fake must do the same or tests exercise a code
    path production cannot reach.
    '''
    return DispatchRemoteError.from_payload({'error': str(exc), 'error_detail': encode_error(exc)})


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
                    error=_as_remote_error(exc),
                )
            result.span_context = request.span_context
            q = self._cog_result_queues.get(request.cog_name)
            if q:
                await q.put(result)
        elif isinstance(request, FetchGuildEmojisRequest):
            try:
                guild = await self.bot.fetch_guild(request.guild_id)
                emojis = await guild.fetch_emojis()
                emoji_result: Any = GuildEmojisResult(guild_id=request.guild_id, emojis=emojis)
            except Exception as exc:  # pylint: disable=broad-except
                emoji_result = GuildEmojisResult(guild_id=request.guild_id, emojis=[],
                                                 error=_as_remote_error(exc))
            emoji_result.span_context = request.span_context
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

    def update_mutable(self, _key: str, _guild_id: int, _content: list, _channel_id: int | None,
                       sticky: bool = True, delete_after: int | None = None) -> None:
        '''No-op stand-in for the BundleDispatchSink protocol.'''
        # bandit: explicit no-op for the bundle dispatcher protocol used in tests
        del sticky, delete_after  # unused

    def remove_mutable(self, _key: str) -> None:
        '''No-op stand-in for the BundleDispatchSink protocol.'''

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
