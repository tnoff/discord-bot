from datetime import datetime, timedelta, timezone
from logging import getLogger
from types import SimpleNamespace

from freezegun import freeze_time
import pytest

from discord_core.clients.dispatch_client_base import DispatchRemoteError
from discord_core.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult, UNKNOWN_MESSAGE_CODE
from discord_core.types.fetched_message import FetchedMessage

from discord_commands.loop_logic.markov import MarkovLoop, clean_message

GUILD = 11
CHANNEL = 22
NOW = datetime(2024, 12, 1, 12, tzinfo=timezone.utc)


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
    '''clean_message keeps emojis that belong to the server (emojis are dicts, as
    the dispatcher serialises them — object access here caused a silent consumer
    crash + memory leak, see docs findings/2026-07-19)'''
    server_emoji = {'id': 1234, 'name': 'Derp', 'animated': False}
    message = f'test message <:Derp:{server_emoji["id"]}>'
    corpus = clean_message(message, [server_emoji])
    assert corpus == [
        'test', 'message', f'<:Derp:{server_emoji["id"]}>'
    ]

def not_found():
    return DispatchRemoteError.from_payload({
        'error': '404 Not Found (error code: 10008): Unknown Message',
        'error_detail': {'status': 404, 'code': UNKNOWN_MESSAGE_CODE, 'type': 'NotFound'},
    })


class Recorder:
    '''Stands in for the dispatcher: records the emoji and history requests.'''

    def __init__(self):
        self.emoji_requests = []
        self.history_requests = []

    async def request_emojis(self, guild_id, max_retries=3):
        self.emoji_requests.append((guild_id, max_retries))

    async def request_history(self, guild_id, channel_id, **kwargs):
        self.history_requests.append((guild_id, channel_id, kwargs))


def make_loop(mocker, channels=(), recorder=None, emoji_cache=None):
    recorder = recorder or Recorder()
    store = mocker.AsyncMock()
    store.list_channels.return_value = list(channels)
    loop = MarkovLoop(getLogger('test'), store, 5, 16, 30, {} if emoji_cache is None else emoji_cache,
                      recorder.request_emojis, recorder.request_history)
    return loop, store, recorder


def fetched(message_id, content, author_bot=False, created_at=NOW):
    return FetchedMessage(id=message_id, content=content, created_at=created_at, author_bot=author_bot)


def history(messages=(), error=None, after_message_id=None):
    return ChannelHistoryResult(guild_id=GUILD, channel_id=CHANNEL, messages=list(messages),
                                error=error, after_message_id=after_message_id)


@pytest.mark.asyncio
@freeze_time(NOW)
async def test_request_starts_a_new_channel_from_the_retention_cutoff(mocker):
    sleep = mocker.patch('discord_commands.loop_logic.markov.sleep')
    channel = SimpleNamespace(server_id=GUILD, channel_id=CHANNEL, last_message_id=None)
    loop, store, recorder = make_loop(mocker, [channel])
    await loop.request()
    sleep.assert_awaited_once_with(5)
    assert recorder.emoji_requests == [(GUILD, 5)]
    assert recorder.history_requests == [(GUILD, CHANNEL, {'limit': 16, 'after': NOW - timedelta(days=30)})]
    store.prune_relations_before.assert_awaited_once_with(NOW - timedelta(days=30))


@pytest.mark.asyncio
@freeze_time(NOW)
async def test_request_continues_a_known_channel_from_its_last_message(mocker):
    mocker.patch('discord_commands.loop_logic.markov.sleep')
    channel = SimpleNamespace(server_id=GUILD, channel_id=CHANNEL, last_message_id=777)
    loop, _, recorder = make_loop(mocker, [channel])
    await loop.request()
    assert recorder.history_requests == [(GUILD, CHANNEL, {'limit': 16, 'after_message_id': 777})]


@pytest.mark.asyncio
@freeze_time(NOW)
async def test_request_with_no_channels_still_prunes(mocker):
    mocker.patch('discord_commands.loop_logic.markov.sleep')
    loop, store, recorder = make_loop(mocker)
    await loop.request()
    assert not recorder.history_requests
    store.prune_relations_before.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_emojis_result_caches_on_success_only(mocker):
    cache = {}
    loop, _, _ = make_loop(mocker, emoji_cache=cache)
    await loop.process_emojis_result(GuildEmojisResult(guild_id=GUILD, emojis=[{'id': 1}], error=Exception('x')))
    assert not cache
    await loop.process_emojis_result(GuildEmojisResult(guild_id=GUILD, emojis=[{'id': 1}]))
    assert cache == {GUILD: [{'id': 1}]}


@pytest.mark.asyncio
async def test_not_found_with_a_message_id_resets_the_channel(mocker):
    loop, store, _ = make_loop(mocker)
    error = not_found()
    await loop.process_history_result(history(error=error, after_message_id=5))
    store.reset_channel.assert_awaited_once_with(GUILD, CHANNEL)
    store.save_messages.assert_not_called()


@pytest.mark.asyncio
async def test_other_errors_are_logged_not_recovered(mocker):
    loop, store, _ = make_loop(mocker)
    loop.logger = mocker.MagicMock()
    await loop.process_history_result(history(error=Exception('boom'), after_message_id=5))
    await loop.process_history_result(history(error=not_found()))
    store.reset_channel.assert_not_called()
    assert loop.logger.error.call_count == 2


@pytest.mark.asyncio
async def test_no_messages_writes_nothing(mocker):
    loop, store, _ = make_loop(mocker)
    await loop.process_history_result(history())
    store.save_messages.assert_not_called()


@pytest.mark.asyncio
async def test_messages_are_filtered_but_every_one_advances_the_cursor(mocker):
    '''Bot posts, commands and empty messages write no pairs yet still move last_message_id.'''
    loop, store, _ = make_loop(mocker)
    await loop.process_history_result(history([
        fetched(1, 'hello big world'),
        fetched(2, 'a bot said this', author_bot=True),
        fetched(3, '!play something'),
        fetched(4, ''),
    ]))
    store.save_messages.assert_awaited_once()
    guild_id, channel_id, writes = store.save_messages.await_args.args
    assert (guild_id, channel_id) == (GUILD, CHANNEL)
    assert [w.last_message_id for w in writes] == [1, 2, 3, 4]
    assert writes[0].word_pairs == [('hello', 'big'), ('big', 'world'), ('world', 'hello')]
    assert [w.word_pairs for w in writes[1:]] == [[], [], []]


@pytest.mark.asyncio
async def test_a_channel_missing_from_the_store_is_skipped(mocker):
    loop, store, _ = make_loop(mocker)
    store.save_messages.return_value = None
    await loop.process_history_result(history([fetched(1, 'hello world')]))
    store.save_messages.assert_awaited_once()


@pytest.mark.asyncio
async def test_emojis_in_the_cache_are_kept_in_the_corpus(mocker):
    loop, store, _ = make_loop(mocker, emoji_cache={GUILD: [{'id': 1234, 'name': 'Derp', 'animated': False}]})
    await loop.process_history_result(history([fetched(1, 'hi <:Derp:1234>')]))
    writes = store.save_messages.await_args.args[2]
    assert ('hi', '<:Derp:1234>') in writes[0].word_pairs


def test_build_word_pairs_drops_words_too_long_for_the_column(mocker):
    loop, _, _ = make_loop(mocker)
    corpus = ['a' * 300, 'foo', 'bar', 'b' * 300, 'bar', 'bar', 'foo', 'foo']
    assert len(loop.build_word_pairs(corpus)) == 4
