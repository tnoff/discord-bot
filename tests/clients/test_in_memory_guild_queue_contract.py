'''
InMemoryBrokerClient's guild queue must answer exactly as HttpBrokerClient does.

The gateway's tests drive the queue through InMemoryBrokerClient (the real GuildQueueBroker, minus
the HTTP hop), while production reaches it through HttpBrokerClient and the broker's server. Any
difference between those two paths is a way for the gateway suite to pass while prod does
something else, so this runs ONE scenario through both and requires the same observations.

Observations use titles, never uuids: each stack mints its own.
'''
import contextlib
import inspect
from pathlib import Path

import aiohttp
import pytest
from aiohttp.test_utils import TestServer

from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.cogs.music_helpers.common import SearchType
from discord_core.interfaces.guild_queue_client import GuildQueueClient
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_queues import (
    make_broker_http_server, make_guild_queue_broker, make_guild_queue_for,
)
from tests.fakes.in_memory_broker_client import InMemoryBrokerClient

GUILD = 77
BUCKET = 'contract-bucket'
TITLES = ('one', 'two', 'three', 'four')


async def _register(broker, title: str, cache_hit: bool = False) -> str:
    request = MediaRequest(
        guild_id=GUILD, channel_id=2, requester_name='Alice', requester_id=9,
        search_result=SearchResult(search_type=SearchType.DIRECT,
                                   raw_search_string=f'https://example.com/{title}'))
    await broker.register_request(request)
    download = MediaDownload(
        Path(f'{title}.mp3'),
        {'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
         'uploader': 'Someone', 'duration': 90, 'extractor': 'youtube'}, request)
    download.cache_hit = cache_hit
    await broker.register_download(download)
    return str(request.uuid)


class _Stack:
    '''One way of reaching a guild queue, plus the two backdoors a scenario needs.'''

    def __init__(self, client, broker, queue):
        self.client = client
        self._broker = broker
        self._queue = queue

    async def register(self, title: str, **kwargs) -> str:
        return await _register(self._broker, title, **kwargs)

    async def kill_heartbeat(self) -> None:
        '''Let the playing record lapse, as it does when a gateway dies.'''
        await self._queue._queues._client.delete(  # pylint: disable=protected-access
            f'discord_bot:broker:gplaying:{GUILD}')


@contextlib.asynccontextmanager
async def _in_memory():
    broker = AsyncioBroker(bucket_name=BUCKET)
    queue = make_guild_queue_for(broker)
    yield _Stack(InMemoryBrokerClient(broker, guild_queue=queue), broker, queue)


@contextlib.asynccontextmanager
async def _over_http():
    queue = make_guild_queue_broker(BUCKET)
    server = make_broker_http_server(queue.broker, guild_queue=queue)
    async with TestServer(server.build_app()) as running, aiohttp.ClientSession() as session:
        client = HttpBrokerClient(str(running.make_url('')), bucket_name=BUCKET, session=session)
        yield _Stack(client, queue.broker, queue)


STACKS = [pytest.param(_in_memory, id='in-memory'), pytest.param(_over_http, id='over-http')]


async def _scenario(stack: _Stack) -> list:
    """Drive a guild's player through every queue call; return what each one said."""
    client = stack.client
    uuids = {title: await stack.register(title) for title in TITLES}
    titles = {uuid: title for title, uuid in uuids.items()}
    seen = []

    def view(snapshot):
        return (snapshot.version, [d.title for d in snapshot.items], snapshot.text_channel_id,
                snapshot.closed, snapshot.skip_for,
                (titles.get(snapshot.playing.uuid), snapshot.playing.gateway_id,
                 snapshot.playing.download.title if snapshot.playing.download else None)
                if snapshot.playing else None)

    seen.append(('open', await client.open_guild(GUILD, 111)))
    for title in TITLES[:3]:
        seen.append(('enqueue', title, await client.enqueue_track(GUILD, uuids[title], max_size=3)))
    seen.append(('full', await client.enqueue_track(GUILD, uuids['four'], max_size=3)))
    seen.append(('duplicate', await client.enqueue_track(GUILD, uuids['one'], max_size=9)))
    seen.append(('queue', view(await client.get_guild_queue(GUILD))))

    bumped = await client.bump_queued_track(GUILD, uuids['three'])
    seen.append(('bump', bumped.title if bumped else None))
    seen.append(('bump-miss', await client.bump_queued_track(GUILD, 'not-queued')))
    removed = await client.remove_queued_track(GUILD, uuids['two'])
    seen.append(('remove', removed.title if removed else None))
    seen.append(('remove-miss', await client.remove_queued_track(GUILD, uuids['two'])))
    seen.append(('queue', view(await client.get_guild_queue(GUILD))))
    seen.append(('poll', await client.poll_guild_queue(GUILD)))
    version = (await client.poll_guild_queue(GUILD))[0]
    seen.append(('poll-unchanged', await client.poll_guild_queue(GUILD, since=version)))
    seen.append(('poll-stale', await client.poll_guild_queue(GUILD, since=version - 1)))

    claimed = await client.claim_next_track(GUILD, 'gw-1')
    seen.append(('claim', claimed.download.title, claimed.checkout.s3_key, claimed.checkout.bucket_name,
                 claimed.download.media_request.requester_name))
    playing_uuid = str(claimed.download.media_request.uuid)
    seen.append(('heartbeat', await client.playing_heartbeat(GUILD, playing_uuid),
                 await client.playing_heartbeat(GUILD, 'someone-else')))
    seen.append(('queue-while-playing', view(await client.get_guild_queue(GUILD))))
    seen.append(('skip-wrong', await client.skip_track(GUILD, 'someone-else')))
    seen.append(('skip', await client.skip_track(GUILD, playing_uuid)))
    seen.append(('poll-skip', (await client.poll_guild_queue(GUILD, since=version + 99))[1] == playing_uuid))
    await client.finish_track(GUILD, playing_uuid, skipped=True, history_cap=5)
    seen.append(('history-after-skip', await client.get_guild_history(GUILD)))

    second = await client.claim_next_track(GUILD, 'gw-1')
    await client.finish_track(GUILD, str(second.download.media_request.uuid), skipped=False, history_cap=5)
    history = await client.get_guild_history(GUILD)
    seen.append(('history', [(h['title'], h['requester_name'], h['cache_hit'], h['webpage_url'])
                             for h in history]))
    seen.append(('claim-empty', await client.claim_next_track(GUILD, 'gw-1')))

    # A gateway dies mid-track; the next one to open the guild gets that track back first.
    await client.enqueue_track(GUILD, uuids['four'])
    interrupted = await client.claim_next_track(GUILD, 'gw-old')
    await client.open_guild(GUILD, 222)  # heartbeat still alive: no recovery
    await stack.kill_heartbeat()
    recovered = await client.open_guild(GUILD, 222)
    seen.append(('recovered', titles.get(recovered), interrupted.download.title))
    again = await client.claim_next_track(GUILD, 'gw-new')
    seen.append(('reclaim', again.download.title, again.checkout.s3_key))

    await client.enqueue_track(GUILD, await stack.register('five'))
    seen.append(('clear', await client.clear_queue(GUILD)))
    seen.append(('close', await client.close_guild(GUILD)))
    seen.append(('queue-closed', view(await client.get_guild_queue(GUILD))))
    seen.append(('after-close', await client.enqueue_track(GUILD, await stack.register('six'))))
    return seen


@pytest.mark.asyncio
async def test_the_scenario_gives_the_same_answers_through_both_paths():
    """One scenario, two paths, identical observations."""
    results = {}
    for name, opener in (('in-memory', _in_memory), ('over-http', _over_http)):
        async with opener() as stack:
            results[name] = await _scenario(stack)
    assert results['in-memory'] == results['over-http']


@pytest.mark.asyncio
@pytest.mark.parametrize('opener', STACKS)
async def test_the_scenario_says_what_the_gateway_relies_on(opener):
    '''Guard against the comparison above passing because both sides are equally wrong.'''
    async with opener() as stack:
        seen = dict((entry[0], entry[1:]) for entry in await _scenario(stack))
    assert seen['duplicate'] == ('duplicate',)
    assert seen['full'] == ('full',)
    assert seen['claim'][:3] == ('three', 'three.mp3', BUCKET)
    _, queued, channel, closed, skip_for, playing = seen['queue-while-playing'][0]
    assert (queued, channel, closed, skip_for) == (['one'], 111, False, None)
    assert playing == ('three', 'gw-1', 'three')
    assert seen['skip-wrong'] == ('not_current',)
    assert seen['skip'] == ('ok',)
    assert seen['history-after-skip'] == ([],)
    assert seen['recovered'] == ('four', 'four')
    assert seen['reclaim'] == ('four', 'four.mp3')
    assert seen['after-close'] == ('closed',)
    assert seen['poll-unchanged'] == (None,)


def test_the_fake_implements_the_whole_protocol():
    '''Every GuildQueueClient method exists on the fake with the same parameters.'''
    for name, member in inspect.getmembers(GuildQueueClient, inspect.isfunction):
        if name.startswith('_'):
            continue
        implemented = getattr(InMemoryBrokerClient, name)
        assert list(inspect.signature(implemented).parameters) == list(inspect.signature(member).parameters), name


@pytest.mark.asyncio
async def test_a_fake_built_without_a_queue_refuses_rather_than_pretends():
    '''Tests that never asked for a queue must not silently get an empty one.'''
    client = InMemoryBrokerClient(AsyncioBroker())
    with pytest.raises(RuntimeError, match='guild_queue'):
        await client.get_guild_queue(GUILD)
    with pytest.raises(RuntimeError, match='guild_queue'):
        _ = client.guild_queue
