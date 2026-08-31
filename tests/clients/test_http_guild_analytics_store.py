'''Tests for HttpGuildAnalyticsStore — against a real DatabaseHttpServer.

Both halves go through aiohttp's TestServer + TestClient rather than a mocked
transport, because the risk in this seam is the wire, not the logic: a
serialisation mismatch between the two sides passes every unit test and fails in
prod. That is how the HttpBrokerClient guild_path bug shipped.

The store behind the server is the real GuildAnalyticsClient on a real postgres
for the round-trip tests, so what is being asserted is that the two
implementations of one Protocol are actually interchangeable -- not that a fake
agrees with itself. The failure paths use a stub, because raising
OperationalError on demand is the point of those.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from datetime import datetime, timezone
from functools import partial

import pytest
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.exc import OperationalError

from discord_bot.clients.guild_analytics_client import GuildAnalyticsClient
from discord_bot.clients.http_guild_analytics_store import HttpGuildAnalyticsStore
from discord_bot.exceptions import DatabaseUnavailable
from discord_bot.interfaces.database_protocols import GuildAnalyticsStore
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.types.guild_analytics import GuildAnalyticsEntry

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 4242


class _RecordingStore:
    '''GuildAnalyticsStore stand-in that records calls and can fail on demand.'''

    def __init__(self, error=None):
        self.error = error
        self.get_calls = []
        self.play_calls = []

    async def get_analytics(self, guild_id):
        self.get_calls.append(guild_id)
        if self.error:
            raise self.error
        return GuildAnalyticsEntry(total_plays=1, cached_plays=0,
                                   total_duration_days=0, total_duration_seconds=5,
                                   created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))

    async def record_play(self, guild_id, duration_seconds, cache_hit):
        self.play_calls.append((guild_id, duration_seconds, cache_hit))
        if self.error:
            raise self.error
        return True


def _live_store(fake_engine) -> GuildAnalyticsClient:  #pylint:disable=redefined-outer-name
    '''Build the real in-process store over the test engine.'''
    return GuildAnalyticsClient(partial(async_mock_session, fake_engine))


def test_http_store_satisfies_the_store_protocol():
    '''HttpGuildAnalyticsStore is a structural GuildAnalyticsStore.

    The whole point of MR 1's Protocols: the cog annotates against the Protocol,
    so this class and GuildAnalyticsClient are substitutable without the cog
    knowing which it has.
    '''
    assert isinstance(HttpGuildAnalyticsStore('http://db:8085'), GuildAnalyticsStore)


@pytest.mark.asyncio
async def test_totals_survive_the_round_trip(fake_engine):  #pylint:disable=redefined-outer-name
    '''Every field written through the wire comes back with its value and type.

    `created_at` is the one that matters: it is a timezone-aware datetime on one
    side, a JSON string on the wire, and `!music-stats` calls `.strftime` on
    whatever it gets back.
    '''
    server = DatabaseHttpServer(_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        await client.record_play(GUILD_ID, 7200, False)
        await client.record_play(GUILD_ID, 3600, True)
        entry = await client.get_analytics(GUILD_ID)

    assert isinstance(entry, GuildAnalyticsEntry)
    assert entry.total_plays == 2
    assert entry.cached_plays == 1
    assert entry.total_duration_seconds == 10800
    assert entry.created_at.tzinfo is not None
    assert entry.created_at.strftime('%Y-%m-%d')


@pytest.mark.asyncio
async def test_both_stores_answer_a_new_guild_alike(fake_engine):  #pylint:disable=redefined-outer-name
    '''A guild with no plays reads as zeroes through the wire, same as in-process.

    Interchangeability asserted against the other implementation rather than
    against a literal, so a change to one side that the other does not make
    fails here.
    '''
    in_process = _live_store(fake_engine)
    server = DatabaseHttpServer(_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        over_http = await client.get_analytics(GUILD_ID)
        direct = await in_process.get_analytics(GUILD_ID)

    assert over_http.model_dump(exclude={'created_at'}) == direct.model_dump(exclude={'created_at'})
    assert over_http.total_plays == 0


@pytest.mark.asyncio
async def test_the_carry_happens_on_the_pod_side(fake_engine):  #pylint:disable=redefined-outer-name
    '''Seconds roll into days across the wire, because the store still does it.

    Guards against the arithmetic drifting to the client, where two callers
    could disagree about it.
    '''
    server = DatabaseHttpServer(_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        await client.record_play(GUILD_ID, (60 * 60 * 24) + 30, False)
        entry = await client.get_analytics(GUILD_ID)

    assert entry.total_duration_days == 1
    assert entry.total_duration_seconds == 30


@pytest.mark.asyncio
async def test_record_play_arguments_arrive_as_sent():
    '''guild_id, duration and the cache flag reach the store as themselves.

    `cache_hit` is the one worth pinning: JSON has a real boolean, but a body
    built with a string would arrive truthy and silently miscount every play as
    cached.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(store).build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        assert await client.record_play(77, 120, True) is True
        assert await client.record_play(77, 120, False) is True

    assert store.play_calls == [(77, 120, True), (77, 120, False)]


@pytest.mark.asyncio
async def test_a_failed_store_call_raises_unavailable():
    '''A store failure crosses as a typed error and is re-raised on the bot side.

    The caller sees DatabaseUnavailable rather than an aiohttp error, and rather
    than SQLAlchemy's OperationalError -- which it could not catch without
    importing SQLAlchemy, the dependency this seam exists to remove.
    '''
    store = _RecordingStore(error=OperationalError('SELECT 1', {}, Exception('gone')))
    async with TestClient(TestServer(DatabaseHttpServer(store).build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DatabaseUnavailable) as caught:
            await client.get_analytics(GUILD_ID)

    assert 'OperationalError' in str(caught.value)


@pytest.mark.asyncio
async def test_a_store_failure_is_not_retried():
    '''The client does not ladder on top of the retries the pod already ran.

    This is the open question the spec asked to settle before MR 2: two retry
    layers that both fire turn one query into nine attempts and ~30 seconds. The
    pod owns retrying the database because it is nearest to it; by the time the
    answer is on the wire that work is done, so it comes back 200 and the
    client's async_retry_broker_command never sees a status to retry.

    One request, not four -- the count is the assertion.
    '''
    store = _RecordingStore(error=OperationalError('SELECT 1', {}, Exception('gone')))
    async with TestClient(TestServer(DatabaseHttpServer(store).build_app())) as tc:
        client = HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DatabaseUnavailable):
            await client.get_analytics(GUILD_ID)

    assert len(store.get_calls) == 1, (
        'the store was called more than once; a failure is being retried across '
        'the wire on top of the retries the pod already ran'
    )


@pytest.mark.asyncio
async def test_a_missing_field_is_rejected_not_guessed():
    '''An incomplete body gets 422, which the retry wrapper propagates at once.

    A caller that omitted a field never reached the database, so it is not an
    envelope error -- and it is the one failure a retry provably cannot fix.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(store).build_app())) as tc:
        response = await tc.post('/database/guild_analytics/record_play',
                                 json={'guild_id': 5})
        assert response.status == 422
        response = await tc.post('/database/guild_analytics/get_analytics', json={})
        assert response.status == 422

    assert not store.get_calls
    assert not store.play_calls


@pytest.mark.asyncio
async def test_the_server_refuses_while_draining():
    '''Draining returns 503, which the client's ladder does retry -- correctly.

    The one failure class the bot side is nearest to: this pod is going away and
    another attempt may reach a healthy one. Distinguishing it from a database
    fault is the whole reason the latter comes back 200.
    '''
    server = DatabaseHttpServer(_RecordingStore())
    server.start_draining()
    async with TestClient(TestServer(server.build_app())) as tc:
        response = await tc.post('/database/guild_analytics/get_analytics',
                                 json={'guild_id': GUILD_ID})
    assert response.status == 503
