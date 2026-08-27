from datetime import datetime, timedelta, timezone
from functools import partial

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.clients.markov_client import MarkovClient
from discord_bot.database import MarkovChannel, MarkovRelation
from discord_bot.interfaces.database_protocols import MarkovStore
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 111
CHANNEL_ID = 222
OTHER_CHANNEL_ID = 333


def build_store(fake_engine) -> MarkovClient:  #pylint:disable=redefined-outer-name
    '''
    Build a MarkovClient over the test engine.

    fake_engine : Async engine fixture, schema created and truncated
    '''
    return MarkovClient(partial(async_mock_session, fake_engine))


@pytest.mark.asyncio
async def test_markov_client_satisfies_the_store_protocol(fake_engine):  #pylint:disable=redefined-outer-name
    '''MarkovClient is a structural MarkovStore.

    The Protocol is what the cog annotates against, so a method renamed here
    without the Protocol following is a bug the type annotation alone will not
    surface at runtime.
    '''
    assert isinstance(build_store(fake_engine), MarkovStore)


@pytest.mark.asyncio
async def test_add_and_get_channel_round_trip(fake_engine):  #pylint:disable=redefined-outer-name
    '''add_channel returns a detached entry that get_channel can find again'''
    store = build_store(fake_engine)
    added = await store.add_channel(GUILD_ID, CHANNEL_ID)

    assert isinstance(added, MarkovChannelEntry)
    assert added.channel_id == CHANNEL_ID
    assert added.server_id == GUILD_ID
    assert added.last_message_id is None

    fetched = await store.get_channel(GUILD_ID, CHANNEL_ID)
    assert fetched == added
    # A store answering over HTTP has to be able to send this.
    assert MarkovChannelEntry.model_validate(fetched.model_dump(mode='json')) == fetched


@pytest.mark.asyncio
async def test_get_channel_returns_none_when_not_tracked(fake_engine):  #pylint:disable=redefined-outer-name
    '''An untracked channel is None, not an error'''
    store = build_store(fake_engine)
    assert await store.get_channel(GUILD_ID, 999999) is None


@pytest.mark.asyncio
async def test_list_channels_returns_entries_after_the_session_closes(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_channels entries are readable once the loading session is gone.

    The producer loop reads channel_id, server_id and last_message_id while
    awaiting Discord dispatches, long after this call returned. Live rows would
    make that a DetachedInstanceError waiting on lazy-load timing, and would
    hold a connection open for the whole fan-out to avoid it.
    '''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.add_channel(GUILD_ID, OTHER_CHANNEL_ID)

    channels = await store.list_channels()

    assert len(channels) == 2
    assert all(isinstance(entry, MarkovChannelEntry) for entry in channels)
    assert sorted(entry.channel_id for entry in channels) == [CHANNEL_ID, OTHER_CHANNEL_ID]
    assert all(entry.server_id == GUILD_ID for entry in channels)


@pytest.mark.asyncio
async def test_list_guild_channel_ids_returns_ints_not_row_tuples(fake_engine):  #pylint:disable=redefined-outer-name
    '''Channel ids come back as ints.

    The caller used to index `row[0]` off a one-column Row, which is a driver
    artifact rather than an answer and has no place on the wire.
    '''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.add_channel(GUILD_ID + 1, OTHER_CHANNEL_ID)

    ids = await store.list_guild_channel_ids(GUILD_ID)

    assert ids == [CHANNEL_ID]
    assert all(isinstance(channel_id, int) for channel_id in ids)


@pytest.mark.asyncio
async def test_save_messages_writes_relations_and_last_message_id(fake_engine):  #pylint:disable=redefined-outer-name
    '''A batch persists every message's pairs and leaves last_message_id at the final one'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    timestamp = datetime(2024, 11, 30, tzinfo=timezone.utc)

    written = await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('this', 'is'), ('is', 'this')],
                           message_timestamp=timestamp, last_message_id=1),
        MarkovMessageWrite(word_pairs=[('a', 'test'), ('test', 'a')],
                           message_timestamp=timestamp, last_message_id=2),
    ])

    assert written == 2
    async with async_mock_session(fake_engine) as session:
        relations = (await session.execute(select(MarkovRelation))).scalars().all()
        channel = (await session.execute(select(MarkovChannel))).scalars().first()
    assert sorted((r.leader_word, r.follower_word) for r in relations) == [
        ('a', 'test'), ('is', 'this'), ('test', 'a'), ('this', 'is'),
    ]
    assert channel.last_message_id == 2


@pytest.mark.asyncio
async def test_save_messages_advances_last_message_id_for_a_wordless_message(fake_engine):  #pylint:disable=redefined-outer-name
    '''A message with no pairs still moves the cursor.

    Bot posts, commands and image-only messages contribute nothing to the chain.
    Skipping their write would leave last_message_id behind them and re-fetch
    the same messages every cycle, forever.
    '''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)

    written = await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=77),
    ])

    assert written == 1
    async with async_mock_session(fake_engine) as session:
        count = (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar()
        channel = (await session.execute(select(MarkovChannel))).scalars().first()
    assert count == 0
    assert channel.last_message_id == 77


@pytest.mark.asyncio
async def test_save_messages_returns_none_for_an_untracked_channel(fake_engine):  #pylint:disable=redefined-outer-name
    '''No such channel is None, distinct from a batch that wrote nothing'''
    store = build_store(fake_engine)

    written = await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])

    assert written is None
    async with async_mock_session(fake_engine) as session:
        count = (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar()
    assert count == 0


@pytest.mark.asyncio
async def test_save_messages_opens_one_session_for_the_whole_batch(fake_engine):  #pylint:disable=redefined-outer-name
    '''Ten messages cost one session, not ten.

    NullPool makes every session a fresh connection, so a per-message signature
    would put ten of them where one belongs -- and one HTTP round trip per
    message once this store is remote. That is the shape `!267` removed one
    layer down; the batch is what keeps it from coming back at this layer.
    '''
    sessions_opened = []
    inner = partial(async_mock_session, fake_engine)

    def counting_session():
        sessions_opened.append(1)
        return inner()

    store = MarkovClient(counting_session)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    sessions_opened.clear()

    timestamp = datetime(2024, 11, 30, tzinfo=timezone.utc)
    written = await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('word', str(index))],
                           message_timestamp=timestamp, last_message_id=index)
        for index in range(10)
    ])

    assert written == 10
    assert len(sessions_opened) == 1, (
        f'expected one session for the batch, got {len(sessions_opened)}'
    )


@pytest.mark.asyncio
async def test_remove_channel_drops_relations_and_the_row(fake_engine):  #pylint:disable=redefined-outer-name
    '''!markov off takes the channel and its words together'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])

    assert await store.remove_channel(GUILD_ID, CHANNEL_ID) is True

    async with async_mock_session(fake_engine) as session:
        channels = (await session.execute(select(sql_count()).select_from(MarkovChannel))).scalar()
        relations = (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar()
    assert channels == 0
    assert relations == 0


@pytest.mark.asyncio
async def test_remove_channel_returns_false_when_not_tracked(fake_engine):  #pylint:disable=redefined-outer-name
    '''False is the answer, not a failure to retry'''
    store = build_store(fake_engine)
    assert await store.remove_channel(GUILD_ID, CHANNEL_ID) is False


@pytest.mark.asyncio
async def test_reset_channel_clears_relations_and_the_cursor_together(fake_engine):  #pylint:disable=redefined-outer-name
    '''Recovery from a vanished last_message_id clears both halves.

    Dropping the cursor while keeping the relations would double every word the
    channel re-gathers from the retention cutoff.
    '''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=42),
    ])

    assert await store.reset_channel(GUILD_ID, CHANNEL_ID) is True

    async with async_mock_session(fake_engine) as session:
        relations = (await session.execute(select(sql_count()).select_from(MarkovRelation))).scalar()
        channel = (await session.execute(select(MarkovChannel))).scalars().first()
    assert relations == 0
    assert channel is not None
    assert channel.last_message_id is None


@pytest.mark.asyncio
async def test_reset_channel_returns_false_when_not_tracked(fake_engine):  #pylint:disable=redefined-outer-name
    '''Nothing to reset is an answer'''
    store = build_store(fake_engine)
    assert await store.reset_channel(GUILD_ID, CHANNEL_ID) is False


@pytest.mark.asyncio
async def test_generate_words_walks_the_chain_in_one_session(fake_engine):  #pylint:disable=redefined-outer-name
    '''A whole sentence costs one session, whatever its length.

    Each word is chosen from the previous one, so a per-word store method would
    be a round trip per word once remote -- the same shape `!268` removed from
    the SQL. The chain here is deterministic (a->b->c->a) so the walk is
    assertable despite postgres picking each step.
    '''
    sessions_opened = []
    inner = partial(async_mock_session, fake_engine)

    def counting_session():
        sessions_opened.append(1)
        return inner()

    store = MarkovClient(counting_session)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b'), ('b', 'c'), ('c', 'a')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])
    sessions_opened.clear()

    words = await store.generate_words(GUILD_ID, 4, first_word='a')

    assert words == ['a', 'b', 'c', 'a']
    assert len(sessions_opened) == 1, (
        f'expected one session for the sentence, got {len(sessions_opened)}'
    )


@pytest.mark.asyncio
async def test_generate_words_returns_empty_when_the_guild_has_nothing(fake_engine):  #pylint:disable=redefined-outer-name
    '''An empty list is the "nothing to say" answer'''
    store = build_store(fake_engine)
    assert await store.generate_words(GUILD_ID, 8) == []


@pytest.mark.asyncio
async def test_generate_words_returns_empty_when_no_relation_leads_with_first_word(fake_engine):  #pylint:disable=redefined-outer-name
    '''No match for the requested opener is also an answer, not an error'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])

    assert await store.generate_words(GUILD_ID, 8, first_word='nope') == []


@pytest.mark.asyncio
async def test_generate_words_stops_at_a_dead_end(fake_engine):  #pylint:disable=redefined-outer-name
    '''A word that leads nowhere ends the sentence short.

    Retention makes this reachable: it can delete every relation in which a word
    leads while keeping one where it follows.
    '''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('start', 'end')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])

    assert await store.generate_words(GUILD_ID, 10, first_word='start') == ['start', 'end']


@pytest.mark.asyncio
async def test_generate_words_returns_empty_for_a_non_positive_count(fake_engine):  #pylint:disable=redefined-outer-name
    '''Asking for no words does not query'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('a', 'b')],
                           message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
                           last_message_id=1),
    ])

    assert await store.generate_words(GUILD_ID, 0) == []


@pytest.mark.asyncio
async def test_generate_words_only_reads_its_own_guild(fake_engine):  #pylint:disable=redefined-outer-name
    '''One guild's chain never borrows another's words'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    await store.add_channel(GUILD_ID + 1, OTHER_CHANNEL_ID)
    timestamp = datetime(2024, 11, 30, tzinfo=timezone.utc)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('mine', 'mine')],
                           message_timestamp=timestamp, last_message_id=1),
    ])
    await store.save_messages(GUILD_ID + 1, OTHER_CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('theirs', 'theirs')],
                           message_timestamp=timestamp, last_message_id=1),
    ])

    assert set(await store.generate_words(GUILD_ID, 5)) == {'mine'}


@pytest.mark.asyncio
async def test_prune_relations_before_drops_only_the_old(fake_engine):  #pylint:disable=redefined-outer-name
    '''Retention deletes past the cutoff and leaves the rest'''
    store = build_store(fake_engine)
    await store.add_channel(GUILD_ID, CHANNEL_ID)
    now = datetime(2024, 11, 30, tzinfo=timezone.utc)
    await store.save_messages(GUILD_ID, CHANNEL_ID, [
        MarkovMessageWrite(word_pairs=[('old', 'old')],
                           message_timestamp=now - timedelta(days=400), last_message_id=1),
        MarkovMessageWrite(word_pairs=[('new', 'new')],
                           message_timestamp=now, last_message_id=2),
    ])

    assert await store.prune_relations_before(now - timedelta(days=365)) is True

    async with async_mock_session(fake_engine) as session:
        relations = (await session.execute(select(MarkovRelation))).scalars().all()
    assert [r.leader_word for r in relations] == ['new']
