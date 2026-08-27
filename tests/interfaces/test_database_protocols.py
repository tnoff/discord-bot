'''Tests for the persistence-tier Protocols themselves.

The Protocol modules carry no behaviour -- their method bodies are docstrings --
so what is worth asserting about them is a property, not a return value: they
must stay implementable by something that is not in this process. The one
mechanical proxy for that is what they drag into an import chain.
'''
from datetime import datetime, timezone

import pytest

from discord_bot.interfaces.database_protocols import MarkovStore, VideoCacheStore
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite

from tests.cli._image_deps import measure


def test_database_protocols_imports_no_third_party_package():
    '''The protocol module pulls nothing from the image vocabulary.

    Not a style check. A signature that names a SQLAlchemy model, or a client
    that imports one, shows up here immediately -- and that signature is exactly
    the one only the in-process implementation could ever satisfy, which is the
    failure this whole seam exists to prevent. Today the chain is empty: the
    DTOs in types/ are pydantic and nothing else.

    Asserted against the whole vocabulary rather than sqlalchemy alone because
    there is no package this module has any business importing. The eventual
    HttpMarkovStore will import aiohttp -- in its own module, not this one.
    '''
    packages = set(measure('discord_bot.interfaces.database_protocols')['packages'])
    assert not packages, (
        f'database_protocols pulled {sorted(packages)} into its import chain. A '
        'method signature here probably names an ORM model or a concrete client; '
        'name a type from discord_bot.types instead.'
    )


def test_stores_are_runtime_checkable():
    '''Both Protocols support isinstance, which is how implementations are asserted.

    `@runtime_checkable` is easy to drop in a refactor and its absence surfaces
    as a TypeError inside another test rather than as a failure here.
    '''
    for protocol in (VideoCacheStore, MarkovStore):
        assert isinstance(object(), protocol) is False


@pytest.mark.asyncio
async def test_markov_store_is_satisfiable_without_a_database():
    '''A dict-backed store satisfies MarkovStore, which is the point of it.

    This is the property the whole seam rests on and the one an annotation
    cannot express: nothing in these signatures is session-bound, so an
    implementation that has never heard of SQLAlchemy can be substituted whole.
    If a future slice writes `database.MarkovChannel` into a signature this test
    keeps passing -- but the class below stops being writable without importing
    the ORM, which is the moment to notice.

    Deliberately not shipped as production code. An InMemoryMarkovStore in
    discord_bot/ would be a second implementation nothing runs, drifting quietly
    from the one that does.
    '''
    class DictMarkovStore:  #pylint:disable=missing-class-docstring
        def __init__(self):
            self.channels = {}
            self.relations = []

        async def list_channels(self):
            return list(self.channels.values())

        async def list_guild_channel_ids(self, guild_id):
            return [entry.channel_id for entry in self.channels.values()
                    if entry.server_id == guild_id]

        async def get_channel(self, guild_id, channel_id):
            return self.channels.get((guild_id, channel_id))

        async def add_channel(self, guild_id, channel_id):
            entry = MarkovChannelEntry(id=len(self.channels) + 1, channel_id=channel_id,
                                       server_id=guild_id, last_message_id=None)
            self.channels[(guild_id, channel_id)] = entry
            return entry

        async def remove_channel(self, guild_id, channel_id):
            return self.channels.pop((guild_id, channel_id), None) is not None

        async def reset_channel(self, guild_id, channel_id):
            entry = self.channels.get((guild_id, channel_id))
            if not entry:
                return False
            self.channels[(guild_id, channel_id)] = entry.model_copy(update={'last_message_id': None})
            return True

        async def save_messages(self, guild_id, channel_id, messages):
            entry = self.channels.get((guild_id, channel_id))
            if not entry:
                return None
            for message in messages:
                for (leader, follower) in message.word_pairs:
                    self.relations.append((guild_id, leader, follower, message.message_timestamp))
                entry = entry.model_copy(update={'last_message_id': message.last_message_id})
            self.channels[(guild_id, channel_id)] = entry
            return len(messages)

        async def generate_words(self, guild_id, count, first_word=None):
            leaders = [leader for (relation_guild, leader, _follower, _created) in self.relations
                       if relation_guild == guild_id
                       and (first_word is None or leader == first_word)]
            return leaders[:count]

        async def prune_relations_before(self, cutoff):
            self.relations = [relation for relation in self.relations if relation[3] >= cutoff]
            return True

    store = DictMarkovStore()
    assert isinstance(store, MarkovStore)

    # Driven, not just isinstance'd: a structural check compares names and would
    # pass against a class whose every method returned None.
    added = await store.add_channel(7, 8)
    assert added.channel_id == 8
    assert await store.list_guild_channel_ids(7) == [8]
    assert await store.save_messages(7, 8, [MarkovMessageWrite(
        word_pairs=[('hello', 'world')],
        message_timestamp=datetime(2024, 11, 30, tzinfo=timezone.utc),
        last_message_id=3)]) == 1
    assert await store.generate_words(7, 5) == ['hello']
    assert await store.generate_words(99, 5) == []
    assert await store.save_messages(7, 9999, []) is None
    assert await store.prune_relations_before(datetime(2025, 1, 1, tzinfo=timezone.utc)) is True
    assert await store.generate_words(7, 5) == []
    assert await store.remove_channel(7, 8) is True
    assert await store.remove_channel(7, 8) is False
