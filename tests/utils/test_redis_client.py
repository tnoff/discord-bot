import pytest
import fakeredis.aioredis

from discord_bot.utils.redis_client import (
    BUNDLE_KEY_PREFIX,
    save_bundle,
    delete_bundle,
    load_all_bundles,
)


@pytest.mark.asyncio
async def test_save_and_load_bundle():
    '''save_bundle then load_all_bundles returns the original dict.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle_dict = {'guild_id': 1, 'channel_id': 2, 'sticky_messages': True, 'message_contexts': []}
    await save_bundle(client, 'key1', bundle_dict)
    result = await load_all_bundles(client)
    assert 'key1' in result
    assert result['key1'] == bundle_dict


@pytest.mark.asyncio
async def test_delete_bundle():
    '''delete_bundle removes the key so load_all_bundles returns empty.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle_dict = {'guild_id': 1, 'channel_id': 2, 'sticky_messages': True, 'message_contexts': []}
    await save_bundle(client, 'key1', bundle_dict)
    await delete_bundle(client, 'key1')
    result = await load_all_bundles(client)
    assert result == {}


@pytest.mark.asyncio
async def test_load_all_bundles_empty():
    '''load_all_bundles returns {} when Redis has no bundle keys.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    result = await load_all_bundles(client)
    assert result == {}


@pytest.mark.asyncio
async def test_load_all_bundles_multiple():
    '''load_all_bundles returns all saved bundles.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle_a = {'guild_id': 1, 'channel_id': 2, 'sticky_messages': True, 'message_contexts': []}
    bundle_b = {'guild_id': 3, 'channel_id': 4, 'sticky_messages': False, 'message_contexts': []}
    await save_bundle(client, 'key_a', bundle_a)
    await save_bundle(client, 'key_b', bundle_b)
    result = await load_all_bundles(client)
    assert 'key_a' in result
    assert 'key_b' in result
    assert result['key_a'] == bundle_a
    assert result['key_b'] == bundle_b


@pytest.mark.asyncio
async def test_load_all_bundles_ignores_unrelated_keys():
    '''Keys without the bundle prefix are not returned.'''
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await client.set('some_other_key', 'value')
    bundle_dict = {'guild_id': 1, 'channel_id': 2, 'sticky_messages': True, 'message_contexts': []}
    await save_bundle(client, 'key1', bundle_dict)
    result = await load_all_bundles(client)
    assert list(result.keys()) == ['key1']
    assert BUNDLE_KEY_PREFIX + 'key1' not in result
