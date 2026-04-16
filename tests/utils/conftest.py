import fakeredis.aioredis
import pytest

from discord_bot.utils.dispatch_queue import RedisDispatchQueue


@pytest.fixture
def redis_client():
    '''Return a FakeRedis instance with decode_responses=True.'''
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def dispatch_queue(request):
    '''RedisDispatchQueue wired to the shared fakeredis client.'''
    redis = request.getfixturevalue('redis_client')
    return RedisDispatchQueue(redis, shard_id=0, pod_id='test-pod')
