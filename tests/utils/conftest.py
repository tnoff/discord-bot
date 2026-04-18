import pytest

from discord_bot.utils.dispatch_queue import RedisDispatchQueue


@pytest.fixture
def dispatch_queue(request):
    '''RedisDispatchQueue wired to the shared fakeredis client.'''
    redis = request.getfixturevalue('redis_client')
    return RedisDispatchQueue(redis, shard_id=0, pod_id='test-pod')
