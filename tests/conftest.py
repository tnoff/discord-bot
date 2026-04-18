import fakeredis.aioredis
import pytest


@pytest.fixture
def redis_client():
    '''Return a FakeRedis instance with decode_responses=True.'''
    return fakeredis.aioredis.FakeRedis(decode_responses=True)
