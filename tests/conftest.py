import fakeredis.aioredis
import pytest

from tests.helpers import fake_context #pylint:disable=unused-import


@pytest.fixture
def redis_client():
    '''Return a FakeRedis instance with decode_responses=True.'''
    return fakeredis.aioredis.FakeRedis(decode_responses=True)
