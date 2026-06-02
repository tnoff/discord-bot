import fakeredis.aioredis
import pytest


# protocol=2 forces RESP2 on every FakeRedis in the test suite (here and in test
# files that construct one directly). fakeredis 2.36.0 + redis-py 8.0.0 returns
# RESP3 wire shape from stream commands but never decodes the bytes — the new
# parse_xread_resp3_to_resp2_legacy -> pairs_to_dict path runs with
# decode_keys=False and ignores decode_responses=True, so XREADGROUP/XINFO GROUPS
# leak b'...' through. Drop protocol=2 (and grep the suite) once fakeredis ships
# a fix; upstream tracking issue is cunla/fakeredis-py#488, and the
# XREADGROUP-specific symptom isn't filed yet — open a focused repro when
# removing this workaround.
@pytest.fixture
def redis_client():
    '''Return a FakeRedis instance with decode_responses=True.'''
    return fakeredis.aioredis.FakeRedis(decode_responses=True, protocol=2)
