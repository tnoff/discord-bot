'''
Shared pytest fixtures.

The database tests run on SQLite: `fake_db_dsn` hands each test a fresh file, and
`fake_engine` (tests/helpers.py) builds the engine over it through the same
`setup_db` the db pod uses. Nothing here starts or needs a database server.
'''
import fakeredis.aioredis
import pytest

from discord_core.clients.http_client_base import SEAM_CLIENTS
from discord_core.utils.loop_health import LOOP_HEALTH

from tests.helpers import fake_context #pylint:disable=unused-import,wrong-import-position


@pytest.fixture(scope="function")
def fake_db_dsn(tmp_path) -> str:
    '''The configured-form DSN of a fresh SQLite test database.'''
    return f'sqlite:///{tmp_path / "test.db"}'


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


@pytest.fixture(autouse=True)
def reset_loop_health():
    '''Clear the process-global loop-health registry between tests.

    LOOP_HEALTH is per-process state, which is exactly right in production (one
    registry per pod) but leaks across tests in a single pytest process: a loop
    registered by one test would otherwise still be there for the next one,
    adding a `loops` key to health payloads and failing probes for loops that
    test never started.
    '''
    LOOP_HEALTH.reset()
    yield
    LOOP_HEALTH.reset()


@pytest.fixture(autouse=True)
def reset_seam_clients():
    '''Clear the process-global seam-client registry between tests.

    Same shape as LOOP_HEALTH above, and newly load-bearing: clients now enrol
    themselves the moment they are given a seam contract config, so without this
    a client built by one test is still enrolled for the next, and a later
    start_seam_checks() walks every client the suite has ever constructed.
    '''
    SEAM_CLIENTS.reset()
    yield
    SEAM_CLIENTS.reset()
