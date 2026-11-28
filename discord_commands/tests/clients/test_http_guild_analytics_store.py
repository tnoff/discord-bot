'''HttpGuildAnalyticsStore, tested against the database contract and nothing else.

The client is served by a `ContractStub` built from `testing/contracts/database.py`, the same description the db pod's
server is proved against. This checks the client's half of the seam: the body it sends is one the contract accepts (the
stub records a violation, and the test fails, if not) and every declared reply becomes what the cog expects. It does not
need the db pod.

That the pod does the right thing with a body it accepts is `tests/routes/` and `discord_db/tests/`, and the round trip
over a real store is tests/integration/commands_to_db/.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from datetime import datetime, timezone

import pytest

from discord_core.exceptions import DatabaseUnavailable
from discord_core.routes import database as database_routes
from discord_core.types.guild_analytics import GuildAnalyticsEntry

from discord_commands.clients.http_guild_analytics_store import HttpGuildAnalyticsStore

from testing.contracts.stub import serving
from testing.db_client_support import answering, failing

GUILD = 4242
ROUTE = database_routes.GUILD_ANALYTICS.routes['get_analytics']
TOTALS = GuildAnalyticsEntry(total_plays=3, cached_plays=1, total_duration_days=0, total_duration_seconds=500,
                             created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))


@pytest.mark.asyncio
async def test_a_guild_asks_for_its_own_totals():
    '''The guild goes in the body and the totals come back as the entry the pod described.'''
    stub = answering(ROUTE, TOTALS)
    async with serving(stub) as tc:
        entry = await HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session).get_analytics(GUILD)
    assert entry == TOTALS
    assert stub.calls_to(ROUTE) == [{'guild_id': GUILD}]


@pytest.mark.asyncio
async def test_a_store_failure_is_unavailable_and_asked_once():
    '''The pod's error body is re-raised as DatabaseUnavailable, and 200 means the client does not retry it.'''
    stub = failing(ROUTE)
    async with serving(stub) as tc:
        with pytest.raises(DatabaseUnavailable, match='OperationalError'):
            await HttpGuildAnalyticsStore(str(tc.make_url('')), session=tc.session).get_analytics(GUILD)
    assert len(stub.calls_to(ROUTE)) == 1
