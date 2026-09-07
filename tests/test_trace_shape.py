'''Trace-shape regression check — does a route's trace arrive in one piece?

Every other span assertion in this suite checks that a *particular* span exists,
is named right, or carries the right status. None of them would notice a trace
arriving as two disconnected halves, because every span in a severed trace is
individually well-formed. That is not hypothetical: on 2026-09-04 a collector
filter written against the bot's noise profile dropped
`sql_retry.retry_db_command` fleet-wide, which on the new `discord-db` tier was
an *intermediate* span rather than a leaf, and severed every trace the tier
produced. It survived for hours because nothing anywhere asserted that a
service's spans are connected. See
projects/tracing-suppression-config-surface.md in the docs repo.

This module asserts the app-side half of that invariant: the code emits one
connected tree per route. It cannot assert the collector-side half -- what the
pipeline does to those spans afterwards is not observable from here -- so the
prod-side alert on SERVER spans with no descendants remains the other half of
the check.

The route under test is a real client -> real aiohttp server -> real store on
real postgres, because the seam is where connectivity is actually at risk: the
parent link crosses a process boundary as a W3C `traceparent` header, and
nothing in-process would notice if it stopped being injected.
'''
from functools import partial

import pytest
from aiohttp.test_utils import TestClient, TestServer
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from discord_bot.clients.http_markov_store import HttpMarkovStore
from discord_bot.clients.markov_client import MarkovClient
from discord_bot.servers.database_server import DatabaseHttpServer

from tests.helpers import assert_one_connected_trace
from tests.helpers import fake_engine  # pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 606
CHANNEL_ID = 707

# The intermediate span the collector rule dropped on 2026-09-04. Named here so
# the severance test reproduces that incident rather than an invented one.
SEVERED_SPAN = 'markov.store.add_channel'


def _recording_tracer(mocker) -> InMemorySpanExporter:
    '''Swap in a recording tracer so emitted spans survive to an assertion.

    The suite runs with no global tracer provider, so spans are non-recording
    and never reach an exporter -- without this every assertion below would
    pass vacuously on an empty list.
    '''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    mocker.patch('discord_bot.utils.otel.TRACER', provider.get_tracer('test'))
    return exporter


async def _capture_add_channel(mocker, fake_engine):  # pylint:disable=redefined-outer-name
    '''Drive one markov write across the HTTP seam, returning its finished spans.'''
    exporter = _recording_tracer(mocker)
    server = DatabaseHttpServer(
        markov_store=MarkovClient(partial(async_mock_session, fake_engine)))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        await client.add_channel(GUILD_ID, CHANNEL_ID)
    return exporter.get_finished_spans()


@pytest.mark.asyncio
async def test_a_db_route_arrives_as_one_connected_tree(mocker, fake_engine):  # pylint:disable=redefined-outer-name
    '''One root, one trace id, and no span whose parent is missing.'''
    spans = await _capture_add_channel(mocker, fake_engine)

    root = assert_one_connected_trace(spans)
    assert root.name == 'markov_store.add_channel'


@pytest.mark.asyncio
async def test_the_tree_actually_crosses_the_process_seam(mocker, fake_engine):  # pylint:disable=redefined-outer-name
    '''The server span's parent is remote, i.e. it came from the traceparent header.

    Without this, the connectivity assertion above would still pass if header
    propagation stopped entirely and the whole tree happened to be built
    in-process -- which is precisely the arrangement a test harness makes easy
    and production never has.
    '''
    spans = await _capture_add_channel(mocker, fake_engine)

    remote_parents = [span.name for span in spans
                      if span.parent is not None and span.parent.is_remote]
    assert remote_parents == ['database.markov.add_channel']


@pytest.mark.asyncio
async def test_dropping_one_middle_span_is_caught(mocker, fake_engine):  # pylint:disable=redefined-outer-name
    '''Deleting an intermediate span severs the tree, and the check must fail.

    The other position of the same assertion, and the reason this module is not
    self-satisfying: a connectivity check that cannot fail proves nothing. This
    reproduces 2026-09-04 exactly -- take the real captured spans, remove the
    one the collector rule removed, and confirm the leaf below it is now an
    orphan rather than a still-plausible-looking span.
    '''
    spans = await _capture_add_channel(mocker, fake_engine)
    # Establish the intact tree first, so a propagation break upstream reports
    # as what it is rather than as this test's own assertion failing oddly.
    assert_one_connected_trace(spans)
    assert SEVERED_SPAN in [span.name for span in spans]

    severed = [span for span in spans if span.name != SEVERED_SPAN]
    with pytest.raises(AssertionError, match='no parent in the trace'):
        assert_one_connected_trace(severed)
