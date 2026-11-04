import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from discord_core.utils.dispatch_queue import RedisDispatchQueue


@pytest.fixture
def dispatch_queue(request):
    '''RedisDispatchQueue wired to the shared fakeredis client.'''
    redis = request.getfixturevalue('redis_client')
    return RedisDispatchQueue(redis, shard_id=0, pod_id='test-pod')


@pytest.fixture
def recording_tracer():
    '''
    A (tracer, exporter) pair backed by a real SDK provider.

    The suite runs with no global tracer provider, so spans are non-recording
    and drop their status entirely.  Span *status* assertions need a real
    provider, and this builds a throwaway one rather than touching global
    state (set_tracer_provider is one-shot per process).
    '''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer('test'), exporter
