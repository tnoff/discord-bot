'''A recording tracer, for tests that assert on the spans (and their status) the code emitted.

The suite runs with no global tracer provider, so spans are non-recording: they drop their status
and attributes and never reach an exporter, and an assertion over them would pass vacuously. The
provider built here is a throwaway, because set_tracer_provider is one-shot per process.
'''
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


def recording_tracer():
    '''A (tracer, exporter) pair over a real SDK provider.'''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer('test'), exporter


def record_spans(mocker) -> InMemorySpanExporter:
    '''Swap a recording tracer into discord_core.utils.otel; return the exporter its spans reach.'''
    tracer, exporter = recording_tracer()
    mocker.patch('discord_core.utils.otel.TRACER', tracer)
    return exporter
