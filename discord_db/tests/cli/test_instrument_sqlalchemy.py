'''
instrument_sqlalchemy against a real engine and a real tracer.

The instrumentor gates itself on the installed SQLAlchemy version and, outside
its range, logs an error and instruments nothing. That is silent at the call
site, so the only way to know the db pod emits SQL spans is to run a query and
look.
'''
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from discord_db.cli._lib import db as db_lib


@pytest.mark.asyncio
async def test_instrument_sqlalchemy_emits_spans_for_queries(mocker):
    '''A query on the instrumented engine produces a span, whatever SQLAlchemy is installed.

    Fails if a SQLAlchemy bump falls outside the instrumentor's range again and
    the version gate is not bypassed (or the instrumentor stops working on it).
    '''
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    mocker.patch.object(db_lib.trace, 'get_tracer_provider', return_value=provider)

    # A process-global singleton: undo any earlier instrumentation, and undo ours.
    SQLAlchemyInstrumentor().uninstrument()
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    try:
        db_lib.instrument_sqlalchemy(engine)
        async with engine.connect() as conn:
            await conn.execute(text('SELECT 1'))
        assert exporter.get_finished_spans()
    finally:
        SQLAlchemyInstrumentor().uninstrument()
        await engine.dispose()
