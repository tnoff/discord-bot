'''
Database fixtures and helpers for tests that need a real (SQLite) engine.

Split out of testing/helpers.py so that the pure helpers there -- and the root conftest.py,
which every package's test run imports -- do not pull in `discord_db`. See
ci/_affected_tests.py for why that coupling mattered.
'''
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from typing import Any, AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, AsyncEngine

from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db import setup_db
from discord_db.clients.markov_client import MarkovClient
from discord_db.clients.playlist_client import PlaylistClient
from discord_db.database import BASE, MarkovChannel, MarkovRelation, Playlist, PlaylistItem, VideoCache


@pytest_asyncio.fixture(scope="function")
async def fake_engine(fake_db_dsn) -> AsyncGenerator[AsyncEngine, None]:
    '''Async SQLite engine with the bot schema, fresh for each test.

    Built by `setup_db`, so the engine under test is the one the db pod builds,
    pragmas and all.
    '''
    engine = setup_db(GeneralConfig(sql_connection_statement=fake_db_dsn))
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()


@dataclass(frozen=True)
class FakeStores:
    '''The stores a test hands to the commands logic; only the playlist store is used.'''
    playlist: Any


@pytest_asyncio.fixture(scope="function")
async def fake_stores(fake_engine) -> AsyncGenerator[FakeStores, None]:  #pylint:disable=redefined-outer-name
    '''
    The commands pod's playlist store over the local test engine.

    Production points an HTTP store at the db pod. Tests use the in-process client over
    `fake_engine`, so the commands logic exercises real queries rather than a mock of what the db
    pod might answer. That substitution is the point of the Protocols: the logic cannot tell the
    two apart, and a test that asserts on rows afterwards asserts on the engine it wrote through.
    The gateway holds no store any more, so its tests do not take this fixture.

    Request `fake_engine` alongside this fixture to read those rows back.
    '''
    session_generator = partial(async_mock_session, fake_engine)
    yield FakeStores(playlist=PlaylistClient(session_generator))


@pytest_asyncio.fixture(scope="function")
async def fake_markov_store(fake_engine) -> AsyncGenerator[MarkovClient, None]:  #pylint:disable=redefined-outer-name
    '''
    The markov store over the local test engine, for the commands pod's markov logic.

    The gateway's DatabaseStores bundle no longer carries it: the markov commands and loops run in the
    commands pod, which holds the store itself.
    '''
    yield MarkovClient(partial(async_mock_session, fake_engine))


@asynccontextmanager
async def async_mock_session(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


# The row models are re-exported so a pod's tests can read rows back through the shared test
# infrastructure instead of importing `discord_db` themselves (tests/checks/test_package_test_imports.py).
__all__ = ['FakeStores', 'MarkovChannel', 'MarkovRelation', 'Playlist', 'PlaylistItem', 'VideoCache',
           'async_mock_session', 'fake_engine', 'fake_markov_store', 'fake_stores']
