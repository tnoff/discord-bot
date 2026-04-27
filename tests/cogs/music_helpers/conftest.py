from typing import AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from discord_bot.database import BASE


@pytest_asyncio.fixture(scope="function")
async def fake_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as conn:
        await conn.run_sync(BASE.metadata.create_all)
    yield engine
    await engine.dispose()
