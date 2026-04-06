from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from discord_bot.cogs.common import CogHelperBase
from discord_bot.utils.sql_retry import async_retry_database_commands


class CogHelper(CogHelperBase):
    '''
    Cog base class with database support. Extends CogHelperBase with async SQLAlchemy
    session management.
    '''

    @asynccontextmanager
    async def with_db_session(self):
        '''
        Yield an async db session from engine
        '''
        session_factory = async_sessionmaker(bind=self.db_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as db_session:
            yield db_session

    async def retry_commit(self, db_session: AsyncSession):
        '''
        Common function to retry db_session commit
        db_session: Sqlalchemy async db session
        '''
        await async_retry_database_commands(db_session, db_session.commit)
