'''
Shared plumbing for the in-process persistence stores.

Three clients now implement Protocols from `interfaces/database_protocols.py`
over a session generator -- video cache, markov, playlists -- and the same four
lines kept appearing in each: open a session, run a statement under the retry
wrapper, map the rows to entries, close. Pylint's duplicate-code check caught
the third copy, which is the right moment to name the shape rather than to
silence the check.

The base owns the *plumbing*, not the queries. Every method here takes a
statement the caller built and a function that turns one row into a value, so
nothing about what a store asks for or what it returns leaks in here. That
matters for the seam: when a store moves behind HTTP it stops inheriting from
this entirely, and nothing it promised its callers changes.
'''
from typing import Callable, List

from discord_bot.utils.sql_retry import async_retry_database_commands


class SessionStoreBase():
    '''
    A store backed by a session generator over the local engine.

    session_generator : Callable returning an async context manager yielding an
                        AsyncSession
    '''

    def __init__(self, session_generator: Callable):
        self.session_generator: Callable = session_generator

    async def _select_all(self, statement, build: Callable) -> List:
        '''
        Run a select and build a value from every row.

        Rows are converted inside the session block, which is the whole point:
        the caller reads the results after this returns, and a live instance
        would be detached by then.

        statement : Sqlalchemy select to run
        build : Callable turning one row into the value to return
        '''
        async with self.session_generator() as db_session:
            rows = await async_retry_database_commands(
                db_session, lambda: db_session.execute(statement))
            return [build(row) for row in rows.scalars().all()]

    async def _delete_row(self, model, row_id: int) -> bool:
        '''
        Delete one row by primary key. False when it was already gone.

        model : Sqlalchemy model class
        row_id : Primary key value
        '''
        async with self.session_generator() as db_session:
            async def delete_record():
                row = await db_session.get(model, row_id)
                if not row:
                    return False
                await db_session.delete(row)
                await db_session.commit()
                return True

            return await async_retry_database_commands(db_session, delete_record)
