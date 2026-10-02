'''
One-off copy of the discord database into a new SQLite file.

`discord-db <config> --copy-to-sqlite <path>` runs this instead of serving. It
exists for the cutover from PostgreSQL to SQLite (tnoff/docker-apps#637): the db
pod can now run on either, but nothing moved the data. It reads every model
table from the source (the config's `general.sql_connection_statement`, so the
password stays in the Secret the config already reads), writes it to a
brand-new SQLite file, and refuses to call the copy good until it has read the
file back and compared it to what it read from the source.

Run it with the db pod stopped. It takes no lock and no snapshot on the source,
on purpose: a snapshot needs a PostgreSQL-only isolation level, and the copy
would then be the one piece of this code that could not be tested without a
server. Instead it re-counts the source after copying and fails if any table
changed, which catches rows added or removed under it. It cannot catch a row
that was edited in place, so stop the writer rather than relying on that.

What it checks, in order, and what each failure means:

* **The destination must not exist.** It never overwrites. The copy is built at
  `<dest>.partial` and renamed into place only after every check passes, so a
  failed or interrupted run leaves no half-written database at the real path.
* **The source schema must match the models, column for column.** Rows are read
  and written through the models, so a column the models do not know would be
  dropped without a word. A table missing, a column missing, or a column extra
  aborts the copy, naming it. Tables the models do not know are refused too
  unless `--allow-extra-tables` says they are meant to be left behind.
* **The source must carry an `alembic_version` row.** It is copied verbatim, so
  the new file is stamped at the revision the data was written at and the pod's
  `alembic upgrade head` carries on from there. The alembic chain is not
  replayed on SQLite; see cli/_lib/migrations.py.
* **Every table is read back from the destination and compared** by row count
  and by a SHA-256 over the rows in primary-key order, taken from the same
  values on both sides (so datetimes are compared after the UTCDateTime
  normalisation, not as backend-specific text).

This module imports no alembic; the import-boundary test keeps that to the db
entrypoint.
'''
import asyncio
import hashlib
import os

from sqlalchemy import Column, MetaData, PrimaryKeyConstraint, String, Table, inspect, select
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.functions import count

from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db import setup_db
from discord_db.database import BASE

# Rows per read and per insert. Large enough that a table of a few million rows
# is a few hundred round trips, small enough that no chunk is a memory problem.
CHUNK_SIZE = 5000

# Alembic's own layout for its bookkeeping table (same column, same constraint
# name), so a later `alembic upgrade head` finds exactly what it would have
# created. Declared here, in its own MetaData, rather than imported from alembic
# (see the module docstring) and kept out of BASE so the models stay the models.
VERSION_METADATA = MetaData()
VERSION_TABLE = Table(
    'alembic_version', VERSION_METADATA,
    Column('version_num', String(32), nullable=False),
    PrimaryKeyConstraint('version_num', name='alembic_version_pkc'),
)


def _engine_for(dsn: str):
    '''Build the same engine the db pod would for this DSN.'''
    return setup_db(GeneralConfig(sql_connection_statement=dsn))


def _remove_sqlite_files(path: str) -> None:
    '''Delete a SQLite file and any journal sidecars it left behind.'''
    for suffix in ('', '-wal', '-shm', '-journal'):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


def refuse_leftover_sidecars(path: str) -> None:
    '''Raise if a WAL or shared-memory file is still beside the database at path.

    WAL is on for the connections this module opens, and the last close folds the
    log back into the main file and deletes it. One still being there means the
    main file alone is missing data, so it must not be renamed into place.
    '''
    for suffix in ('-wal', '-shm'):
        if os.path.exists(path + suffix):
            raise DiscordBotException(
                f'{path + suffix} was left behind; the copy at {path} is incomplete'
            )


class _Digest:
    '''Row count and running SHA-256 over a table's rows, in order.'''

    def __init__(self):
        self.rows = 0
        self._hash = hashlib.sha256()

    def add(self, chunk) -> None:
        '''Fold a chunk of row mappings into the digest.'''
        for row in chunk:
            self._hash.update(repr(tuple(row.values())).encode())
            self.rows += 1

    @property
    def hexdigest(self) -> str:
        '''The digest so far, as hex.'''
        return self._hash.hexdigest()


def _ordered_select(table):
    '''SELECT every column of the table, ordered by its primary key.'''
    return select(table).order_by(*table.primary_key.columns)


async def _check_source_schema(source: AsyncConnection, allow_extra_tables: bool) -> str:
    '''Verify the source matches the models and return its alembic revision.

    source : Open connection to the source database
    allow_extra_tables : Tolerate source tables the models do not describe
    '''
    present = set(await source.run_sync(lambda conn: inspect(conn).get_table_names()))
    wanted = {table.name for table in BASE.metadata.sorted_tables}
    problems = [f'table {name} is missing from the source' for name in sorted(wanted - present)]
    extra = present - wanted - {VERSION_TABLE.name}
    if extra and not allow_extra_tables:
        problems.append(
            f'the source has tables the models do not know: {sorted(extra)} '
            '(pass --allow-extra-tables to leave them behind)'
        )
    for table in BASE.metadata.sorted_tables:
        if table.name not in present:
            continue
        actual = {column['name'] for column in await source.run_sync(
            lambda conn, name=table.name: inspect(conn).get_columns(name))}
        expected = {column.name for column in table.columns}
        if actual != expected:
            problems.append(
                f'table {table.name} differs from the models: '
                f'missing {sorted(expected - actual)}, unexpected {sorted(actual - expected)}'
            )
    if VERSION_TABLE.name not in present:
        problems.append(f'the source has no {VERSION_TABLE.name} table')
    if problems:
        raise DiscordBotException('; '.join(problems))
    versions = (await source.execute(select(VERSION_TABLE.c.version_num))).scalars().all()
    if len(versions) != 1:
        raise DiscordBotException(
            f'expected exactly one {VERSION_TABLE.name} row in the source, found {len(versions)}'
        )
    return versions[0]


async def _copy_table(source: AsyncConnection, dest: AsyncConnection, table) -> _Digest:
    '''Stream one table from source to dest, returning what was read.'''
    digest = _Digest()
    result = await source.stream(_ordered_select(table).execution_options(yield_per=CHUNK_SIZE))
    async for chunk in result.mappings().partitions(CHUNK_SIZE):
        digest.add(chunk)
        await dest.execute(table.insert(), [dict(row) for row in chunk])
    return digest


async def read_table(conn: AsyncConnection, table) -> _Digest:
    '''Digest one table as it now stands in the connection's database.'''
    digest = _Digest()
    result = await conn.stream(_ordered_select(table).execution_options(yield_per=CHUNK_SIZE))
    async for chunk in result.mappings().partitions(CHUNK_SIZE):
        digest.add(chunk)
    return digest


async def count_rows(conn: AsyncConnection, table) -> int:
    '''Number of rows currently in the table.'''
    return (await conn.execute(select(count()).select_from(table))).scalar_one()


async def copy_database(source_dsn: str, dest_path: str, allow_extra_tables: bool = False) -> dict:
    '''Copy every model table from source_dsn into a new SQLite file at dest_path.

    Returns {table name: (row count, sha256 hex)}. Raises DiscordBotException
    for any refusal or any mismatch; nothing is left at dest_path in that case.

    source_dsn : DSN of the database to read (postgresql:// or sqlite:///)
    dest_path : Path of the SQLite file to create; must not exist
    allow_extra_tables : Tolerate source tables the models do not describe
    '''
    if os.path.exists(dest_path):
        raise DiscordBotException(f'{dest_path} already exists; refusing to overwrite it')
    partial_path = dest_path + '.partial'
    _remove_sqlite_files(partial_path)
    try:
        source_engine = _engine_for(source_dsn)
    except (ValueError, ArgumentError) as error:
        # Reached when the DSN env var was unset: pyaml-env turns a missing
        # variable into the string "N/A", which is not a URL.
        raise DiscordBotException(f'the source DSN is not usable: {error}') from error
    dest_engine = _engine_for(f'sqlite:///{partial_path}')
    report = {}
    try:
        async with source_engine.connect() as source:
            version = await _check_source_schema(source, allow_extra_tables)
            async with dest_engine.begin() as dest:
                await dest.run_sync(BASE.metadata.create_all)
                await dest.run_sync(VERSION_METADATA.create_all)
                await dest.execute(VERSION_TABLE.insert(), {'version_num': version})
            for table in BASE.metadata.sorted_tables:
                async with dest_engine.begin() as dest:
                    copied = await _copy_table(source, dest, table)
                async with dest_engine.connect() as dest:
                    stored = await read_table(dest, table)
                if (copied.rows, copied.hexdigest) != (stored.rows, stored.hexdigest):
                    raise DiscordBotException(
                        f'table {table.name}: read {copied.rows} rows from the source but '
                        f'{stored.rows} came back from the copy, or their contents differ'
                    )
                if await count_rows(source, table) != copied.rows:
                    raise DiscordBotException(
                        f'table {table.name} changed in the source while it was being copied; '
                        'stop the db pod and run again'
                    )
                report[table.name] = (copied.rows, copied.hexdigest)
        await dest_engine.dispose()
        refuse_leftover_sidecars(partial_path)
        os.replace(partial_path, dest_path)
    except BaseException:
        await dest_engine.dispose()
        _remove_sqlite_files(partial_path)
        raise
    finally:
        await source_engine.dispose()
    return report


def copy_from_config(general_config: GeneralConfig, dest_path: str,
                     allow_extra_tables: bool = False) -> dict:
    '''Run copy_database against the DSN this pod is configured with.

    general_config : The pod's validated general config
    dest_path : Path of the SQLite file to create; must not exist
    allow_extra_tables : Tolerate source tables the models do not describe
    '''
    if not general_config.sql_connection_statement:
        raise DiscordBotException('general.sql_connection_statement is not set; nothing to copy from')
    return asyncio.run(copy_database(general_config.sql_connection_statement, dest_path,
                                     allow_extra_tables))
