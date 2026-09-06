import asyncio
import os
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context

from discord_bot.database import BASE

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Convert the URL to the asyncpg driver. PostgreSQL is the only supported backend.
#
# config.attributes first, then the environment. The in-process runner
# (discord_bot/cli/_lib/migrations.py) passes the DSN the pod is already
# configured with, so the schema is upgraded on the same database the process is
# about to serve -- taking it from the environment there would let
# general.sql_connection_statement and DATABASE_URL disagree, and the migration
# would win silently. The CLI path (`alembic upgrade head`, and every test in
# tests/test_alembic_chain.py) sets no attribute and keeps reading the variable.
_database_url = config.attributes.get("database_url") or os.environ.get("DATABASE_URL")
if not _database_url:
    raise RuntimeError("DATABASE_URL must be set to a postgresql:// connection string")
_raw_url = make_url(_database_url)
if not _raw_url.drivername.startswith("postgresql"):
    raise RuntimeError(
        f"Unsupported database driver {_raw_url.drivername!r}; only postgresql is supported"
    )
_async_url = _raw_url.set(drivername="postgresql+asyncpg")

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = BASE.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=_async_url.render_as_string(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


# A constant, and it must stay constant: the lock only serializes processes that
# agree on the key. Advisory locks are namespaced per DATABASE, not per table, so
# every pod pointed at this database contends on this one number.
MIGRATION_LOCK_KEY = 0x646973636f726462


def _do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        # Serialize concurrent upgrades. discord-db runs replicas: 1, but two
        # pods exist on every rollout (maxSurge: 1) and a node loss mid-rollout
        # can leave two of them STARTING -- so "only one process migrates" is a
        # convention, not a guarantee, and nothing enforces the replica count.
        #
        # Measured without this lock: the loser dies with
        # UniqueViolationError on pg_type_typname_nsp_index, both processes
        # having tried to CREATE TABLE alembic_version. The database survives
        # (postgres DDL is transactional, so the loser rolls back) -- what does
        # not survive is the pod, and on this tier that is a CrashLoop and a
        # failed rollout. With it, the loser blocks, re-reads the version inside
        # the lock, finds head and runs nothing.
        #
        # _xact_ rather than pg_advisory_lock: it releases with the transaction,
        # so there is no unlock path to get wrong and an OOM-killed pod drops the
        # lock with its session instead of wedging the next one.
        #
        # Correctness rests on READ COMMITTED, postgres' default: the version
        # read after acquiring the lock takes a fresh snapshot and sees the
        # winner's commit. Under REPEATABLE READ the loser would read a stale
        # version and try to re-apply what just landed.
        connection.exec_driver_sql(f"SELECT pg_advisory_xact_lock({MIGRATION_LOCK_KEY})")
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode using an async engine."""
    engine = create_async_engine(_async_url, poolclass=pool.NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(_do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
