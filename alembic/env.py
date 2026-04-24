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

# Convert the URL to the appropriate async driver.
_raw_url = make_url(os.environ.get("DATABASE_URL", "sqlite:///local.db"))
if _raw_url.drivername.startswith("postgresql"):
    _async_url = _raw_url.set(drivername="postgresql+asyncpg")
elif _raw_url.drivername == "sqlite":
    _async_url = _raw_url.set(drivername="sqlite+aiosqlite")
else:
    _async_url = _raw_url

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


def _do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
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
