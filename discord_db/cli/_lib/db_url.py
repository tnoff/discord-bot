'''Map the configured database DSN onto the async driver this pod runs.

Its own module, with nothing but sqlalchemy.engine.url imported, because two
callers need the same answer and must not disagree: cli/_lib/db.py (the serving
engine) and alembic/env.py (the migration engine). A DSN one accepted and the
other rejected would let the pod migrate one database and serve another.

PostgreSQL and SQLite are the supported backends. Config files use the plain
`postgresql://` / `sqlite:///` forms; the async driver is chosen here, so
`+asyncpg` or `+aiosqlite` written into config is a mistake the rewrite would
otherwise double-apply.
'''
from sqlalchemy.engine.url import URL, make_url

POSTGRES = 'postgresql'
SQLITE = 'sqlite'

_ASYNC_DRIVERS = {
    POSTGRES: 'postgresql+asyncpg',
    SQLITE: 'sqlite+aiosqlite',
}


def backend_name(raw_url: str) -> str:
    '''Return 'postgresql' or 'sqlite' for a DSN, raising ValueError otherwise.

    raw_url : Connection string as configured
    '''
    name = make_url(raw_url).get_backend_name()
    if name not in _ASYNC_DRIVERS:
        raise ValueError(
            f'Unsupported database driver {make_url(raw_url).drivername!r}; '
            'only postgresql and sqlite are supported'
        )
    return name


def async_url(raw_url: str) -> URL:
    '''Return the DSN with its drivername swapped for the async driver.

    raw_url : Connection string as configured
    '''
    return make_url(raw_url).set(drivername=_ASYNC_DRIVERS[backend_name(raw_url)])


def is_in_memory_sqlite(url: URL) -> bool:
    '''True for a sqlite URL with no file behind it.

    url : Parsed URL
    '''
    return url.get_backend_name() == SQLITE and url.database in (None, '', ':memory:')
