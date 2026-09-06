'''Bring the database schema to head, from inside the process that serves it.

Its own module rather than a function in cli/_lib/db.py, for the same reason
that file exists: this one imports alembic, and only the db entrypoint may. The
import-boundary test asserts the shape from the other side.

**Nothing ran migrations before this.** `cli/_lib/db.py::setup_db` called
`BASE.metadata.create_all`, which creates missing tables and never ALTERs
anything, so every schema change to date was applied by hand. See
projects/alembic-migration-ownership.md (docs) for how the chain was repaired and
why the runner landed here rather than in an initContainer or a Job.

**It ships disabled.** `general.run_migrations` defaults to False, so merging
this changes nothing in prod until the ConfigMap says otherwise -- the same
sequencing the db tier's routes and entrypoint used. That is not caution for its
own sake: prod is stamped at head while at least one of the revisions it is
stamped past demonstrably never ran, and an automatic upgrade against that
database is a no-op that freezes the mismatch. The flag is what keeps "the code
exists" separate from "it ran".
'''
import os
from pathlib import Path

from alembic import command
from alembic.config import Config

from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.common import GeneralConfig

# alembic.ini sets `script_location = %(here)s/alembic`, so the ini's own
# directory decides where the revisions are read from. Relative by design: it
# resolves against the working directory, which is the repo root in development
# and /opt/discord in the image, and both hold the pair. ALEMBIC_CONFIG overrides
# it for anything that runs from somewhere else.
DEFAULT_CONFIG_FILENAME = 'alembic.ini'


def _config_path() -> Path:
    return Path(os.environ.get('ALEMBIC_CONFIG', DEFAULT_CONFIG_FILENAME)).resolve()


def run_pending_migrations(general_config: GeneralConfig) -> bool:
    '''Run `alembic upgrade head` when enabled. Returns whether it ran.

    Called before the process binds a port, and deliberately not inside the
    event loop: alembic/env.py calls asyncio.run() at module scope, so an
    already-running loop turns this into a RuntimeError at the worst possible
    moment. test_migrations.py pins that ordering.
    '''
    if not general_config.run_migrations:
        return False
    if not general_config.sql_connection_statement:
        raise DiscordBotException(
            'general.run_migrations is set but general.sql_connection_statement is not'
        )
    config_path = _config_path()
    if not config_path.is_file():
        # Loud, and naming the path. The alternative is alembic reporting "no
        # such revision" or building nothing at all, from an image whose
        # Dockerfile forgot to COPY the migrations -- a failure that reads as a
        # broken chain rather than a missing file.
        raise DiscordBotException(
            f'general.run_migrations is set but no alembic config at {config_path}'
        )
    config = Config(str(config_path))
    # env.py reads this in preference to DATABASE_URL, so the upgrade lands on
    # the database this process is configured to serve rather than on whatever
    # the environment happens to say.
    config.attributes['database_url'] = general_config.sql_connection_statement
    command.upgrade(config, 'head')
    return True
