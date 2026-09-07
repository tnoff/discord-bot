Fixed the migration runner switching off the process's own logging.

`alembic/env.py` calls `fileConfig()`, which defaults to
`disable_existing_loggers=True` and disables every logger not named in
`alembic.ini`. Under the CLI that is harmless. In-process it disabled the
loggers already carrying the OTLP handler, so the db tier stopped shipping logs
the moment `general.run_migrations` was enabled -- while staying healthy and
serving normally, which is why nothing caught it.

`env.py` now honours a `configure_logger` attribute, the runner sets it False
and raises the `alembic` logger itself so `Running upgrade` reaches the app's
handlers rather than only stdout.
