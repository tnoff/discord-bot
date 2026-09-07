`setup_db` no longer builds a schema.

It called `BASE.metadata.create_all`, which creates missing tables and never
`ALTER`s anything -- so it could stand up a fresh database but never migrate
one, which is why every schema change before the alembic runner was applied by
hand. The chain owns that job now.

`setup_db` also opens no connection at all any more, which removes the
throwaway-event-loop hazard that made the explicit dispose load-bearing rather
than just deleting the call. Its docstring described behaviour it did not have
for a long time; it now describes what the function does.
