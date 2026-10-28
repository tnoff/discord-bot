Add a CI guard that fails a new Alembic revision which drops, renames or alters schema unless it declares `CONTRACT_STEP`, so a rolling update never runs the old pod against a schema it cannot read.
