'''Tests for the db pod's engine helpers (managed_db, setup_db).'''
from unittest.mock import MagicMock, AsyncMock

import pytest

from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db import managed_db, setup_db


def test_managed_db_rewrites_the_url_and_disposes_without_a_loop(mocker):
    '''postgresql:// is rewritten to postgresql+asyncpg, and dispose runs with no loop.

    Both halves used to be reached through the bot entrypoint, which built an
    engine of its own. It does not any more -- the bot holds HTTP stores and the
    db pod is the only process that opens a connection -- so this drives
    managed_db directly rather than through an entrypoint that would no longer
    touch it.

    Deliberately synchronous: dispose_db_engine's no-running-loop branch is the
    one that runs on a pod roll (managed_db's finally fires after run_loop's
    asyncio.run has already returned), and it is only reachable from a test with
    no loop of its own. close=False is asserted because passing close=True there
    is what logged a traceback per pooled connection on every shutdown.
    '''

    mock_async_engine = AsyncMock()
    mock_async_engine.sync_engine = MagicMock()
    create_async_engine_mock = mocker.patch('discord_db.cli._lib.db.create_async_engine',
                                            return_value=mock_async_engine)

    general_config = GeneralConfig(
        discord_token='foo',
        sql_connection_statement='postgresql://user:pass@localhost/testdb')

    with managed_db(general_config) as engine:
        assert engine is mock_async_engine

    called_url = create_async_engine_mock.call_args[0][0]
    assert 'asyncpg' in str(called_url)
    mock_async_engine.dispose.assert_awaited_once_with(close=False)


def test_setup_db_rejects_unsupported_backends():
    '''Only postgresql and sqlite are supported; everything else raises.'''
    with pytest.raises(ValueError, match='Unsupported database driver'):
        setup_db(GeneralConfig(discord_token='foo', sql_connection_statement='mysql://u:p@h/db'))
    with pytest.raises(ValueError, match='Unsupported database driver'):
        setup_db(GeneralConfig(discord_token='foo', sql_connection_statement='oracle://u:p@h/db'))
