'''Tests for the schema runner on the db entrypoint.

The chain itself is tested against a real postgres in tests/test_alembic_chain.py
-- that it replays, that concurrent runs survive each other, that an older image
refuses a newer database. What is left for here is the decision layer: whether
the runner fires at all, what it hands alembic, and how it fails when the image
is missing the files it would need.
'''
import pytest

from discord_bot.cli._lib import migrations
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.common import GeneralConfig

DSN = 'postgresql://user:pass@postgres.example:5432/discord'


class _Recorder:
    '''Captures what would have been handed to alembic.'''
    def __init__(self):
        self.config = None
        self.revision = None
        self.calls = 0

    def __call__(self, config, revision):
        self.config = config
        self.revision = revision
        self.calls += 1


@pytest.fixture(name='recorder')
def fixture_recorder(mocker):
    recorder = _Recorder()
    mocker.patch.object(migrations.command, 'upgrade', recorder)
    return recorder


def test_disabled_by_default_and_runs_nothing(recorder):
    '''The flag is what keeps merging this out of prod's schema.'''
    config = GeneralConfig(sql_connection_statement=DSN)
    assert config.run_migrations is False
    assert migrations.run_pending_migrations(config) is False
    assert recorder.calls == 0


def test_enabled_upgrades_to_head_on_the_configured_dsn(recorder, tmp_path, monkeypatch):
    '''The DSN comes from config, not the environment.

    env.py prefers config.attributes over DATABASE_URL precisely so these two
    cannot disagree -- an upgrade that lands on a different database than the
    process serves is the failure this wiring exists to prevent, and it would
    not announce itself.
    '''
    ini = tmp_path / 'alembic.ini'
    ini.write_text('[alembic]\nscript_location = %(here)s/alembic\n')
    monkeypatch.setenv('ALEMBIC_CONFIG', str(ini))
    monkeypatch.setenv('DATABASE_URL', 'postgresql://wrong:wrong@elsewhere:5432/wrong')

    config = GeneralConfig(sql_connection_statement=DSN, run_migrations=True)
    assert migrations.run_pending_migrations(config) is True

    assert recorder.calls == 1
    assert recorder.revision == 'head'
    assert recorder.config.attributes['database_url'] == DSN
    assert recorder.config.config_file_name == str(ini)


def test_enabled_without_a_dsn_is_a_startup_error(recorder):
    '''Nothing to migrate is a misconfiguration, not a no-op to shrug at.'''
    config = GeneralConfig(run_migrations=True)
    with pytest.raises(DiscordBotException) as exc:
        migrations.run_pending_migrations(config)
    assert 'sql_connection_statement' in str(exc.value)
    assert recorder.calls == 0


def test_missing_config_file_names_the_path(recorder, tmp_path, monkeypatch):
    '''An image that forgot the COPY must say so.

    Dockerfile.db lands alembic/ and alembic.ini in ${WORKDIR} because the
    install layer deletes ${APPDIR}. If that regresses, the failure should name
    the path it looked at rather than surfacing as an alembic error about a
    chain that is perfectly fine.
    '''
    monkeypatch.setenv('ALEMBIC_CONFIG', str(tmp_path / 'nope' / 'alembic.ini'))
    config = GeneralConfig(sql_connection_statement=DSN, run_migrations=True)
    with pytest.raises(DiscordBotException) as exc:
        migrations.run_pending_migrations(config)
    assert 'no alembic config at' in str(exc.value)
    assert str(tmp_path / 'nope' / 'alembic.ini') in str(exc.value)
    assert recorder.calls == 0


def test_config_path_defaults_to_the_working_directory(monkeypatch):
    '''Relative on purpose: repo root in development, /opt/discord in the image.'''
    monkeypatch.delenv('ALEMBIC_CONFIG', raising=False)
    monkeypatch.chdir('/tmp')
    assert migrations._config_path().name == 'alembic.ini'  # pylint: disable=protected-access
    assert str(migrations._config_path()).startswith('/tmp')  # pylint: disable=protected-access
