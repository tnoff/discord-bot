'''The one-off PostgreSQL -> SQLite copy.

CI has no database server, so the source here is a SQLite file too. That is a
fair test of everything the tool is responsible for -- schema checks, ordering,
verification, cleanup -- because it reads and writes only through SQLAlchemy and
the models. The PostgreSQL-specific part is the driver, and the type round trip
that matters (timezone-aware datetimes) goes through UTCDateTime on both sides.
'''
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

from discord_core.exceptions import DiscordBotException

from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib import copy_data
from discord_db.database import (BASE, Guild, GuildVideoAnalytics, MarkovChannel, MarkovRelation,
                                 Playlist, PlaylistItem, VideoCache)

REVISION = 'abc123def456'
NOW = datetime(2026, 10, 2, 12, 30, 45, 123456, tzinfo=timezone.utc)


def _build_source(tmp_path, revision_rows=(REVISION,)):
    '''A SQLite stand-in for the production source, with a row in every table.'''
    path = tmp_path / 'source.db'
    engine = create_engine(f'sqlite:///{path}')
    BASE.metadata.create_all(engine)
    with engine.begin() as conn:
        copy_data.VERSION_METADATA.create_all(conn)
        for revision in revision_rows:
            conn.execute(text('INSERT INTO alembic_version VALUES (:r)'), {'r': revision})
        conn.execute(MarkovChannel.__table__.insert(), [
            {'id': 1, 'channel_id': 2 ** 40, 'server_id': 2 ** 41, 'last_message_id': 7},
            {'id': 2, 'channel_id': 5, 'server_id': 6, 'last_message_id': None},
        ])
        conn.execute(MarkovRelation.__table__.insert(), [
            {'id': i, 'channel_id': 1 + i % 2, 'leader_word': f'lead{i}',
             'follower_word': f'follow{i} 日本', 'created_at': NOW + timedelta(seconds=i)}
            for i in range(1, 12001)  # more than two chunks
        ])
        conn.execute(Playlist.__table__.insert(), [
            {'id': 1, 'name': 'mix', 'server_id': 2 ** 40, 'last_queued': NOW,
             'created_at': NOW, 'is_history': False},
            {'id': 2, 'name': 'hist', 'server_id': 2 ** 40, 'last_queued': None,
             'created_at': NOW, 'is_history': True},
        ])
        conn.execute(PlaylistItem.__table__.insert(), [
            {'id': 1, 'title': 't', 'video_url': 'https://x/1', 'uploader': 'u',
             'playlist_id': 1, 'created_at': NOW},
        ])
        conn.execute(VideoCache.__table__.insert(), [
            {'id': 1, 'video_id': 'v', 'video_url': 'https://x/1', 'title': 't', 'uploader': 'u',
             'duration': 61, 'extractor': 'youtube', 'last_iterated_at': NOW, 'created_at': NOW,
             'count': 3, 'ready_for_deletion': False, 'file_size_bytes': None,
             'base_path': '/c/v', 'storage_type': 's3'},
        ])
        conn.execute(Guild.__table__.insert(), [{'id': 1, 'server_id': 2 ** 40}])
        conn.execute(GuildVideoAnalytics.__table__.insert(), [
            {'id': 1, 'guild_id': 1, 'total_plays': 9, 'cached_plays': 4,
             'total_duration_days': 1, 'total_duration_seconds': 5,
             'created_at': NOW, 'updated_at': NOW},
        ])
    engine.dispose()
    return path


def _execute(path, statement):
    engine = create_engine(f'sqlite:///{path}')
    with engine.begin() as conn:
        conn.execute(text(statement))
    engine.dispose()


def _dump(path):
    '''Every table's rows as plain tuples, for comparing two files.'''
    engine = create_engine(f'sqlite:///{path}')
    try:
        with engine.connect() as conn:
            out = {table.name: conn.execute(
                table.select().order_by(*table.primary_key.columns)).fetchall()
                for table in BASE.metadata.sorted_tables}
            out['version'] = conn.execute(text('SELECT version_num FROM alembic_version')).fetchall()
            out['fk'] = conn.execute(text('PRAGMA foreign_key_check')).fetchall()
            out['integrity'] = conn.execute(text('PRAGMA integrity_check')).fetchall()
        return out
    finally:
        engine.dispose()


async def _copy(source, dest, **kwargs):
    return await copy_data.copy_database(f'sqlite:///{source}', str(dest), **kwargs)


@pytest.mark.asyncio
async def test_copies_every_table_and_verifies_it(tmp_path):
    '''Rows, types and ordering survive; the report names every table.'''
    source = _build_source(tmp_path)
    dest = tmp_path / 'out' / 'discord.db'
    dest.parent.mkdir()
    report = await _copy(source, dest)
    assert {name: rows for name, (rows, _) in report.items()} == {
        'markov_channel': 2, 'markov_relation': 12000, 'playlist': 2, 'playlist_item': 1,
        'video_cache': 1, 'guild': 1, 'server_video_analytics': 1,
    }
    copied = _dump(dest)
    assert copied == _dump(source)
    assert copied['version'] == [(REVISION,)]
    assert copied['fk'] == []
    assert copied['integrity'] == [('ok',)]
    assert copied['markov_channel'][0].channel_id == 2 ** 40
    assert copied['markov_relation'][0].created_at == NOW + timedelta(seconds=1)
    assert copied['markov_relation'][0].created_at.tzinfo is not None
    assert sorted(p.name for p in dest.parent.iterdir()) == ['discord.db']


@pytest.mark.asyncio
async def test_an_empty_source_copies_to_an_empty_file(tmp_path):
    '''A database with no rows is a valid copy, not an error.'''
    source = _build_source(tmp_path)
    for table in reversed(BASE.metadata.sorted_tables):
        _execute(source, f'DELETE FROM {table.name}')
    report = await _copy(source, tmp_path / 'discord.db')
    assert all(rows == 0 for rows, _ in report.values())


@pytest.mark.asyncio
async def test_refuses_to_overwrite_an_existing_destination(tmp_path):
    '''An existing file is never touched.'''
    source = _build_source(tmp_path)
    dest = tmp_path / 'discord.db'
    dest.write_text('precious')
    with pytest.raises(DiscordBotException, match='already exists'):
        await _copy(source, dest)
    assert dest.read_text() == 'precious'


@pytest.mark.asyncio
async def test_a_stale_partial_file_is_replaced(tmp_path):
    '''A leftover from an interrupted run does not block the next one.'''
    source = _build_source(tmp_path)
    dest = tmp_path / 'discord.db'
    (tmp_path / 'discord.db.partial').write_text('junk from a killed run')
    await _copy(source, dest)
    assert dest.exists()
    assert not (tmp_path / 'discord.db.partial').exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('rows', [(), ('one', 'two')])
async def test_needs_exactly_one_alembic_revision(tmp_path, rows):
    '''No revision, or two, means the source is not in a state to copy.'''
    source = _build_source(tmp_path, revision_rows=rows)
    with pytest.raises(DiscordBotException, match='exactly one alembic_version row'):
        await _copy(source, tmp_path / 'discord.db')
    assert not (tmp_path / 'discord.db').exists()
    assert not (tmp_path / 'discord.db.partial').exists()


@pytest.mark.asyncio
async def test_refuses_a_source_without_alembic_version(tmp_path):
    '''A database that was never stamped is named, not guessed at.'''
    source = _build_source(tmp_path)
    _execute(source, 'DROP TABLE alembic_version')
    with pytest.raises(DiscordBotException, match='no alembic_version table'):
        await _copy(source, tmp_path / 'discord.db')


@pytest.mark.asyncio
async def test_refuses_a_missing_table(tmp_path):
    '''A model table absent from the source aborts with its name.'''
    source = _build_source(tmp_path)
    _execute(source, 'DROP TABLE server_video_analytics')
    with pytest.raises(DiscordBotException, match='table server_video_analytics is missing'):
        await _copy(source, tmp_path / 'discord.db')


@pytest.mark.asyncio
async def test_refuses_a_column_the_models_lack_and_one_they_expect(tmp_path):
    '''Column drift in either direction would silently lose or invent data.'''
    source = _build_source(tmp_path)
    _execute(source, 'ALTER TABLE guild ADD COLUMN nickname TEXT')
    _execute(source, 'ALTER TABLE playlist DROP COLUMN is_history')
    with pytest.raises(DiscordBotException) as error:
        await _copy(source, tmp_path / 'discord.db')
    message = str(error.value)
    assert 'table guild differs from the models: missing [], unexpected [\'nickname\']' in message
    assert 'table playlist differs from the models: missing [\'is_history\'], unexpected []' in message


@pytest.mark.asyncio
async def test_extra_tables_are_refused_unless_allowed(tmp_path):
    '''A table the models do not know is not copied, so it has to be a choice.'''
    source = _build_source(tmp_path)
    _execute(source, 'CREATE TABLE legacy (id INTEGER PRIMARY KEY)')
    with pytest.raises(DiscordBotException, match=r"tables the models do not know: \['legacy'\]"):
        await _copy(source, tmp_path / 'discord.db')
    report = await _copy(source, tmp_path / 'discord.db', allow_extra_tables=True)
    assert 'legacy' not in report


@pytest.mark.asyncio
async def test_a_copy_that_does_not_read_back_identical_is_rejected(tmp_path, monkeypatch):
    '''If the file does not hold what was read, nothing is left at the real path.'''
    source = _build_source(tmp_path)
    real = copy_data.read_table

    async def lossy(conn, table):
        digest = await real(conn, table)
        digest.rows -= 1
        return digest

    monkeypatch.setattr(copy_data, 'read_table', lossy)
    with pytest.raises(DiscordBotException, match='came back from the copy, or their contents differ'):
        await _copy(source, tmp_path / 'discord.db')
    assert not (tmp_path / 'discord.db').exists()
    assert not (tmp_path / 'discord.db.partial').exists()


@pytest.mark.asyncio
async def test_a_source_that_changed_during_the_copy_is_rejected(tmp_path, monkeypatch):
    '''Rows added under the copy fail it, and say to stop the pod.'''
    source = _build_source(tmp_path)
    real = copy_data.count_rows

    async def grown(conn, table):
        return await real(conn, table) + 1

    monkeypatch.setattr(copy_data, 'count_rows', grown)
    with pytest.raises(DiscordBotException, match='changed in the source while it was being copied'):
        await _copy(source, tmp_path / 'discord.db')
    assert not (tmp_path / 'discord.db').exists()


def test_leftover_wal_files_block_the_rename(tmp_path):
    '''A WAL beside the file means the file alone is incomplete.'''
    path = str(tmp_path / 'x.partial')
    copy_data.refuse_leftover_sidecars(path)
    (tmp_path / 'x.partial-wal').write_text('')
    with pytest.raises(DiscordBotException, match='was left behind'):
        copy_data.refuse_leftover_sidecars(path)


def test_copy_from_config_reads_the_pods_own_dsn(tmp_path):
    '''The source is general.sql_connection_statement, not a second setting.'''
    source = _build_source(tmp_path)
    dest = tmp_path / 'discord.db'
    report = copy_data.copy_from_config(
        GeneralConfig(sql_connection_statement=f'sqlite:///{source}'), str(dest))
    assert report['guild'][0] == 1
    assert dest.exists()


def test_copy_from_config_needs_a_dsn(tmp_path):
    '''No configured database is a refusal, not an attempt on some default.'''
    with pytest.raises(DiscordBotException, match='sql_connection_statement is not set'):
        copy_data.copy_from_config(GeneralConfig(), str(tmp_path / 'discord.db'))


@pytest.mark.parametrize('dsn', ['N/A', 'mysql://u:p@h/db'])
def test_an_unusable_dsn_is_a_clean_refusal(tmp_path, dsn):
    '''An unset env var ("N/A" from pyaml-env) or a foreign backend names the DSN problem.'''
    with pytest.raises(DiscordBotException, match='the source DSN is not usable'):
        copy_data.copy_from_config(GeneralConfig(sql_connection_statement=dsn),
                                   str(tmp_path / 'discord.db'))
    assert not (tmp_path / 'discord.db').exists()
