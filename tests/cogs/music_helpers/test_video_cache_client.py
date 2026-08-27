from functools import partial
from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import asc, select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.database import VideoCache
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.interfaces.database_protocols import VideoCacheStore
from discord_bot.types.video_cache import VideoCacheEntry

from tests.helpers import async_mock_session, fake_source_dict, fake_media_download, generate_fake_context
from tests.helpers import fake_engine #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_iterate_file_new_and_iterate(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            await x.iterate_file(s)
            await x.iterate_file(s)
            async with async_mock_session(fake_engine) as session:
                assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 1
                query = (await session.execute(select(VideoCache))).scalars().first()
                assert query.count == 2

@pytest.mark.asyncio
async def test_webpage_get_source(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            await x.iterate_file(s)
            result = await x.get_webpage_url_item(s.media_request)
            assert result.file_path
            assert result.webpage_url == s.media_request.search_result.resolved_search_string

@pytest.mark.asyncio
async def test_webpage_get_source_non_existing(fake_engine):  #pylint:disable=redefined-outer-name
    fake_context = generate_fake_context()
    x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
    sd = fake_source_dict(fake_context, is_direct_search=True)
    result = await x.get_webpage_url_item(sd)
    assert result is None

@pytest.mark.asyncio
async def test_iterate_file_stores_file_size(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            s.file_size_bytes = 12345
            await x.iterate_file(s)
            async with async_mock_session(fake_engine) as session:
                query = (await session.execute(select(VideoCache))).scalars().first()
                assert query.file_size_bytes == 12345

@pytest.mark.asyncio
async def test_cache_hit_propagates_file_size(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            s.file_size_bytes = 99999
            await x.iterate_file(s)
            result = await x.get_webpage_url_item(s.media_request)
            assert result.file_size_bytes == 99999

@pytest.mark.asyncio
async def test_ready_remove_size_limit(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        # size limit: 300 bytes; two files at 200 bytes each => oldest must be evicted
        x = VideoCacheClient(100, partial(async_mock_session, fake_engine), max_cache_size_bytes=300)
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            with fake_media_download(tmp_dir, fake_context=fake_context) as t:
                s.file_size_bytes = 200
                t.file_size_bytes = 200
                await x.iterate_file(s)
                await x.iterate_file(t)
                await x.ready_remove()
                async with async_mock_session(fake_engine) as session:
                    flagged = (await session.execute(select(sql_count()).select_from(VideoCache).where(VideoCache.ready_for_deletion.is_(True)))).scalar()
                    assert flagged == 1

@pytest.mark.asyncio
async def test_ready_remove_count_and_size_combined(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        # count limit 2, size limit 500 bytes; three files at 200 bytes each
        # count eviction removes 1 (oldest) → 1 flagged
        # size eviction: excludes already-flagged entry; sees 2 unflagged at 200 bytes = 400 <= 500 → no extra flags
        # Without the exclusion, total would be 600 > 500 and size would flag a second entry
        x = VideoCacheClient(2, partial(async_mock_session, fake_engine), max_cache_size_bytes=500)
        with fake_media_download(tmp_dir, fake_context=fake_context) as a:
            with fake_media_download(tmp_dir, fake_context=fake_context) as b:
                with fake_media_download(tmp_dir, fake_context=fake_context) as c:
                    a.file_size_bytes = 200
                    b.file_size_bytes = 200
                    c.file_size_bytes = 200
                    await x.iterate_file(a)
                    await x.iterate_file(b)
                    await x.iterate_file(c)
                    await x.ready_remove()
                    async with async_mock_session(fake_engine) as session:
                        flagged = (await session.execute(select(sql_count()).select_from(VideoCache).where(VideoCache.ready_for_deletion.is_(True)))).scalar()
                        assert flagged == 1

@pytest.mark.asyncio
async def test_storage_type_mismatch_iterate_updates_path(fake_engine):  #pylint:disable=redefined-outer-name
    '''iterate_file with a different storage_type updates base_path and storage_type in-place.'''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        mr = fake_source_dict(fake_context, is_direct_search=True)
        # Insert with 'local' storage type
        x_local = VideoCacheClient(10, partial(async_mock_session, fake_engine), storage_type='local')
        with fake_media_download(tmp_dir, media_request=mr) as s:
            await x_local.iterate_file(s)
            async with async_mock_session(fake_engine) as session:
                entry = (await session.execute(select(VideoCache))).scalars().first()
                assert entry.storage_type == 'local'
                old_path = entry.base_path

            # Re-iterate the same URL with 's3' storage type and a new file path
            x_s3 = VideoCacheClient(10, partial(async_mock_session, fake_engine), storage_type='s3')
            with fake_media_download(tmp_dir, media_request=mr) as t:
                await x_s3.iterate_file(t)
                async with async_mock_session(fake_engine) as session:
                    assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 1
                    entry = (await session.execute(select(VideoCache))).scalars().first()
                    assert entry.storage_type == 's3'
                    assert entry.base_path == str(t.file_path)
                    assert entry.base_path != old_path
                    assert entry.ready_for_deletion is False


@pytest.mark.asyncio
async def test_storage_type_mismatch_get_returns_none(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_webpage_url_item returns None and flags entry when storage_type doesn't match.'''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x_local = VideoCacheClient(10, partial(async_mock_session, fake_engine), storage_type='local')
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            await x_local.iterate_file(s)
            # Now query as if we switched to s3 mode
            x_s3 = VideoCacheClient(10, partial(async_mock_session, fake_engine), storage_type='s3')
            result = await x_s3.get_webpage_url_item(s.media_request)
            assert result is None
            async with async_mock_session(fake_engine) as session:
                entry = (await session.execute(select(VideoCache))).scalars().first()
                assert entry.ready_for_deletion is True


@pytest.mark.asyncio
async def test_remove(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(1, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            with fake_media_download(tmp_dir, fake_context=fake_context) as t2:
                await x.iterate_file(s)
                await x.iterate_file(t2)
                await x.ready_remove()

                async with async_mock_session(fake_engine) as session:
                    assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 2
                    query = (await session.execute(
                        select(VideoCache).order_by(asc(VideoCache.last_iterated_at))
                    )).scalars().first()
                    assert query.ready_for_deletion is True

                    await x.remove_video_cache([query.id])
                    assert (await session.execute(select(sql_count()).select_from(VideoCache))).scalar() == 1


@pytest.mark.asyncio
async def test_get_cache_count(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
        assert await x.get_cache_count() == 0
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            await x.iterate_file(s)
            assert await x.get_cache_count() == 1


@pytest.mark.asyncio
async def test_get_deletable_entries_returns_detached_entries(fake_engine):  #pylint:disable=redefined-outer-name
    '''The entries outlive the session that loaded them.

    Reading base_path after the session generator closes is what cache_cleanup
    does -- it holds the list across a delete_file loop.  On live ORM rows that
    is a DetachedInstanceError waiting on lazy-load timing; the assertion here
    is that the values are already materialized.
    '''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(1, partial(async_mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            s.file_size_bytes = 4096
            await x.iterate_file(s)
        async with async_mock_session(fake_engine) as session:
            row = (await session.execute(select(VideoCache))).scalars().first()
            row.ready_for_deletion = True
            await session.commit()
            row_id = row.id

        entries = await x.get_deletable_entries()

        assert [e.id for e in entries] == [row_id]
        entry = entries[0]
        assert isinstance(entry, VideoCacheEntry)
        assert entry.video_url == s.webpage_url
        assert entry.base_path
        assert entry.file_size_bytes == 4096
        assert entry.ready_for_deletion is True
        # Round-trips as JSON, which the ORM row it replaced does not.
        assert VideoCacheEntry.model_validate(entry.model_dump(mode='json')) == entry


@pytest.mark.asyncio
async def test_get_deletable_entries_empty_when_nothing_flagged(fake_engine):  #pylint:disable=redefined-outer-name
    '''Nothing flagged is an empty list, not an error.'''
    x = VideoCacheClient(10, partial(async_mock_session, fake_engine))
    assert await x.get_deletable_entries() == []


def test_video_cache_client_satisfies_the_store_protocol():
    '''VideoCacheClient is the in-memory VideoCacheStore.

    runtime_checkable only checks method names, so this catches a rename or a
    dropped method -- the failure mode when the Protocol and its only
    implementation drift while the HTTP sibling does not exist yet to notice.
    '''
    assert isinstance(VideoCacheClient(10, partial(async_mock_session, None)), VideoCacheStore)
