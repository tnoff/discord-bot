from functools import partial
from tempfile import TemporaryDirectory

from discord_bot.database import VideoCache
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from tests.helpers import mock_session, fake_source_dict, fake_media_download, generate_fake_context
from tests.helpers import fake_engine #pylint:disable=unused-import


def test_iterate_file_new_and_iterate(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            x.iterate_file(s)
            x.iterate_file(s)
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                query = session.query(VideoCache).first()
                assert query.count == 2

def test_webpage_get_source(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            x.iterate_file(s)
            result = x.get_webpage_url_item(s.media_request)
            assert result.file_path
            assert result.webpage_url == s.media_request.search_result.resolved_search_string

def test_webpage_get_source_non_existing(fake_engine):  #pylint:disable=redefined-outer-name
    fake_context = generate_fake_context()
    x = VideoCacheClient(10, partial(mock_session, fake_engine))
    sd = fake_source_dict(fake_context, is_direct_search=True)
    result = x.get_webpage_url_item(sd)
    assert result is None

def test_iterate_file_stores_file_size(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            s.file_size_bytes = 12345
            x.iterate_file(s)
            with mock_session(fake_engine) as session:
                query = session.query(VideoCache).first()
                assert query.file_size_bytes == 12345

def test_cache_hit_propagates_file_size(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(10, partial(mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            s.file_size_bytes = 99999
            x.iterate_file(s)
            result = x.get_webpage_url_item(s.media_request)
            assert result.file_size_bytes == 99999

def test_ready_remove_size_limit(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        # size limit: 300 bytes; two files at 200 bytes each => oldest must be evicted
        x = VideoCacheClient(100, partial(mock_session, fake_engine), max_cache_size_bytes=300)
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            with fake_media_download(tmp_dir, fake_context=fake_context) as t:
                s.file_size_bytes = 200
                t.file_size_bytes = 200
                x.iterate_file(s)
                x.iterate_file(t)
                x.ready_remove()
                with mock_session(fake_engine) as session:
                    flagged = session.query(VideoCache).filter(VideoCache.ready_for_deletion.is_(True)).count()
                    assert flagged == 1

def test_ready_remove_count_and_size_combined(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        # count limit 2, size limit 500 bytes; three files at 200 bytes each
        # count eviction removes 1 (oldest) → 1 flagged
        # size eviction: excludes already-flagged entry; sees 2 unflagged at 200 bytes = 400 <= 500 → no extra flags
        # Without the exclusion, total would be 600 > 500 and size would flag a second entry
        x = VideoCacheClient(2, partial(mock_session, fake_engine), max_cache_size_bytes=500)
        with fake_media_download(tmp_dir, fake_context=fake_context) as a:
            with fake_media_download(tmp_dir, fake_context=fake_context) as b:
                with fake_media_download(tmp_dir, fake_context=fake_context) as c:
                    a.file_size_bytes = 200
                    b.file_size_bytes = 200
                    c.file_size_bytes = 200
                    x.iterate_file(a)
                    x.iterate_file(b)
                    x.iterate_file(c)
                    x.ready_remove()
                    with mock_session(fake_engine) as session:
                        flagged = session.query(VideoCache).filter(VideoCache.ready_for_deletion.is_(True)).count()
                        assert flagged == 1

def test_storage_type_mismatch_iterate_updates_path(fake_engine):  #pylint:disable=redefined-outer-name
    '''iterate_file with a different storage_type updates base_path and storage_type in-place.'''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        mr = fake_source_dict(fake_context, is_direct_search=True)
        # Insert with 'local' storage type
        x_local = VideoCacheClient(10, partial(mock_session, fake_engine), storage_type='local')
        with fake_media_download(tmp_dir, media_request=mr) as s:
            x_local.iterate_file(s)
            with mock_session(fake_engine) as session:
                entry = session.query(VideoCache).first()
                assert entry.storage_type == 'local'
                old_path = entry.base_path

            # Re-iterate the same URL with 's3' storage type and a new file path
            x_s3 = VideoCacheClient(10, partial(mock_session, fake_engine), storage_type='s3')
            with fake_media_download(tmp_dir, media_request=mr) as t:
                x_s3.iterate_file(t)
                with mock_session(fake_engine) as session:
                    assert session.query(VideoCache).count() == 1
                    entry = session.query(VideoCache).first()
                    assert entry.storage_type == 's3'
                    assert entry.base_path == str(t.file_path)
                    assert entry.base_path != old_path
                    assert entry.ready_for_deletion is False


def test_storage_type_mismatch_get_returns_none(fake_engine):  #pylint:disable=redefined-outer-name
    '''get_webpage_url_item returns None and flags entry when storage_type doesn't match.'''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x_local = VideoCacheClient(10, partial(mock_session, fake_engine), storage_type='local')
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            x_local.iterate_file(s)
            # Now query as if we switched to s3 mode
            x_s3 = VideoCacheClient(10, partial(mock_session, fake_engine), storage_type='s3')
            result = x_s3.get_webpage_url_item(s.media_request)
            assert result is None
            with mock_session(fake_engine) as session:
                entry = session.query(VideoCache).first()
                assert entry.ready_for_deletion is True


def test_remove(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(1, partial(mock_session, fake_engine))
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            with fake_media_download(tmp_dir, fake_context=fake_context) as t2:
                x.iterate_file(s)
                x.iterate_file(t2)
                x.ready_remove()

                with mock_session(fake_engine) as session:
                    assert session.query(VideoCache).count() == 2
                    query = session.query(VideoCache).first()
                    assert query.ready_for_deletion is True

                    x.remove_video_cache([query.id])
                    assert session.query(VideoCache).count() == 1
