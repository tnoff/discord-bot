from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from discord_bot.database import VideoCache, VideoCacheBackup
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from tests.helpers import mock_session, fake_source_dict, fake_source_download, generate_fake_context
from tests.helpers import fake_engine #pylint:disable=unused-import

def test_verify_cache(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_file:
            x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
            x.verify_cache()

            assert not Path(tmp_file.name).exists()

def test_verify_cache_with_dir(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory(ignore_cleanup_errors=True, dir=tmp_dir) as tmp_dir2:
            with NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_file:
                x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
                x.verify_cache()

                assert not Path(tmp_file.name).exists()
                assert not Path(tmp_dir2).exists()

def test_verify_cache_with_files_that_no_longer_exist(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.mp3', dir=tmp_dir, delete=False) as extra_file:
            fake_context = generate_fake_context()
            x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
            with fake_source_download(tmp_dir, fake_context=fake_context) as s:
                x.iterate_file(s)
                Path(s.file_path).unlink()
                x.verify_cache()
                with mock_session(fake_engine) as session:
                    assert session.query(VideoCache).count() == 0
                    assert not Path(extra_file.name).exists()

def test_verify_cache_with_files_that_no_longer_exist_redownload_with_s3(mocker, fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
        mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.get_file', return_value=True)
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), 's3', 'foo')
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            x.iterate_file(s)
            x.object_storage_backup(1)
            Path(s.file_path).unlink()
            x.verify_cache()
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1

def test_iterate_file_new_and_iterate(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            x.iterate_file(s)
            x.iterate_file(s)
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                query = session.query(VideoCache).first()
                assert query.count == 2

def test_object_storage_backup(mocker, fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), 's3', 'foo')
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
            x.iterate_file(s)
            x.object_storage_backup(1)
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                assert session.query(VideoCacheBackup).count() == 1

            # Make sure if we call again another one doesn't get created
            x.object_storage_backup(1)
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                assert session.query(VideoCacheBackup).count() == 1

def test_object_storage_backup_no_file_is_noop(mocker, fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), 's3', 'foo')
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
            x.iterate_file(s)
            # Currently this cant happen naturally
            with mock_session(fake_engine) as session:
                query = session.query(VideoCache).first()
                query.base_path = None
                session.commit()
            x.object_storage_backup(1)
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                assert session.query(VideoCacheBackup).count() == 0

def test_object_storage_backup_remove(mocker, fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), 's3', 'foo')
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.delete_file', return_value=True)
            x.iterate_file(s)
            x.object_storage_backup(1)
            x.remove_video_cache([1])
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 0
                assert session.query(VideoCacheBackup).count() == 0

def test_webpage_get_source(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            x.iterate_file(s)
            result = x.get_webpage_url_item(s.source_dict)
            assert result.file_path
            assert result.webpage_url == s.source_dict.search_string

def test_webpage_get_source_non_existing(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
        sd = fake_source_dict(fake_context, is_direct_search=True)
        result = x.get_webpage_url_item(sd)
        assert result is None

def test_remove(fake_engine):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 1, partial(mock_session, fake_engine), None, None)
        with fake_source_download(tmp_dir, fake_context=fake_context) as s:
            with fake_source_download(tmp_dir, fake_context=fake_context) as t2:
                x.iterate_file(s)
                x.iterate_file(t2)
                x.ready_remove()

                with mock_session(fake_engine) as session:
                    assert session.query(VideoCache).count() == 2
                    query = session.query(VideoCache).first()
                    assert query.ready_for_deletion is True

                    x.remove_video_cache([query.id])
                    assert session.query(VideoCache).count() == 1

def test_search_existing_file(fake_engine):  #pylint:disable=redefined-outer-name
    test_extractor = 'foo-extractor'
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
        with fake_source_download(tmp_dir, fake_context=fake_context, extractor=test_extractor) as s:
            # Override the id to match our test
            x.iterate_file(s)
            result = x.search_existing_file(test_extractor, s.id) #pylint:disable=no-member
            assert result.base_path == str(s.file_path)
            generated = x.generate_download_from_existing(s.source_dict, result)
            assert generated.webpage_url == s.webpage_url
