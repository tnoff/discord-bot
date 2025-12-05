from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from discord_bot.database import VideoCache, VideoCacheBackup
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from tests.helpers import mock_session, fake_source_dict, fake_media_download, generate_fake_context
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
            with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
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
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as s:
            x.iterate_file(s)
            result = x.get_webpage_url_item(s.media_request)
            assert result.file_path
            assert result.webpage_url == s.media_request.search_string  # pylint: disable=no-member

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

def test_search_existing_file(fake_engine):  #pylint:disable=redefined-outer-name
    test_extractor = 'foo-extractor'
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
        with fake_media_download(tmp_dir, fake_context=fake_context, extractor=test_extractor) as s:
            # Override the id to match our test
            x.iterate_file(s)
            result = x.search_existing_file(test_extractor, s.id) #pylint:disable=no-member
            assert result.base_path == str(s.file_path)
            generated = x.generate_download_from_existing(s.media_request, result)
            assert generated.webpage_url == s.webpage_url  # pylint: disable=no-member

def test_verify_cache_ignore_cleanup_paths_file(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that files in ignore_cleanup_paths are not deleted'''
    with TemporaryDirectory() as tmp_dir:
        # Create files that should be ignored and files that should be deleted
        ignored_file = Path(tmp_dir) / 'keep_this.txt'
        ignored_file.write_text('important data')

        deleted_file = Path(tmp_dir) / 'delete_this.txt'
        deleted_file.write_text('temporary data')

        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['keep_this.txt'])
        x.verify_cache()

        # Ignored file should still exist
        assert ignored_file.exists()
        # Non-ignored file should be deleted
        assert not deleted_file.exists()

def test_verify_cache_ignore_cleanup_paths_directory(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that directories in ignore_cleanup_paths are not deleted'''
    with TemporaryDirectory() as tmp_dir:
        # Create directory that should be ignored
        ignored_dir = Path(tmp_dir) / 'keep_dir'
        ignored_dir.mkdir()
        (ignored_dir / 'file1.txt').write_text('data1')
        (ignored_dir / 'file2.txt').write_text('data2')

        # Create directory that should be deleted
        deleted_dir = Path(tmp_dir) / 'delete_dir'
        deleted_dir.mkdir()
        (deleted_dir / 'file3.txt').write_text('data3')

        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['keep_dir'])
        x.verify_cache()

        # Ignored directory and its contents should still exist
        assert ignored_dir.exists()
        assert (ignored_dir / 'file1.txt').exists()
        assert (ignored_dir / 'file2.txt').exists()

        # Non-ignored directory should be deleted
        assert not deleted_dir.exists()

def test_verify_cache_ignore_cleanup_paths_nested(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that nested paths within ignored directories are not deleted'''
    with TemporaryDirectory() as tmp_dir:
        # Create nested structure in ignored directory
        ignored_dir = Path(tmp_dir) / 'important'
        ignored_dir.mkdir()
        nested_dir = ignored_dir / 'nested' / 'deep'
        nested_dir.mkdir(parents=True)
        (nested_dir / 'file.txt').write_text('nested data')

        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['important'])
        x.verify_cache()

        # Entire ignored directory tree should still exist
        assert ignored_dir.exists()
        assert nested_dir.exists()
        assert (nested_dir / 'file.txt').exists()

def test_verify_cache_ignore_cleanup_paths_multiple(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that multiple ignore paths work correctly'''
    with TemporaryDirectory() as tmp_dir:
        # Create multiple files/dirs to ignore
        file1 = Path(tmp_dir) / 'keep1.txt'
        file1.write_text('data1')

        dir1 = Path(tmp_dir) / 'keep_dir1'
        dir1.mkdir()

        file2 = Path(tmp_dir) / 'keep2.txt'
        file2.write_text('data2')

        # Create file that should be deleted
        deleted_file = Path(tmp_dir) / 'delete.txt'
        deleted_file.write_text('temp')

        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['keep1.txt', 'keep_dir1', 'keep2.txt'])
        x.verify_cache()

        # All ignored items should exist
        assert file1.exists()
        assert dir1.exists()
        assert file2.exists()

        # Non-ignored file should be deleted
        assert not deleted_file.exists()

def test_verify_cache_ignore_cleanup_paths_with_cached_files(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that ignored paths work correctly alongside cached files'''
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()

        # Create an ignored file
        ignored_file = Path(tmp_dir) / 'important.txt'
        ignored_file.write_text('keep this')

        # Create a cached file
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['important.txt'])
        with fake_media_download(tmp_dir, fake_context=fake_context) as s:
            x.iterate_file(s)
            x.verify_cache()

            # Both cached file and ignored file should exist
            assert Path(s.file_path).exists()
            assert ignored_file.exists()

def test_should_ignore_path_file(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test _should_ignore_path method with files'''
    with TemporaryDirectory() as tmp_dir:
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['ignore_me.txt', 'subdir/file.txt'])

        # Test exact match
        assert x._should_ignore_path(Path(tmp_dir) / 'ignore_me.txt') is True  #pylint:disable=protected-access

        # Test non-match
        assert x._should_ignore_path(Path(tmp_dir) / 'delete_me.txt') is False  #pylint:disable=protected-access

        # Test nested path
        assert x._should_ignore_path(Path(tmp_dir) / 'subdir' / 'file.txt') is True  #pylint:disable=protected-access

def test_should_ignore_path_directory(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test _should_ignore_path method with directories'''
    with TemporaryDirectory() as tmp_dir:
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['important_dir'])

        # Test directory match
        assert x._should_ignore_path(Path(tmp_dir) / 'important_dir') is True  #pylint:disable=protected-access

        # Test file inside ignored directory
        assert x._should_ignore_path(Path(tmp_dir) / 'important_dir' / 'file.txt') is True  #pylint:disable=protected-access

        # Test nested directory inside ignored directory
        assert x._should_ignore_path(Path(tmp_dir) / 'important_dir' / 'subdir' / 'file.txt') is True  #pylint:disable=protected-access

        # Test different directory
        assert x._should_ignore_path(Path(tmp_dir) / 'other_dir') is False  #pylint:disable=protected-access
