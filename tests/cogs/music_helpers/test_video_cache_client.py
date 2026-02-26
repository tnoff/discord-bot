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
                # Directories are skipped, not deleted
                assert Path(tmp_dir2).exists()

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
            assert result.webpage_url == s.media_request.search_result.resolved_search_string

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
    '''Test that files with exact path matches in ignore_cleanup_paths are not deleted'''
    with TemporaryDirectory() as tmp_dir:
        # Create files that should be ignored (exact path match)
        kept_file1 = Path(tmp_dir) / 'keep_file1.txt'
        kept_file1.write_text('data1')
        kept_file2 = Path(tmp_dir) / 'keep_file2.txt'
        kept_file2.write_text('data2')

        # Create file that should be deleted
        deleted_file = Path(tmp_dir) / 'delete_file.txt'
        deleted_file.write_text('data3')

        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['keep_file1.txt', 'keep_file2.txt'])
        x.verify_cache()

        # Ignored files should still exist
        assert kept_file1.exists()
        assert kept_file2.exists()

        # Non-ignored file should be deleted
        assert not deleted_file.exists()

def test_verify_cache_ignore_cleanup_paths_nested(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that files with exact nested paths in ignore_cleanup_paths are not deleted'''
    with TemporaryDirectory() as tmp_dir:
        # Create nested structure
        nested_dir = Path(tmp_dir) / 'important' / 'nested' / 'deep'
        nested_dir.mkdir(parents=True)
        kept_file = nested_dir / 'file.txt'
        kept_file.write_text('nested data')

        deleted_file = Path(tmp_dir) / 'delete_me.txt'
        deleted_file.write_text('delete this')

        # Use exact path to protect the nested file
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['important/nested/deep/file.txt'])
        x.verify_cache()

        # Exact path match should be protected
        assert kept_file.exists()
        # Other file should be deleted
        assert not deleted_file.exists()

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
    '''Test _should_ignore_path method with exact path matching'''
    with TemporaryDirectory() as tmp_dir:
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['important_dir', 'keep_file.txt'])

        # Test exact directory match
        assert x._should_ignore_path(Path(tmp_dir) / 'important_dir') is True  #pylint:disable=protected-access

        # Test exact file match
        assert x._should_ignore_path(Path(tmp_dir) / 'keep_file.txt') is True  #pylint:disable=protected-access

        # Test non-matching paths (no parent directory matching)
        assert x._should_ignore_path(Path(tmp_dir) / 'important_dir' / 'file.txt') is False  #pylint:disable=protected-access
        assert x._should_ignore_path(Path(tmp_dir) / 'other_dir') is False  #pylint:disable=protected-access


def test_verify_cache_ignore_file_in_directory(fake_engine):  #pylint:disable=redefined-outer-name
    '''Test that ignoring a file with exact path match works for nested files'''
    with TemporaryDirectory() as tmp_dir:
        # Create a directory with a file inside (like db/discord.sql)
        db_dir = Path(tmp_dir) / 'db'
        db_dir.mkdir()
        db_file = db_dir / 'discord.sql'
        db_file.write_text('database content')

        # Create another file that should be deleted
        extra_file = Path(tmp_dir) / 'extra.txt'
        extra_file.write_text('should be deleted')

        # Ignore the database file with exact path
        x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None,
                           ignore_cleanup_paths=['db/discord.sql'])
        x.verify_cache()

        # The ignored file should still exist (exact match)
        assert db_file.exists(), 'db/discord.sql should not be deleted'
        # Directory is skipped (not deleted)
        assert db_dir.exists(), 'db directory is skipped'
        # Extra file should be deleted
        assert not extra_file.exists(), 'extra.txt should be deleted'
