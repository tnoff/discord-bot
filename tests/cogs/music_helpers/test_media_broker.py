from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from discord_bot.cogs.music_helpers.media_broker import MediaBroker, Zone
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.database import VideoCache
from discord_bot.types.media_download import MediaDownload

from tests.helpers import (
    mock_session, fake_media_download, generate_fake_context, fake_source_dict,
)
from tests.helpers import fake_engine  # pylint:disable=unused-import


# ---------------------------------------------------------------------------
# Local transient mode (no bucket_name, no video_cache)
# ---------------------------------------------------------------------------

def test_register_request_idempotent():
    fake_context = generate_fake_context()
    mr = fake_source_dict(fake_context)
    broker = MediaBroker()
    broker.register_request(mr)
    broker.register_request(mr)
    assert len(broker) == 1


def test_register_download_local_mode():
    '''register_download in local mode keeps the file on disk, no upload'''
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            original_path = md.file_path
            broker = MediaBroker(file_dir=Path(tmp_dir))
            broker.register_download(md)
            assert md.file_path == original_path
            assert md.file_path.exists()
            entry = broker.get_entry(str(md.media_request.uuid))
            assert entry is not None
            assert entry.zone == Zone.AVAILABLE


def test_checkout_local_mode():
    '''checkout copies base file into guild dir'''
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(tmp_dir, fake_context=fake_context) as md:
                broker = MediaBroker(file_dir=Path(tmp_dir))
                broker.register_download(md)
                result = broker.checkout(str(md.media_request.uuid), 123, guild_path=Path(guild_dir))
                assert result is not None
                assert result.exists()
                entry = broker.get_entry(str(md.media_request.uuid))
                assert entry.zone == Zone.CHECKED_OUT


def test_checkout_missing_local_file_raises():
    '''checkout raises FileNotFoundError when base file is gone in local mode'''
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(tmp_dir, fake_context=fake_context) as md:
                broker = MediaBroker(file_dir=Path(tmp_dir))
                broker.register_download(md)
                md.file_path.unlink()
                with pytest.raises(FileNotFoundError):
                    broker.checkout(str(md.media_request.uuid), 123, guild_path=Path(guild_dir))


def test_discard_local_mode_deletes_file():
    '''discard in local mode (no cache) always deletes the staging file'''
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            broker = MediaBroker(file_dir=Path(tmp_dir))
            broker.register_download(md)
            local_path = md.file_path
            broker.discard(str(md.media_request.uuid))
            assert not local_path.exists()


def test_cache_cleanup_no_video_cache_is_noop():
    '''cache_cleanup returns False immediately when no video_cache is set'''
    broker = MediaBroker()
    assert broker.cache_cleanup() is False


def test_release_deletes_guild_file():
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(tmp_dir, fake_context=fake_context) as md:
                broker = MediaBroker(file_dir=Path(tmp_dir))
                broker.register_download(md)
                guild_path = broker.checkout(str(md.media_request.uuid), 123, guild_path=Path(guild_dir))
                assert guild_path.exists()
                broker.release(str(md.media_request.uuid))
                assert not guild_path.exists()
                assert broker.get_entry(str(md.media_request.uuid)) is None


# ---------------------------------------------------------------------------
# S3 cache mode — DownloadClient has already uploaded; file_path is an S3 key
# ---------------------------------------------------------------------------

def _make_s3_media_download(fake_context) -> MediaDownload:
    '''
    Simulate what DownloadClient produces in S3 mode: a MediaDownload whose
    file_path is already an S3 object key (no local file).
    '''
    mr = fake_source_dict(fake_context)
    return MediaDownload(Path(f'cache/{mr.uuid}.mp3'), {
        'duration': 120,
        'webpage_url': f'https://example.com/s3-test/{mr.uuid}',
        'title': 'S3 Test',
        'id': 'abc123',
        'uploader': 'tester',
        'extractor': 'youtube',
    }, mr)


def test_register_download_s3_mode():
    '''register_download with an S3 key file_path stores it as-is in AVAILABLE state'''
    fake_context = generate_fake_context()
    md = _make_s3_media_download(fake_context)
    broker = MediaBroker(bucket_name='my-bucket')
    broker.register_download(md)
    assert str(md.file_path).startswith('cache/')
    entry = broker.get_entry(str(md.media_request.uuid))
    assert entry.zone == Zone.AVAILABLE


def test_checkout_s3_mode(mocker):
    '''checkout in S3 mode calls get_file with the S3 key'''
    get_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
    fake_context = generate_fake_context()
    md = _make_s3_media_download(fake_context)
    with TemporaryDirectory() as guild_dir:
        broker = MediaBroker(bucket_name='my-bucket')
        broker.register_download(md)
        result = broker.checkout(str(md.media_request.uuid), 123, guild_path=Path(guild_dir))
        get_mock.assert_called_once()
        assert get_mock.call_args[0][0] == 'my-bucket'
        assert str(get_mock.call_args[0][1]).startswith('cache/')
        assert result is not None


def test_discard_s3_mode_no_cache_deletes_s3_object(mocker):
    '''discard in S3 mode (no video_cache) calls delete_file on the S3 key'''
    delete_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.delete_file', return_value=True)
    fake_context = generate_fake_context()
    md = _make_s3_media_download(fake_context)
    s3_key = str(md.file_path)
    broker = MediaBroker(bucket_name='my-bucket')
    broker.register_download(md)
    broker.discard(str(md.media_request.uuid))
    delete_mock.assert_called_once_with('my-bucket', s3_key)


def test_cache_cleanup_s3_mode(mocker, fake_engine):  #pylint:disable=redefined-outer-name
    '''cache_cleanup in S3 mode deletes S3 objects and removes DB records'''
    delete_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.delete_file', return_value=True)
    fake_context = generate_fake_context()
    vc = VideoCacheClient(1, partial(mock_session, fake_engine))
    broker = MediaBroker(video_cache=vc, bucket_name='my-bucket')
    md1 = _make_s3_media_download(fake_context)
    md2 = _make_s3_media_download(generate_fake_context())
    # Simulate post-upload state: both already in S3, iterate_file creates DB records
    vc.iterate_file(md1)
    vc.iterate_file(md2)
    # Neither is in broker registry → both evictable
    result = broker.cache_cleanup()
    assert result is True
    delete_mock.assert_called_once()
    assert delete_mock.call_args[0][0] == 'my-bucket'
    with mock_session(fake_engine) as session:
        assert session.query(VideoCache).count() == 1


# ---------------------------------------------------------------------------
# Eviction queries
# ---------------------------------------------------------------------------

def test_can_evict_base_not_evictable_while_checked_out():
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            broker = MediaBroker(file_dir=Path(tmp_dir))
            broker.register_download(md)
            broker.checkout(str(md.media_request.uuid), 123)
            assert not broker.can_evict_base(md.webpage_url)


def test_can_evict_base_evictable_when_not_registered():
    broker = MediaBroker()
    assert broker.can_evict_base('https://example.com/not-tracked')


def test_get_checked_out_by():
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            broker = MediaBroker(file_dir=Path(tmp_dir))
            broker.register_download(md)
            broker.checkout(str(md.media_request.uuid), guild_id=42)
            entries = broker.get_checked_out_by(42)
            assert len(entries) == 1
            assert broker.get_checked_out_by(99) == []


# ---------------------------------------------------------------------------
# checkout guard — skip re-staging when already CHECKED_OUT
# ---------------------------------------------------------------------------

def test_checkout_skips_restage_if_already_checked_out(mocker):
    '''checkout returns existing guild_file_path without calling get_file again if already staged'''
    get_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
    fake_context = generate_fake_context()
    md = _make_s3_media_download(fake_context)
    with TemporaryDirectory() as guild_dir:
        broker = MediaBroker(bucket_name='my-bucket')
        broker.register_download(md)
        # First checkout — stages the file
        guild_path = Path(guild_dir)
        result1 = broker.checkout(str(md.media_request.uuid), 123, guild_path=guild_path)
        assert get_mock.call_count == 1
        # Manually create the staged file so exists() returns True
        result1.touch()
        # Second checkout — should skip re-staging
        result2 = broker.checkout(str(md.media_request.uuid), 123, guild_path=guild_path)
        assert get_mock.call_count == 1  # not called again
        assert result2 == result1


# ---------------------------------------------------------------------------
# prefetch
# ---------------------------------------------------------------------------

def test_prefetch_noop_in_local_mode():
    '''prefetch is a no-op when bucket_name is not set (local mode)'''
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            broker = MediaBroker(file_dir=Path(tmp_dir))
            broker.register_download(md)
            # Should not raise and should not change zone
            broker.prefetch([md], 123, Path(tmp_dir), limit=5)
            entry = broker.get_entry(str(md.media_request.uuid))
            assert entry.zone == Zone.AVAILABLE


def test_prefetch_stages_available_items(mocker):
    '''prefetch calls checkout for AVAILABLE items up to limit'''
    get_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
    fake_context = generate_fake_context()
    md1 = _make_s3_media_download(fake_context)
    md2 = _make_s3_media_download(generate_fake_context())
    md3 = _make_s3_media_download(generate_fake_context())
    with TemporaryDirectory() as guild_dir:
        broker = MediaBroker(bucket_name='my-bucket')
        broker.register_download(md1)
        broker.register_download(md2)
        broker.register_download(md3)
        broker.prefetch([md1, md2, md3], 123, Path(guild_dir), limit=2)
        # Only 2 of the 3 items should have been staged
        assert get_mock.call_count == 2
        assert broker.get_entry(str(md1.media_request.uuid)).zone == Zone.CHECKED_OUT
        assert broker.get_entry(str(md2.media_request.uuid)).zone == Zone.CHECKED_OUT
        assert broker.get_entry(str(md3.media_request.uuid)).zone == Zone.AVAILABLE


def test_prefetch_skips_already_checked_out(mocker):
    '''prefetch counts CHECKED_OUT items toward the limit without re-staging'''
    get_mock = mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
    fake_context = generate_fake_context()
    md1 = _make_s3_media_download(fake_context)
    md2 = _make_s3_media_download(generate_fake_context())
    with TemporaryDirectory() as guild_dir:
        broker = MediaBroker(bucket_name='my-bucket')
        broker.register_download(md1)
        broker.register_download(md2)
        # Manually put md1 into CHECKED_OUT without staging a file
        broker.checkout(str(md1.media_request.uuid), 123, guild_path=Path(guild_dir))
        assert get_mock.call_count == 1
        # prefetch with limit=1 — md1 already checked out fills the slot
        broker.prefetch([md1, md2], 123, Path(guild_dir), limit=1)
        assert get_mock.call_count == 1  # md2 not staged
        assert broker.get_entry(str(md2.media_request.uuid)).zone == Zone.AVAILABLE
