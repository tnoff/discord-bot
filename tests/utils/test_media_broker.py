from pathlib import Path
from tempfile import TemporaryDirectory

from discord_bot.cogs.music_helpers.media_broker import MediaBroker, Zone

from tests.helpers import fake_media_download, fake_source_dict, generate_fake_context

# Tests inspect MediaBroker internals directly; suppress the blanket warning.
# pylint: disable=protected-access


def _make_request():
    fake_context = generate_fake_context()
    return fake_source_dict(fake_context)


# ---------------------------------------------------------------------------
# MediaDownload.uuid
# ---------------------------------------------------------------------------

def test_media_download_uuid_is_set():
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            assert md.uuid
            assert isinstance(md.uuid, str)


def test_media_download_uuid_unique():
    fake_context = generate_fake_context()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as md1:
            with fake_media_download(tmp_dir, fake_context=fake_context) as md2:
                assert md1.uuid != md2.uuid


# ---------------------------------------------------------------------------
# MediaBroker registration
# ---------------------------------------------------------------------------

def test_register_request_creates_in_flight_entry():
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    key = str(mr.uuid)
    entry = broker.get_entry(key)
    assert entry is not None
    assert entry.zone == Zone.IN_FLIGHT
    assert entry.request is mr
    assert entry.download is None


def test_register_request_idempotent():
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    broker.register_request(mr)
    assert len(broker) == 1


def test_register_download_transitions_to_available():
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            entry = broker.get_entry(str(mr.uuid))
            assert entry is not None
            assert entry.zone == Zone.AVAILABLE
            assert entry.download is md


def test_register_download_without_prior_request_creates_available_entry():
    # Cache-hit path: download arrives without a prior register_request call
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            key = str(mr.uuid)
            entry = broker.get_entry(key)
            assert entry is not None
            assert entry.zone == Zone.AVAILABLE
            assert entry.download is md


# ---------------------------------------------------------------------------
# Checkout / remove
# ---------------------------------------------------------------------------

def test_checkout_transitions_to_checked_out():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            broker.checkout(str(mr.uuid), guild_id)
            entry = broker.get_entry(str(mr.uuid))
            assert entry is not None
            assert entry.zone == Zone.CHECKED_OUT
            assert entry.checked_out_by == guild_id


def test_checkout_with_guild_path_copies_file():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as base_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(base_dir, media_request=mr) as md:
                original_base = md.base_path
                broker.register_download(md)
                broker.checkout(str(mr.uuid), guild_id, Path(guild_dir))
                # file_path should have been updated to guild copy
                assert md.file_path != original_base
                assert md.file_path.parent == Path(guild_dir)
                assert md.file_path.exists()
                # base_path still points to original
                assert md.base_path == original_base


def test_checkout_without_guild_path_does_not_copy():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            original_path = md.file_path
            broker.register_download(md)
            broker.checkout(str(mr.uuid), guild_id)
            # file_path unchanged when no guild_path given
            assert md.file_path == original_path


def test_checkout_missing_entry_is_noop():
    broker = MediaBroker()
    broker.checkout('nonexistent-uuid', 123)  # should not raise


def test_remove_deletes_entry():
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    broker.remove(str(mr.uuid))
    assert broker.get_entry(str(mr.uuid)) is None


def test_remove_missing_entry_is_noop():
    broker = MediaBroker()
    broker.remove('nonexistent-uuid')  # should not raise


# ---------------------------------------------------------------------------
# can_evict_request
# ---------------------------------------------------------------------------

def test_can_evict_request_not_present_returns_true():
    broker = MediaBroker()
    assert broker.can_evict_request('nonexistent-uuid') is True


def test_can_evict_request_in_flight_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    assert broker.can_evict_request(str(mr.uuid)) is False


def test_can_evict_request_available_returns_true():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            assert broker.can_evict_request(str(mr.uuid)) is True


def test_can_evict_request_checked_out_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            broker.checkout(str(mr.uuid), mr.guild_id)
            assert broker.can_evict_request(str(mr.uuid)) is False


# ---------------------------------------------------------------------------
# can_evict_base
# ---------------------------------------------------------------------------

def test_can_evict_base_no_entries_returns_true():
    broker = MediaBroker()
    assert broker.can_evict_base('https://example.com/video') is True


def test_can_evict_base_in_flight_only_returns_true():
    # IN_FLIGHT means no file on disk yet, base can be evicted
    broker = MediaBroker()
    mr = _make_request()
    broker.register_request(mr)
    assert broker.can_evict_base('https://example.com/video') is True


def test_can_evict_base_available_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            assert broker.can_evict_base(md.webpage_url) is False


def test_can_evict_base_checked_out_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            broker.checkout(str(mr.uuid), mr.guild_id)
            assert broker.can_evict_base(md.webpage_url) is False


def test_can_evict_base_shared_url_one_available_blocks_eviction():
    # Two requests for the same URL share the same base file.
    # As long as one is still AVAILABLE, the base file must not be evicted.
    broker = MediaBroker()
    fake_context = generate_fake_context()
    mr1 = fake_source_dict(fake_context)
    mr2 = fake_source_dict(fake_context)
    shared_url = 'https://example.com/shared-video'
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr1) as md1:
            md1.webpage_url = shared_url
            with fake_media_download(tmp_dir, media_request=mr2) as md2:
                md2.webpage_url = shared_url
                broker.register_download(md1)
                broker.register_download(md2)
                # Checkout and finish the first one
                broker.checkout(str(mr1.uuid), mr1.guild_id)
                broker.remove(str(mr1.uuid))
                # Second is still AVAILABLE — base must not be evicted
                assert broker.can_evict_base(shared_url) is False


def test_can_evict_base_after_all_removed_returns_true():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            broker.checkout(str(mr.uuid), mr.guild_id)
            broker.remove(str(mr.uuid))
            assert broker.can_evict_base(md.webpage_url) is True


# ---------------------------------------------------------------------------
# get_checked_out_by
# ---------------------------------------------------------------------------

def test_get_checked_out_by_returns_entries_for_guild():
    broker = MediaBroker()
    fake_context = generate_fake_context()
    mr1 = fake_source_dict(fake_context)
    mr2 = fake_source_dict(fake_context)
    guild_id = mr1.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr1) as md1:
            with fake_media_download(tmp_dir, media_request=mr2) as md2:
                broker.register_download(md1)
                broker.register_download(md2)
                broker.checkout(str(mr1.uuid), guild_id)
                # Only first one is checked out
                results = broker.get_checked_out_by(guild_id)
                assert len(results) == 1
                assert results[0].request is mr1


def test_get_checked_out_by_empty_when_none_checked_out():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            broker.register_download(md)
            assert broker.get_checked_out_by(mr.guild_id) == []


# ---------------------------------------------------------------------------
# Full lifecycle
# ---------------------------------------------------------------------------

def test_full_lifecycle():
    broker = MediaBroker()
    mr = _make_request()
    uuid = str(mr.uuid)

    # Step 1: request registered, IN_FLIGHT
    broker.register_request(mr)
    assert broker.get_entry(uuid).zone == Zone.IN_FLIGHT
    assert not broker.can_evict_request(uuid)

    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                url = md.webpage_url

                # Step 2: download completes, AVAILABLE
                broker.register_download(md)
                assert broker.get_entry(uuid).zone == Zone.AVAILABLE
                assert broker.can_evict_request(uuid)
                assert not broker.can_evict_base(url)

                # Step 3: player dequeues, broker copies to guild dir, CHECKED_OUT
                broker.checkout(uuid, mr.guild_id, Path(guild_dir))
                assert broker.get_entry(uuid).zone == Zone.CHECKED_OUT
                assert not broker.can_evict_request(uuid)
                assert not broker.can_evict_base(url)
                assert md.file_path.parent == Path(guild_dir)

                # Step 4: player finishes, entry removed
                broker.remove(uuid)
                assert broker.get_entry(uuid) is None
                assert broker.can_evict_request(uuid)
                assert broker.can_evict_base(url)
