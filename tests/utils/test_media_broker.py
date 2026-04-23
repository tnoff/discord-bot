from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

import pytest

from discord_bot.interfaces.broker_protocols import Zone
from discord_bot.workers.asyncio_broker import AsyncioBroker as MediaBroker

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

@pytest.mark.asyncio
async def test_register_request_creates_in_flight_entry():
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    key = str(mr.uuid)
    entry = await broker.get_entry(key)
    assert entry is not None
    assert entry.zone == Zone.IN_FLIGHT
    assert entry.request is mr
    assert entry.download is None


@pytest.mark.asyncio
async def test_register_request_idempotent():
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    await broker.register_request(mr)
    assert len(broker) == 1


@pytest.mark.asyncio
async def test_register_download_transitions_to_available():
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            entry = await broker.get_entry(str(mr.uuid))
            assert entry is not None
            assert entry.zone == Zone.AVAILABLE
            assert entry.download is md


@pytest.mark.asyncio
async def test_register_download_without_prior_request_creates_available_entry():
    # Cache-hit path: download arrives without a prior register_request call
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            key = str(mr.uuid)
            entry = await broker.get_entry(key)
            assert entry is not None
            assert entry.zone == Zone.AVAILABLE
            assert entry.download is md


# ---------------------------------------------------------------------------
# Checkout / remove
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_checkout_transitions_to_checked_out():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            await broker.checkout(str(mr.uuid), guild_id)
            entry = await broker.get_entry(str(mr.uuid))
            assert entry is not None
            assert entry.zone == Zone.CHECKED_OUT
            assert entry.checked_out_by == guild_id


@pytest.mark.asyncio
async def test_checkout_with_guild_path_copies_file():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as base_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(base_dir, media_request=mr) as md:
                original_file_path = md.file_path
                await broker.register_download(md)
                guild_file_path = await broker.checkout(str(mr.uuid), guild_id, Path(guild_dir))
                # guild_file_path should be in the guild directory
                assert guild_file_path is not None
                assert guild_file_path.parent == Path(guild_dir)
                assert guild_file_path.exists()
                # file_path (base) is unchanged
                assert md.file_path == original_file_path


@pytest.mark.asyncio
async def test_checkout_without_guild_path_does_not_copy():
    broker = MediaBroker()
    mr = _make_request()
    guild_id = mr.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            original_path = md.file_path
            await broker.register_download(md)
            await broker.checkout(str(mr.uuid), guild_id)
            # file_path unchanged when no guild_path given
            assert md.file_path == original_path


@pytest.mark.asyncio
async def test_checkout_missing_entry_is_noop():
    broker = MediaBroker()
    result = await broker.checkout('nonexistent-uuid', 123)
    assert result is None


@pytest.mark.asyncio
async def test_remove_deletes_entry():
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    await broker.remove(str(mr.uuid))
    assert await broker.get_entry(str(mr.uuid)) is None


@pytest.mark.asyncio
async def test_remove_missing_entry_is_noop():
    broker = MediaBroker()
    await broker.remove('nonexistent-uuid')  # should not raise


# ---------------------------------------------------------------------------
# can_evict_request
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_can_evict_request_not_present_returns_true():
    broker = MediaBroker()
    assert await broker.can_evict_request('nonexistent-uuid') is True


@pytest.mark.asyncio
async def test_can_evict_request_in_flight_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    assert await broker.can_evict_request(str(mr.uuid)) is False


@pytest.mark.asyncio
async def test_can_evict_request_available_returns_true():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            assert await broker.can_evict_request(str(mr.uuid)) is True


@pytest.mark.asyncio
async def test_can_evict_request_checked_out_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            await broker.checkout(str(mr.uuid), mr.guild_id)
            assert await broker.can_evict_request(str(mr.uuid)) is False


# ---------------------------------------------------------------------------
# can_evict_base
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_can_evict_base_no_entries_returns_true():
    broker = MediaBroker()
    assert await broker.can_evict_base('https://example.com/video') is True


@pytest.mark.asyncio
async def test_can_evict_base_in_flight_only_returns_true():
    # IN_FLIGHT means no file on disk yet, base can be evicted
    broker = MediaBroker()
    mr = _make_request()
    await broker.register_request(mr)
    assert await broker.can_evict_base('https://example.com/video') is True


@pytest.mark.asyncio
async def test_can_evict_base_available_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            assert await broker.can_evict_base(md.webpage_url) is False


@pytest.mark.asyncio
async def test_can_evict_base_checked_out_returns_false():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            await broker.checkout(str(mr.uuid), mr.guild_id)
            assert await broker.can_evict_base(md.webpage_url) is False


@pytest.mark.asyncio
async def test_can_evict_base_shared_url_one_available_blocks_eviction():
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
                await broker.register_download(md1)
                await broker.register_download(md2)
                # Checkout and finish the first one
                await broker.checkout(str(mr1.uuid), mr1.guild_id)
                await broker.remove(str(mr1.uuid))
                # Second is still AVAILABLE — base must not be evicted
                assert await broker.can_evict_base(shared_url) is False


@pytest.mark.asyncio
async def test_can_evict_base_after_all_removed_returns_true():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            await broker.checkout(str(mr.uuid), mr.guild_id)
            await broker.remove(str(mr.uuid))
            assert await broker.can_evict_base(md.webpage_url) is True


# ---------------------------------------------------------------------------
# get_checked_out_by
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_checked_out_by_returns_entries_for_guild():
    broker = MediaBroker()
    fake_context = generate_fake_context()
    mr1 = fake_source_dict(fake_context)
    mr2 = fake_source_dict(fake_context)
    guild_id = mr1.guild_id
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr1) as md1:
            with fake_media_download(tmp_dir, media_request=mr2) as md2:
                await broker.register_download(md1)
                await broker.register_download(md2)
                await broker.checkout(str(mr1.uuid), guild_id)
                # Only first one is checked out
                results = await broker.get_checked_out_by(guild_id)
                assert len(results) == 1
                assert results[0].request is mr1


@pytest.mark.asyncio
async def test_get_checked_out_by_empty_when_none_checked_out():
    broker = MediaBroker()
    mr = _make_request()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, media_request=mr) as md:
            await broker.register_download(md)
            assert await broker.get_checked_out_by(mr.guild_id) == []


# ---------------------------------------------------------------------------
# Full lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_lifecycle():
    broker = MediaBroker()
    mr = _make_request()
    uuid = str(mr.uuid)

    # Step 1: request registered, IN_FLIGHT
    await broker.register_request(mr)
    assert (await broker.get_entry(uuid)).zone == Zone.IN_FLIGHT
    assert not await broker.can_evict_request(uuid)

    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory() as guild_dir:
            with fake_media_download(tmp_dir, media_request=mr) as md:
                url = md.webpage_url

                # Step 2: download completes, AVAILABLE
                await broker.register_download(md)
                assert (await broker.get_entry(uuid)).zone == Zone.AVAILABLE
                assert await broker.can_evict_request(uuid)
                assert not await broker.can_evict_base(url)

                # Step 3: player dequeues, broker copies to guild dir, CHECKED_OUT
                guild_file_path = await broker.checkout(uuid, mr.guild_id, Path(guild_dir))
                assert (await broker.get_entry(uuid)).zone == Zone.CHECKED_OUT
                assert not await broker.can_evict_request(uuid)
                assert not await broker.can_evict_base(url)
                assert guild_file_path is not None
                assert guild_file_path.parent == Path(guild_dir)
                # base file_path unchanged
                assert md.file_path.parent == Path(tmp_dir)

                # Step 4: player finishes, entry removed
                await broker.remove(uuid)
                assert await broker.get_entry(uuid) is None
                assert await broker.can_evict_request(uuid)
                assert await broker.can_evict_base(url)


# ---------------------------------------------------------------------------
# get_cache_count
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_cache_count_no_video_cache_returns_zero():
    broker = MediaBroker()
    assert broker.video_cache is None
    assert await broker.get_cache_count() == 0


@pytest.mark.asyncio
async def test_get_cache_count_delegates_to_video_cache():
    broker = MediaBroker()
    mock_cache = AsyncMock()
    mock_cache.get_cache_count.return_value = 42
    broker.video_cache = mock_cache
    assert await broker.get_cache_count() == 42
    mock_cache.get_cache_count.assert_awaited_once()
