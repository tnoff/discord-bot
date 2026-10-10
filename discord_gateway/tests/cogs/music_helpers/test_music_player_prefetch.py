'''
The bot-side prefetch window: the next queued tracks are downloaded from S3 into
the guild's player directory before the player reaches them, and the staged
copies are removed once they have played or left the queue.
'''
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

import pytest

from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.guild_queue import ClaimedDownload
from discord_core.utils.integrations.s3 import ObjectStorageException

from discord_gateway.cogs.music_helpers.music_player import MusicPlayer

from tests.fakes.asyncio_broker import AsyncioBroker
from tests.fakes.asyncio_broker_client import AsyncioBrokerClient
from tests.fakes.asyncio_queues import make_guild_queue_for
from tests.helpers import fake_context, fake_media_download, FakeVoiceClient #pylint:disable=unused-import

GET_FILE = 'discord_gateway.cogs.music_helpers.music_player.get_file'


def _player(fake_context, tmp_dir, bucket_name='my-bucket', prefetch_limit=2): #pylint:disable=redefined-outer-name
    '''A player over the real guild queue, with the bucket configured on both sides.'''
    engine = AsyncioBroker(bucket_name=bucket_name)
    broker = AsyncioBrokerClient(engine, guild_queue=make_guild_queue_for(engine))
    return MusicPlayer(
        fake_context['bot'], fake_context['guild'], fake_context['channel'], {}, 10, 0.01, Path(tmp_dir),
        broker, 'gw-test', prefetch_limit=prefetch_limit, bucket_name=bucket_name,
    )


async def _enqueue(player, media_download):
    await player.broker.register_download(media_download)
    assert await player.broker.enqueue_track(player.guild.id, str(media_download.media_request.uuid), 10) == 'ok'


def _writes_audio(_bucket, _key, dest):
    Path(dest).write_bytes(b'audio')


async def _settle(player):
    await player._prefetch_task #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_prefetch_stages_only_the_window(fake_context): #pylint:disable=redefined-outer-name
    '''Only the first prefetch_limit queued items are downloaded.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir, prefetch_limit=2)
        items = []
        for _ in range(3):
            cm = fake_media_download(tmp_dir, fake_context=fake_context)
            items.append(cm.__enter__())
            await _enqueue(player, items[-1])
        with patch(GET_FILE, side_effect=_writes_audio) as mock_get:
            player.trigger_prefetch()
            await _settle(player)
        assert mock_get.call_count == 2
        assert player._staged_path(str(items[0].media_request.uuid), items[0].file_path).exists() #pylint:disable=protected-access
        assert not player._staged_path(str(items[2].media_request.uuid), items[2].file_path).exists() #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_prefetch_skips_what_is_already_on_disk(fake_context): #pylint:disable=redefined-outer-name
    '''A second trigger does not download a track twice.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            await _enqueue(player, md)
            with patch(GET_FILE, side_effect=_writes_audio) as mock_get:
                player.trigger_prefetch()
                await _settle(player)
                player.trigger_prefetch()
                await _settle(player)
            assert mock_get.call_count == 1


@pytest.mark.asyncio
async def test_prefetch_is_a_noop_without_a_bucket_or_limit(fake_context): #pylint:disable=redefined-outer-name
    '''No bucket (local files) or a zero limit never starts a download.'''
    with TemporaryDirectory() as tmp_dir:
        for kwargs in ({'bucket_name': None}, {'prefetch_limit': 0}):
            player = _player(fake_context, tmp_dir, **kwargs)
            with fake_media_download(tmp_dir, fake_context=fake_context) as md:
                await _enqueue(player, md)
                with patch(GET_FILE) as mock_get:
                    player.trigger_prefetch()
                assert player._prefetch_task is None #pylint:disable=protected-access
                mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_a_failed_prefetch_is_survivable(fake_context): #pylint:disable=redefined-outer-name
    '''One track failing to download does not stop the rest, and leaves no partial file.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        with fake_media_download(tmp_dir, fake_context=fake_context) as first, \
                fake_media_download(tmp_dir, fake_context=fake_context) as second:
            await _enqueue(player, first)
            await _enqueue(player, second)
            calls = []

            def _first_fails(bucket, key, dest):
                calls.append(key)
                if len(calls) == 1:
                    Path(dest).write_bytes(b'half')
                    raise ObjectStorageException('boom')
                _writes_audio(bucket, key, dest)

            with patch(GET_FILE, side_effect=_first_fails):
                player.trigger_prefetch()
                await _settle(player)
            assert len(calls) == 2
            assert not list(Path(tmp_dir).glob('*.part'))
            assert not player._staged_path(str(first.media_request.uuid), first.file_path).exists() #pylint:disable=protected-access
            assert player._staged_path(str(second.media_request.uuid), second.file_path).exists() #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_playback_reuses_a_prefetched_file(fake_context): #pylint:disable=redefined-outer-name
    '''A track the window already staged is played without a second download,
    and the staged copy is removed once it has played.'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            await _enqueue(player, md)
            staged = player._staged_path(str(md.media_request.uuid), md.file_path) #pylint:disable=protected-access
            with patch(GET_FILE, side_effect=_writes_audio) as mock_get:
                player.trigger_prefetch()
                await _settle(player)
                assert staged.exists()
                await player.player_loop()
            assert mock_get.call_count == 1
            assert not staged.exists()


@pytest.mark.asyncio
async def test_playback_joins_a_download_already_in_flight(fake_context): #pylint:disable=redefined-outer-name
    '''Playback awaits the prefetch's download of the same object instead of repeating it.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        gate = asyncio.Event()
        started = asyncio.Event()
        loop = asyncio.get_running_loop()

        def _slow_get(bucket, key, dest):
            loop.call_soon_threadsafe(started.set)
            asyncio.run_coroutine_threadsafe(gate.wait(), loop).result()
            _writes_audio(bucket, key, dest)

        with patch(GET_FILE, side_effect=_slow_get) as mock_get:
            first = asyncio.create_task(player._ensure_staged('abc', 'my-bucket', 'cache/x.mp3')) #pylint:disable=protected-access
            await started.wait()
            second = asyncio.create_task(player._ensure_staged('abc', 'my-bucket', 'cache/x.mp3')) #pylint:disable=protected-access
            await asyncio.sleep(0)
            gate.set()
            paths = await asyncio.gather(first, second)
        assert mock_get.call_count == 1
        assert paths[0] == paths[1] and paths[0].exists()


@pytest.mark.asyncio
async def test_playback_falls_back_to_the_staged_file_on_a_checkout_miss(fake_context): #pylint:disable=redefined-outer-name
    '''If the broker's claim carries no S3 key but the file is already staged, it still plays.'''
    fake_context['guild'].voice_client = FakeVoiceClient()
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            staged = player._staged_path(str(md.media_request.uuid), md.file_path) #pylint:disable=protected-access
            staged.write_bytes(b'audio')
            player.broker = Mock()
            player.broker.claim_next_track = AsyncMock(
                return_value=ClaimedDownload(download=md, checkout=CheckoutResult(s3_key=None, bucket_name=None)))
            player.broker.finish_track = AsyncMock()
            player.broker.playing_heartbeat = AsyncMock(return_value=True)
            with patch.object(player.logger, 'warning') as warning:
                await player.player_loop()
            assert not any('No playable file' in str(call) for call in warning.call_args_list)
            assert player.current_audio_source is not None
            assert not staged.exists()


def test_discarding_a_track_discards_only_its_staged_file(fake_context): #pylint:disable=redefined-outer-name
    '''A track that leaves the queue unplayed takes its staged copy with it, and no one else's.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        with fake_media_download(tmp_dir, fake_context=fake_context) as first, \
                fake_media_download(tmp_dir, fake_context=fake_context) as second:
            paths = []
            for item in (first, second):
                path = player._staged_path(str(item.media_request.uuid), item.file_path) #pylint:disable=protected-access
                path.write_bytes(b'audio')
                paths.append(path)
            player.discard_staged(first)
            assert not paths[0].exists() and paths[1].exists()
            player.discard_staged(second)
            assert not paths[1].exists()


class _BlockedDownload:
    '''A get_file stand-in that blocks until released, so a test can act while a download is in flight.'''
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.started = asyncio.Event()
        self.gate = asyncio.Event()

    def __call__(self, bucket, key, dest):
        self.loop.call_soon_threadsafe(self.started.set)
        asyncio.run_coroutine_threadsafe(self.gate.wait(), self.loop).result()
        _writes_audio(bucket, key, dest)

    def release(self):
        '''Let the blocked download finish.'''
        self.gate.set()


@pytest.mark.asyncio
async def test_a_new_trigger_supersedes_the_running_prefetch(fake_context): #pylint:disable=redefined-outer-name
    '''Triggering again cancels the previous window task but not its download.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        blocked = _BlockedDownload()
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            await _enqueue(player, md)
            with patch(GET_FILE, side_effect=blocked) as mock_get:
                player.trigger_prefetch()
                first = player._prefetch_task #pylint:disable=protected-access
                await blocked.started.wait()
                player.trigger_prefetch()
                await asyncio.sleep(0)
                assert first.cancelled() or first.cancelling()
                blocked.release()
                await _settle(player)
            assert mock_get.call_count == 1
            assert player._staged_path(str(md.media_request.uuid), md.file_path).exists() #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_discarding_a_track_cancels_its_download(fake_context): #pylint:disable=redefined-outer-name
    '''A track that leaves the queue mid-download stops that download.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        blocked = _BlockedDownload()
        with fake_media_download(tmp_dir, fake_context=fake_context) as md:
            with patch(GET_FILE, side_effect=blocked):
                waiter = asyncio.create_task(
                    player._ensure_staged(str(md.media_request.uuid), 'my-bucket', md.file_path)) #pylint:disable=protected-access
                await blocked.started.wait()
                player._discard_staged(md) #pylint:disable=protected-access
                with pytest.raises(asyncio.CancelledError):
                    await waiter
                blocked.release()
            assert not player._staging #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_cleanup_cancels_downloads_in_flight(fake_context): #pylint:disable=redefined-outer-name
    '''Tearing the player down cancels any download still running.'''
    with TemporaryDirectory() as tmp_dir:
        player = _player(fake_context, tmp_dir)
        blocked = _BlockedDownload()
        with patch(GET_FILE, side_effect=blocked):
            waiter = asyncio.create_task(player._ensure_staged('abc', 'my-bucket', 'cache/x.mp3')) #pylint:disable=protected-access
            await blocked.started.wait()
            await player.cleanup()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            blocked.release()
