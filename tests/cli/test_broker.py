'''Tests for the standalone broker CLI entrypoint.'''
import pytest

from discord_bot.cli import broker as broker_cli
from discord_bot.clients.http_video_cache_store import HttpVideoCacheStore
from discord_bot.cogs.music import MusicGeneralConfig


def _settings(*, dispatch_http_url=None, bucket='media-bucket', music_general=None,
              database_http_url=None):
    '''Build a minimal raw settings dict for run().'''
    general = {}
    if dispatch_http_url is not None:
        general['dispatch_http_url'] = dispatch_http_url
    if database_http_url is not None:
        general['database_http_url'] = database_http_url
    music = {'storage': {'bucket_name': bucket}, 'download': {}}
    if music_general is not None:
        music['general'] = music_general
    return {
        'general': general,
        'music': music,
    }


class _GeneralConfig:
    '''Stand-in for GeneralConfig with the fields run() reads.'''
    def __init__(self, *, monitoring=None):
        self.monitoring = monitoring


def _patch_collaborators(mocker):
    '''Patch every constructor/helper run() touches; return the mocks.'''
    return {
        'setup_observability': mocker.patch.object(broker_cli, 'setup_observability'),
        'RedisManager': mocker.patch.object(broker_cli, 'RedisManager'),
        'RedisBrokerRegistry': mocker.patch.object(broker_cli, 'RedisBrokerRegistry'),
        '_build_video_cache': mocker.patch.object(broker_cli, '_build_video_cache', return_value=None),
        'HttpDispatchClient': mocker.patch.object(broker_cli, 'HttpDispatchClient'),
        'RedisBroker': mocker.patch.object(broker_cli, 'RedisBroker'),
        'RedisDownloadResultQueue': mocker.patch.object(broker_cli, 'RedisDownloadResultQueue'),
        'BrokerMetrics': mocker.patch.object(broker_cli, 'BrokerMetrics'),
        'BrokerHttpServer': mocker.patch.object(broker_cli, 'BrokerHttpServer'),
        'BrokerHealthServer': mocker.patch.object(broker_cli, 'BrokerHealthServer'),
        'run_broker': mocker.patch.object(broker_cli, 'run_broker'),
    }


def test_run_wires_dispatcher_when_configured(mocker):
    '''A configured general.dispatch_http_url builds an HttpDispatchClient and
    forwards it to the broker so bundle UI reaches the dispatcher pod.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(dispatch_http_url='http://dispatcher:8082')

    broker_cli.run(settings, _GeneralConfig())

    mocks['HttpDispatchClient'].assert_called_once_with('http://dispatcher:8082')
    _, broker_kwargs = mocks['RedisBroker'].call_args
    assert broker_kwargs['dispatcher'] is mocks['HttpDispatchClient'].return_value


def test_run_warns_and_skips_dispatcher_when_unset(mocker):
    '''With no general.dispatch_http_url the broker gets dispatcher=None and a
    warning is logged — otherwise all bundle UI is silently dropped.'''
    mocks = _patch_collaborators(mocker)
    warn = mocker.patch.object(broker_cli.logger, 'warning')

    broker_cli.run(_settings(), _GeneralConfig())

    mocks['HttpDispatchClient'].assert_not_called()
    _, broker_kwargs = mocks['RedisBroker'].call_args
    assert broker_kwargs['dispatcher'] is None
    warn.assert_called_once()


def test_run_passes_default_message_delete_after(mocker):
    '''An unconfigured music.general still hands the broker a concrete
    message_delete_after.

    This is the regression guard: the base class defaults the kwarg to None, and
    a None reaches Discord as "never expire". The failure summary goes out via a
    one-shot send_message, so once it is sent with no delete_after nothing holds
    a reference to it and "Error Details for Failed Downloads" stays in the
    channel permanently. Assert the value, not just that the kwarg was passed.
    '''
    mocks = _patch_collaborators(mocker)

    broker_cli.run(_settings(), _GeneralConfig())

    _, broker_kwargs = mocks['RedisBroker'].call_args
    assert broker_kwargs['message_delete_after'] == broker_cli.DEFAULT_MESSAGE_DELETE_AFTER
    assert broker_kwargs['message_delete_after'] is not None


def test_run_honours_configured_message_delete_after(mocker):
    '''music.general.message_delete_after overrides the default, mirroring how
    the in-process broker reads it off MusicGeneralConfig.'''
    mocks = _patch_collaborators(mocker)

    broker_cli.run(_settings(music_general={'message_delete_after': 45}), _GeneralConfig())

    _, broker_kwargs = mocks['RedisBroker'].call_args
    assert broker_kwargs['message_delete_after'] == 45


def test_default_message_delete_after_matches_music_config():
    '''The CLI default is a hand-copy of MusicGeneralConfig.message_delete_after
    (the broker process cannot import cogs.music — that drags discord.py /
    yt-dlp into a slim pod). Pin them together so the copy cannot drift.'''
    assert (broker_cli.DEFAULT_MESSAGE_DELETE_AFTER
            == MusicGeneralConfig().message_delete_after)


# --- MR 4a: the catalog is remote -------------------------------------------

def test_build_video_cache_returns_http_store_when_configured():
    '''Cache enabled + a db pod url + a bucket yields the HTTP store, pointed at the pod.'''
    store = broker_cli._build_video_cache(  # pylint: disable=protected-access
        {'enable_cache_files': True}, 'http://discord-db:8085', 'media-bucket')
    assert isinstance(store, HttpVideoCacheStore)
    assert store._base_url == 'http://discord-db:8085'  # pylint: disable=protected-access


def test_build_video_cache_sends_no_eviction_policy():
    '''
    max_cache_files / max_cache_size_mb stay with the pod that owns the catalog.

    The in-process client took them as constructor arguments. Passing them over
    the wire instead would let the broker and the db pod disagree about one
    catalog's eviction policy, so the HTTP store must not carry them at all --
    asserted on the built object rather than on the call, because the whole point
    is that there is nowhere for them to go.
    '''
    store = broker_cli._build_video_cache(  # pylint: disable=protected-access
        {'enable_cache_files': True, 'max_cache_files': 17, 'max_cache_size_mb': 5},
        'http://discord-db:8085', 'media-bucket')
    assert not hasattr(store, 'max_cache_files')
    assert not hasattr(store, 'max_cache_size_bytes')


def test_build_video_cache_returns_none_when_cache_disabled(caplog):
    '''Cache off is not a misconfiguration, so it must not warn.'''
    assert broker_cli._build_video_cache({}, 'http://discord-db:8085', 'b') is None  # pylint: disable=protected-access
    assert 'video cache disabled' not in caplog.text


@pytest.mark.parametrize('url,bucket,missing', [
    (None, 'media-bucket', 'general.database_http_url'),
    ('http://discord-db:8085', None, 'music.storage.bucket_name'),
])
def test_build_video_cache_warns_when_enabled_but_unreachable(url, bucket, missing, caplog):
    '''
    Cache asked for and not delivered is loud, and names the key that is missing.

    Under the in-process client a missing engine disabled the cache silently. The
    catalog now sits behind a URL, which is one more thing to fat-finger, and a
    typo there would otherwise read exactly like "caching intentionally off".
    '''
    with caplog.at_level('WARNING'):
        result = broker_cli._build_video_cache(  # pylint: disable=protected-access
            {'enable_cache_files': True}, url, bucket)
    assert result is None
    assert missing in caplog.text


@pytest.mark.asyncio
async def test_main_loop_closes_the_catalog_client(mocker):
    '''
    The catalog owns an aiohttp session now, so shutdown has to close it.

    Without this the process exits holding an open session and aiohttp logs
    "Unclosed client session" on every roll -- the same class of shutdown noise
    the pooled engine produced before dispose(close=False).
    '''
    broker_server = mocker.AsyncMock()
    redis_manager = mocker.AsyncMock()
    broker_metrics = mocker.MagicMock()
    broker_metrics.run = mocker.AsyncMock()
    video_cache = mocker.AsyncMock()
    mocker.patch.object(broker_cli.asyncio, 'Event', return_value=mocker.AsyncMock(wait=mocker.AsyncMock()))
    mocker.patch.object(broker_cli.signal, 'signal')

    await broker_cli.main_loop(broker_server, None, redis_manager, broker_metrics,
                               video_cache=video_cache)

    video_cache.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_survives_a_catalog_without_close(mocker):
    '''A stub catalog with no close() must not take the shutdown path down.'''
    broker_server = mocker.AsyncMock()
    redis_manager = mocker.AsyncMock()
    broker_metrics = mocker.MagicMock()
    broker_metrics.run = mocker.AsyncMock()
    mocker.patch.object(broker_cli.asyncio, 'Event', return_value=mocker.AsyncMock(wait=mocker.AsyncMock()))
    mocker.patch.object(broker_cli.signal, 'signal')

    await broker_cli.main_loop(broker_server, None, redis_manager, broker_metrics,
                               video_cache=object())

    redis_manager.close.assert_awaited_once()
