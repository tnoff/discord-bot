'''Tests for the standalone broker CLI entrypoint.'''
import click
import pytest
from click.testing import CliRunner

from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import SeamContractConfig

from discord_gateway.cogs.music import MusicGeneralConfig
from discord_broker.cli import broker as broker_cli
from discord_broker.clients.http_video_cache_store import HttpVideoCacheStore


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
    def __init__(self, *, monitoring=None, seam_contract=None):
        self.monitoring = monitoring
        # The real GeneralConfig always has this -- it carries defaults -- so the
        # stub does too. Defaulting it to None here rather than omitting it keeps
        # the stub honest about what run() reads.
        self.seam_contract = seam_contract or SeamContractConfig()


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

    mocks['HttpDispatchClient'].assert_called_once_with(
        'http://dispatcher:8082', seam_contract=SeamContractConfig())
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
    # Other settings may warn too (no db pod, say); the dispatcher one is the point here.
    assert [call.args[0] for call in warn.call_args_list if 'dispatch_http_url' in call.args[0]]


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


# ---------------------------------------------------------------------------
# --flush-cache
# ---------------------------------------------------------------------------

FLUSH_SETTINGS = {
    'general': {'database_http_url': 'http://discord-db:8085'},
    'music': {'storage': {'bucket_name': 'media-bucket'},
              'download': {'cache': {'enable_cache_files': True}}},
}


def _patch_flush_collaborators(mocker, *, video_cache, removed=3, counts=(5, 2)):
    redis_manager = mocker.MagicMock()
    redis_manager.start = mocker.AsyncMock()
    redis_manager.close = mocker.AsyncMock()
    mocker.patch.object(broker_cli, 'RedisManager').from_general_config.return_value = redis_manager
    mocker.patch.object(broker_cli, 'RedisBrokerRegistry')
    build = mocker.patch.object(broker_cli, '_build_video_cache', return_value=video_cache)
    broker = mocker.MagicMock()
    broker.get_cache_count = mocker.AsyncMock(side_effect=list(counts))
    broker.evict_cache = mocker.AsyncMock(return_value=removed)
    broker_cls = mocker.patch.object(broker_cli, 'RedisBroker', return_value=broker)
    return redis_manager, build, broker, broker_cls


@pytest.mark.asyncio
async def test_flush_cache_entries_evicts_everything_and_closes_clients(mocker):
    '''--yes evicts with evict_all and closes the db client and Redis afterwards.'''
    video_cache = mocker.AsyncMock()
    redis_manager, build, broker, broker_cls = _patch_flush_collaborators(mocker, video_cache=video_cache)

    removed = await broker_cli.flush_cache_entries(FLUSH_SETTINGS, _GeneralConfig(), assume_yes=True)

    assert removed == 3
    broker.evict_cache.assert_awaited_once_with(evict_all=True)
    assert build.call_args.args[2] == 'media-bucket'
    assert broker_cls.call_args.kwargs == {'video_cache': video_cache, 'bucket_name': 'media-bucket'}
    video_cache.close.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_cache_entries_declined_deletes_nothing(mocker):
    '''Answering no at the prompt aborts before anything is evicted, and still closes the clients.'''
    video_cache = mocker.AsyncMock()
    redis_manager, _, broker, _ = _patch_flush_collaborators(mocker, video_cache=video_cache)
    mocker.patch.object(broker_cli.click, 'confirm', side_effect=click.Abort())

    with pytest.raises(click.Abort):
        await broker_cli.flush_cache_entries(FLUSH_SETTINGS, _GeneralConfig(), assume_yes=False)

    broker.evict_cache.assert_not_awaited()
    video_cache.close.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_cache_entries_requires_cache_enabled(mocker):
    '''No cache on this pod is an error, not a silent no-op, and Redis is never opened.'''
    redis_manager, _, _, _ = _patch_flush_collaborators(mocker, video_cache=None)

    with pytest.raises(DiscordBotException):
        await broker_cli.flush_cache_entries(FLUSH_SETTINGS, _GeneralConfig(), assume_yes=True)

    redis_manager.start.assert_not_awaited()


def test_main_flush_cache_flag_flushes_instead_of_serving(mocker, tmp_path):
    '''--flush-cache --yes runs the flush and never starts the broker.'''
    config_file = tmp_path / 'discord.cnf'
    config_file.write_text('general: {}\n')
    general_config = _GeneralConfig()
    mocker.patch.object(broker_cli, 'parse_and_validate_config', return_value=(FLUSH_SETTINGS, general_config))
    flush = mocker.patch.object(broker_cli, 'flush_cache_entries', new=mocker.AsyncMock(return_value=0))
    run = mocker.patch.object(broker_cli, 'run')

    result = CliRunner().invoke(broker_cli.main, [str(config_file), '--flush-cache', '--yes'])

    assert result.exit_code == 0
    flush.assert_awaited_once_with(FLUSH_SETTINGS, general_config, assume_yes=True)
    run.assert_not_called()


def test_main_without_flag_still_serves(mocker, tmp_path):
    '''The container's normal invocation is unchanged: no flag runs the broker.'''
    config_file = tmp_path / 'discord.cnf'
    config_file.write_text('general: {}\n')
    general_config = _GeneralConfig()
    mocker.patch.object(broker_cli, 'parse_and_validate_config', return_value=(FLUSH_SETTINGS, general_config))
    flush = mocker.patch.object(broker_cli, 'flush_cache_entries', new=mocker.AsyncMock())
    run = mocker.patch.object(broker_cli, 'run')

    result = CliRunner().invoke(broker_cli.main, [str(config_file)])

    assert result.exit_code == 0
    run.assert_called_once_with(FLUSH_SETTINGS, general_config)
    flush.assert_not_called()
