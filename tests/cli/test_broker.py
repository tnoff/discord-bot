'''Tests for the standalone broker CLI entrypoint.'''
from discord_bot.cli import broker as broker_cli


def _settings(*, dispatch_http_url=None, bucket='media-bucket'):
    '''Build a minimal raw settings dict for run().'''
    general = {}
    if dispatch_http_url is not None:
        general['dispatch_http_url'] = dispatch_http_url
    return {
        'general': general,
        'music': {'storage': {'bucket_name': bucket}, 'download': {}},
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
        'managed_db': mocker.patch.object(broker_cli, 'managed_db'),
        'instrument_sqlalchemy': mocker.patch.object(broker_cli, 'instrument_sqlalchemy'),
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
