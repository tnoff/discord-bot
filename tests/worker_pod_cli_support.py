'''
Helpers and shared cases for the tests of the two worker pods' CLIs (downloader and search).

Both pods are built on ``discord_core.cli._lib.worker_pod``, so ``run()`` validates the same
settings, builds the same health server, and ``main_loop()`` shuts down the same way. A pod's
CLI test module subclasses ``WorkerPodCliSuite`` and uses the helpers for the pod-specific cases.

Test names here must not be exactly 40 characters long: trufflehog's Lob detector matches
`test_` + 35 chars, the shape of a Lob test API key, and fails pr-check:secrets.
'''
import asyncio
import logging

import pytest

from discord_core.exceptions import DiscordBotException, ExitEarlyException
from discord_core.utils.common import SeamContractConfig
from discord_core.utils.loop_health import LoopHealth, LoopStatus


class GeneralConfig:
    '''Stand-in for GeneralConfig with the fields run() reads.'''
    def __init__(self, *, redis_url='redis://localhost:6379', redis_sentinel=None,
                 monitoring=None, logging_config=None, seam_contract=None):
        self.redis_url = redis_url
        self.redis_sentinel = redis_sentinel
        self.monitoring = monitoring
        self.logging = logging_config
        # The real GeneralConfig always has this -- it carries defaults -- so the
        # stub does too. Defaulting it to None here rather than omitting it keeps
        # the stub honest about what run() reads.
        self.seam_contract = seam_contract or SeamContractConfig()


def shutdown_mocks(mocker):
    '''The async collaborators main_loop drives and drains: (redis_manager, server, metrics).'''
    redis_manager = mocker.Mock()
    redis_manager.start = mocker.AsyncMock()
    redis_manager.close = mocker.AsyncMock()
    server = mocker.Mock()
    server.serve = mocker.AsyncMock()
    server.drain_and_stop = mocker.AsyncMock()
    metrics = mocker.Mock()
    metrics.run = mocker.AsyncMock()
    return redis_manager, server, metrics


def health_server_mock(mocker):
    '''A health server whose serve() can be awaited.'''
    health_server = mocker.Mock()
    health_server.serve = mocker.AsyncMock()
    return health_server


def assert_pod_served(redis_manager, server, health_server, metrics):
    '''main_loop started redis, then served the pod's server, health server and metrics.'''
    redis_manager.start.assert_awaited_once()
    server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    metrics.run.assert_awaited_once()


def health_server_monitoring(mocker):
    '''A monitoring config with the health server enabled on 8080.'''
    health_cfg = mocker.Mock(enabled=True, port=8080, bind_address='0.0.0.0')
    return mocker.Mock(health_server=health_cfg)


class WorkerPodCliSuite:
    '''Cases both worker pods' CLIs must pass.

    Not collected itself; a subclass in a ``test_*`` module supplies the hooks below.
    '''
    # The pod's CLI module, with run() and the driver loop.
    cli = None
    # The key in patch_collaborators()'s dict for the pod's run-loop mock.
    run_loop_mock = ''

    def patch_collaborators(self, mocker):
        '''Patch every constructor/helper run() touches; return the mocks by name.'''
        raise NotImplementedError('patch_collaborators')

    def settings(self):
        '''A minimal raw settings dict for run().'''
        raise NotImplementedError('settings')

    def make_runner(self, mocker, run):
        '''The object the pod's driver loop drives, whose loop coroutine is ``run``.'''
        raise NotImplementedError('make_runner')

    async def drive(self, runner, stop_event, log, health):
        '''Run the pod's driver loop once over ``runner``.'''
        raise NotImplementedError('drive')

    def runner_run(self, runner):
        '''The loop coroutine make_runner() installed.'''
        raise NotImplementedError('runner_run')

    def test_run_builds_health_server_when_enabled(self, mocker):
        '''A monitoring.health_server.enabled config builds a RedisPingHealthServer.'''
        mocks = self.patch_collaborators(mocker)
        general = GeneralConfig(monitoring=health_server_monitoring(mocker))
        self.cli.run(self.settings(), general)
        mocks['RedisPingHealthServer'].assert_called_once()
        _, hs_kwargs = mocks['RedisPingHealthServer'].call_args
        assert hs_kwargs['port'] == 8080
        assert hs_kwargs['bind_address'] == '0.0.0.0'
        health_server = mocks['RedisPingHealthServer'].return_value
        assert mocks[self.run_loop_mock].call_args[0][2] is health_server

    def test_run_missing_redis_raises(self, mocker):
        '''No redis_url and no redis_sentinel -> DiscordBotException.'''
        self.patch_collaborators(mocker)
        general = GeneralConfig(redis_url=None, redis_sentinel=None)
        with pytest.raises(DiscordBotException, match='Redis required'):
            self.cli.run(self.settings(), general)

    def test_run_missing_broker_url_raises(self, mocker):
        '''No music.broker_client.url -> DiscordBotException.'''
        self.patch_collaborators(mocker)
        settings = self.settings()
        settings['music']['broker_client'] = {}
        with pytest.raises(DiscordBotException, match='broker_client.url required'):
            self.cli.run(settings, GeneralConfig())

    @pytest.mark.asyncio
    async def test_drive_guards_broad_exception(self, mocker):
        '''A loop exception is logged and the loop continues (does not propagate).'''
        stop_event = asyncio.Event()
        calls = []

        async def _run(_evt):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError('redis blip')
            # Second call: request shutdown so the loop exits cleanly.
            stop_event.set()

        runner = self.make_runner(mocker, _run)
        mocker.patch.object(self.cli.asyncio, 'sleep', new=mocker.AsyncMock())
        log = mocker.Mock(spec=logging.Logger)
        health = LoopHealth('test_loop')

        # Must not raise despite the RuntimeError on the first iteration.
        await self.drive(runner, stop_event, log, health)

        assert len(calls) == 2
        log.exception.assert_called_once()
        # The failed iteration was recorded, the following one re-armed the window.
        assert health.consecutive_errors == 0
        assert health.status == LoopStatus.OK

    @pytest.mark.asyncio
    async def test_drive_exits_on_exit_early(self, mocker):
        '''ExitEarlyException (shutdown mid-backoff) breaks the loop without logging an error.'''
        async def _run(_evt):
            raise ExitEarlyException('shutdown')

        runner = self.make_runner(mocker, _run)
        log = mocker.Mock(spec=logging.Logger)
        await self.drive(runner, asyncio.Event(), log, LoopHealth('test_loop'))
        log.exception.assert_not_called()

    @pytest.mark.asyncio
    async def test_drive_skips_when_already_stopped(self, mocker):
        '''A pre-set stop_event means the loop body is never called.'''
        stop_event = asyncio.Event()
        stop_event.set()
        runner = self.make_runner(mocker, mocker.AsyncMock())
        await self.drive(runner, stop_event, mocker.Mock(), LoopHealth('test_loop'))
        self.runner_run(runner).assert_not_called()
