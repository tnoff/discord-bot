'''Tests for ExitProbe / MullvadExitProbe — the downloader's egress-exit probe.'''
import asyncio

import aiohttp
import pytest

from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.integrations.egress_probe import (
    EXIT_PROBE_TYPES, MULLVAD_JSON_URL, MullvadExitProbe, UNKNOWN_EXIT,
    _default_session_factory, build_exit_probe, cached_exit_attributes,
    cached_exit_hostname,
)


class _FakeResponse:
    '''aiohttp-response-shaped async context manager returning a fixed payload.'''

    def __init__(self, payload, raise_exc=None):
        self._payload = payload
        self._raise_exc = raise_exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def raise_for_status(self):
        '''Mimic aiohttp's raise_for_status, raising the configured error.'''
        if self._raise_exc is not None:
            raise self._raise_exc

    async def json(self):
        '''Return the canned JSON payload.'''
        return self._payload


class _FakeSession:
    '''aiohttp-session-shaped async context manager recording get() calls.'''

    def __init__(self, response):
        self._response = response
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def get(self, url, proxy=None):
        '''Record the URL + proxy and hand back the canned response.'''
        self.get_calls.append((url, proxy))
        return self._response


def _probe(payload=None, raise_exc=None, proxy='http://discord-vpn:8888'):
    '''Return (probe, session) wired to a fake session returning payload.'''
    session = _FakeSession(_FakeResponse(payload or {}, raise_exc=raise_exc))
    probe = MullvadExitProbe(proxy, session_factory=lambda: session)
    return probe, session


@pytest.mark.asyncio
async def test_refresh_caches_hostname_and_ip_through_proxy():
    '''refresh() caches the exit hostname + IP and requests through the proxy.'''
    probe, session = _probe({'mullvad_exit_ip_hostname': 'us-lax-wg-101', 'ip': '1.2.3.4'})
    await probe.refresh()
    assert probe.exit_hostname == 'us-lax-wg-101'
    assert probe.exit_ip == '1.2.3.4'
    assert session.get_calls == [(MULLVAD_JSON_URL, 'http://discord-vpn:8888')]


@pytest.mark.asyncio
async def test_refresh_failsafe_on_non_dict_payload():
    '''A non-dict JSON body (e.g. a list) normalizes to unknown, never raises.'''
    probe, _ = _probe(['not', 'a', 'dict'])
    await probe.refresh()
    assert probe.exit_hostname is None
    assert probe.exit_ip is None
    assert cached_exit_attributes(probe) == (UNKNOWN_EXIT, UNKNOWN_EXIT)


@pytest.mark.asyncio
async def test_refresh_failsafe_on_non_string_fields():
    '''Null / non-string fields normalize to None (=> unknown downstream).'''
    probe, _ = _probe({'mullvad_exit_ip_hostname': None, 'ip': 12345})
    await probe.refresh()
    assert probe.exit_hostname is None
    assert probe.exit_ip is None


@pytest.mark.asyncio
async def test_refresh_strips_and_drops_blank_fields():
    '''Padded values are stripped; whitespace-only fields collapse to None.'''
    probe, _ = _probe({'mullvad_exit_ip_hostname': '  us-lax-wg-101  ', 'ip': '   '})
    await probe.refresh()
    assert probe.exit_hostname == 'us-lax-wg-101'
    assert probe.exit_ip is None


@pytest.mark.asyncio
async def test_refresh_raises_on_http_error():
    '''A non-2xx response propagates out of refresh() (run() is what swallows it).'''
    probe, _ = _probe(raise_exc=aiohttp.ClientResponseError(None, None, status=502))
    with pytest.raises(aiohttp.ClientResponseError):
        await probe.refresh()
    # Never-probed cache stays None.
    assert probe.exit_hostname is None
    assert probe.exit_ip is None


@pytest.mark.asyncio
async def test_run_swallows_error_and_keeps_last_value(mocker):
    '''run() keeps the last-known exit when a later refresh fails, never raising.'''
    probe, _ = _probe({'mullvad_exit_ip_hostname': 'se-sto-wg-001', 'ip': '9.9.9.9'})
    await probe.refresh()  # seed a known-good value
    stop = asyncio.Event()

    async def _boom():
        stop.set()
        raise RuntimeError('proxy down')

    mocker.patch.object(probe, 'refresh', side_effect=_boom)
    await probe.run(stop, interval=0.001)  # must not raise
    # Last-known value survived the failed refresh.
    assert probe.exit_hostname == 'se-sto-wg-001'
    assert probe.exit_ip == '9.9.9.9'


@pytest.mark.asyncio
async def test_run_refreshes_each_tick_until_stopped(mocker):
    '''run() refreshes across ticks (exercising the inter-tick wait) then exits.'''
    probe, _ = _probe()
    stop = asyncio.Event()
    calls = {'n': 0}

    async def _refresh():
        calls['n'] += 1
        if calls['n'] == 2:
            stop.set()

    mocker.patch.object(probe, 'refresh', side_effect=_refresh)
    await probe.run(stop, interval=0.001)
    assert calls['n'] == 2


def test_build_exit_probe_none_when_unconfigured():
    '''A falsy egress_probe config disables attribution (no probe built).'''
    assert build_exit_probe(None, 'http://discord-vpn:8888') is None
    assert build_exit_probe('', None) is None


def test_build_exit_probe_selects_mullvad():
    '''egress_probe=mullvad builds a MullvadExitProbe wired to the proxy.'''
    probe = build_exit_probe('mullvad', 'http://discord-vpn:8888')
    assert isinstance(probe, MullvadExitProbe)
    assert probe.PROBE_URL == MULLVAD_JSON_URL
    assert 'mullvad' in EXIT_PROBE_TYPES


def test_build_exit_probe_unknown_type_raises():
    '''An unknown egress_probe fails loudly rather than silently disabling.'''
    with pytest.raises(DiscordBotException):
        build_exit_probe('nordvpn', None)


def test_cached_exit_attributes_none_probe():
    '''A None probe (in-process/bot path) attributes both fields to unknown.'''
    assert cached_exit_attributes(None) == (UNKNOWN_EXIT, UNKNOWN_EXIT)


@pytest.mark.asyncio
async def test_cached_exit_attributes_partial_falls_back():
    '''A missing IP field falls back to unknown while the hostname is kept.'''
    probe, _ = _probe({'mullvad_exit_ip_hostname': 'us-lax-wg-101'})
    await probe.refresh()
    assert cached_exit_attributes(probe) == ('us-lax-wg-101', UNKNOWN_EXIT)


def test_cached_exit_hostname_unknown_when_never_probed():
    '''cached_exit_hostname on a fresh probe reads unknown.'''
    probe, _ = _probe()
    assert cached_exit_hostname(probe) == UNKNOWN_EXIT


@pytest.mark.asyncio
async def test_default_session_factory_builds_client_session():
    '''The default factory yields a real aiohttp ClientSession.'''
    session = _default_session_factory()
    try:
        assert isinstance(session, aiohttp.ClientSession)
    finally:
        await session.close()
