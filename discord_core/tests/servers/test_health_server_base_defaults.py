'''HealthServerBase's own defaults: what a subclass inherits.'''

import pytest

from discord_core.servers.health_server_base import HealthServerBase
from discord_core.utils.loop_health import LOOP_HEALTH


@pytest.mark.asyncio
async def test_health_server_base_check_not_implemented():
    '''HealthServerBase._check raises NotImplementedError — subclasses must override it.'''
    base = HealthServerBase(port=0, bind_address='127.0.0.1')
    with pytest.raises(NotImplementedError):
        await base._check()  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_health_server_base_readiness_defaults_to_check():
    '''HealthServerBase._readiness_check delegates to _check by default.'''
    class _Stub(HealthServerBase):
        async def _check(self):
            return False, {'why': 'stub'}
    stub = _Stub(port=0, bind_address='127.0.0.1')
    ok, extra = await stub._readiness_check()  #pylint:disable=protected-access
    assert ok is False
    assert extra == {'why': 'stub'}


def test_apply_loop_health_omits_the_key_when_no_loops_are_registered():
    # Processes without background loops are unaffected — no empty 'loops' dict.
    ok, extra = HealthServerBase._apply_loop_health(True, {'db': 'ok'})  #pylint:disable=protected-access
    assert ok is True
    assert extra == {'db': 'ok'}


def test_apply_loop_health_cannot_rescue_an_already_failing_check():
    # Loop health only ever adds a reason to fail, never masks one.
    LOOP_HEALTH.register('process_search_results', stale_after_seconds=60)
    ok, extra = HealthServerBase._apply_loop_health(False, {})  #pylint:disable=protected-access
    assert ok is False
    assert extra == {'loops': {'process_search_results': 'ok'}}
