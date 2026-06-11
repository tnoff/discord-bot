'''Tests for the async CircuitBreaker.'''
import pytest

from discord_bot.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
)


class FakeClock:
    '''Manually advanced monotonic clock for breaker recovery-timeout tests.'''

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        '''Return the current fake monotonic time.'''
        return self.now

    def advance(self, seconds: float) -> None:
        '''Move the fake clock forward by *seconds*.'''
        self.now += seconds


def _ok():
    async def _inner():
        return 'ok'
    return _inner


def _bad():
    async def _inner():
        raise RuntimeError('boom')
    return _inner


@pytest.mark.asyncio
async def test_starts_closed_and_passes_through():
    '''Initial state is CLOSED; successful calls return their result.'''
    breaker = CircuitBreaker(name='test', failure_threshold=3, time_func=FakeClock())
    assert breaker.state is CircuitState.CLOSED
    assert await breaker.call(_ok()) == 'ok'
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_opens_after_threshold_failures():
    '''CLOSED → OPEN once consecutive failures reach the threshold.'''
    breaker = CircuitBreaker(name='test', failure_threshold=3, time_func=FakeClock())
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    assert breaker.state is CircuitState.OPEN


@pytest.mark.asyncio
async def test_open_raises_circuit_breaker_open_error_fast():
    '''Calls while OPEN raise CircuitBreakerOpenError without invoking the func.'''
    breaker = CircuitBreaker(name='test', failure_threshold=2, time_func=FakeClock())
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    invoked = False
    async def _spy():
        nonlocal invoked
        invoked = True
        return 'ok'
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(_spy)
    assert invoked is False


@pytest.mark.asyncio
async def test_success_resets_failure_counter_in_closed():
    '''A success in CLOSED state resets the consecutive-failure counter.'''
    breaker = CircuitBreaker(name='test', failure_threshold=3, time_func=FakeClock())
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    assert breaker.state is CircuitState.CLOSED
    await breaker.call(_ok())
    # Two more failures shouldn't open it yet — counter reset
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_recovery_timeout_transitions_to_half_open():
    '''After recovery_timeout elapses, the next call transitions OPEN → HALF_OPEN.'''
    clock = FakeClock()
    breaker = CircuitBreaker(name='test', failure_threshold=2,
                             recovery_timeout=10.0, time_func=clock)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    assert breaker.state is CircuitState.OPEN
    clock.advance(10.1)
    # The next call probes — success closes the breaker
    assert await breaker.call(_ok()) == 'ok'
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_half_open_failure_reopens_breaker():
    '''A failed probe call reopens the breaker and restarts the timer.'''
    clock = FakeClock()
    breaker = CircuitBreaker(name='test', failure_threshold=2,
                             recovery_timeout=10.0, time_func=clock)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    clock.advance(10.1)
    with pytest.raises(RuntimeError):
        await breaker.call(_bad())
    assert breaker.state is CircuitState.OPEN
    # Immediately after, breaker still open
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(_ok())


@pytest.mark.asyncio
async def test_open_before_timeout_stays_open():
    '''Calls before recovery_timeout elapses keep raising CircuitBreakerOpenError.'''
    clock = FakeClock()
    breaker = CircuitBreaker(name='test', failure_threshold=2,
                             recovery_timeout=10.0, time_func=clock)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_bad())
    clock.advance(5.0)
    with pytest.raises(CircuitBreakerOpenError):
        await breaker.call(_ok())
    assert breaker.state is CircuitState.OPEN
