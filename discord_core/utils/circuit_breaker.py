"""
Async circuit breaker for outbound RPCs.

Wraps a callable in a three-state breaker (CLOSED → OPEN → HALF_OPEN) so a
peer outage stops burning per-call retry budget after N consecutive failures.
Time source is injectable so tests can fast-forward without sleeping.
"""
import asyncio
import logging
import time
from enum import Enum
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(str, Enum):
    """Breaker state. String-typed so OTel attributes serialize cleanly."""
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half_open'


class CircuitBreakerOpenError(Exception):
    """Raised by CircuitBreaker.call when the breaker is OPEN."""


class CircuitBreaker:
    """
    Three-state async circuit breaker.

    - CLOSED: calls run normally. Consecutive failures count up; on reaching
      ``failure_threshold`` the breaker transitions to OPEN.
    - OPEN: every call raises ``CircuitBreakerOpenError`` immediately for
      ``recovery_timeout`` seconds, then the next call transitions to HALF_OPEN.
    - HALF_OPEN: one probe call is allowed. Success → CLOSED + counter reset.
      Failure → OPEN + timer restart.

    ``time_func`` defaults to ``time.monotonic`` and can be replaced for tests.
    """

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 time_func: Callable[[], float] = time.monotonic):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._time = time_func
        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Read the current state. Side-effect-free; transition only happens on call()."""
        return self._state

    async def call(self, func: Callable[[], Awaitable[T]]) -> T:
        """
        Run ``func()`` through the breaker.

        Raises ``CircuitBreakerOpenError`` immediately when OPEN and the
        recovery timeout hasn't elapsed. Otherwise runs the call; on exception
        records a failure (and may transition to OPEN); on success records a
        success (and may transition to CLOSED from HALF_OPEN).
        """
        async with self._lock:
            self._maybe_half_open()
            if self._state is CircuitState.OPEN:
                raise CircuitBreakerOpenError(
                    f'CircuitBreaker[{self.name}] is open'
                )
        try:
            result = await func()
        except Exception:
            async with self._lock:
                self._record_failure()
            raise
        async with self._lock:
            self._record_success()
        return result

    def _maybe_half_open(self) -> None:
        if self._state is not CircuitState.OPEN:
            return
        if self._time() - self._opened_at < self.recovery_timeout:
            return
        self._state = CircuitState.HALF_OPEN
        logger.info('CircuitBreaker[%s] OPEN → HALF_OPEN', self.name)

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._state is CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = self._time()
            logger.warning('CircuitBreaker[%s] HALF_OPEN → OPEN (probe failed)', self.name)
            return
        if self._consecutive_failures >= self.failure_threshold:
            previous = self._state
            self._state = CircuitState.OPEN
            self._opened_at = self._time()
            if previous is not CircuitState.OPEN:
                logger.warning(
                    'CircuitBreaker[%s] CLOSED → OPEN (%d consecutive failures)',
                    self.name, self._consecutive_failures,
                )

    def _record_success(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            logger.info('CircuitBreaker[%s] HALF_OPEN → CLOSED (probe succeeded)', self.name)
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = None
