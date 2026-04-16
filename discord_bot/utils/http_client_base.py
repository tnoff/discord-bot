'''Shared aiohttp session helpers used by HTTP client classes.'''
import aiohttp
from opentelemetry.propagate import inject


class HttpClientMixin:
    '''Mixin providing lazy aiohttp session management and trace header injection.'''
    _session: aiohttp.ClientSession | None = None

    def _get_session(self) -> aiohttp.ClientSession:
        '''Return the shared session, creating it lazily on first use.'''
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        '''Close the underlying aiohttp session.'''
        if self._session and not self._session.closed:
            await self._session.close()

    def _trace_headers(self) -> dict[str, str]:
        '''Return headers dict with W3C traceparent injected from the active span, if any.'''
        headers: dict[str, str] = {}
        inject(headers)
        return headers
