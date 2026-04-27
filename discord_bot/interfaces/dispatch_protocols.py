'''
Abstract base classes for the MessageDispatcher service.

BundleStore and WorkQueue define the interface that both in-process
(asyncio) and Redis-backed implementations must satisfy.
'''
from abc import ABC, abstractmethod


class BundleStore(ABC):
    '''Async interface for persisting mutable-bundle state.'''

    @abstractmethod
    async def save(self, key: str, bundle_dict: dict) -> None:
        '''Persist *bundle_dict* under *key*.'''

    @abstractmethod
    async def delete(self, key: str) -> None:
        '''Remove the bundle stored under *key*.'''

    @abstractmethod
    async def load(self, key: str) -> dict | None:
        '''Return the bundle dict for *key*, or None if not found.'''

    @abstractmethod
    async def load_all(self) -> dict[str, dict]:
        '''Return all persisted bundles keyed by their bundle key.'''


class WorkQueue(ABC):
    '''Async interface for the dispatch work queue.'''

    @abstractmethod
    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        '''Add a new entry to the queue.'''

    @abstractmethod
    async def enqueue_unique(self, member: str, payload: dict, priority: int) -> None:
        '''Add an entry only if *member* is not already queued; always update payload.'''

    @abstractmethod
    async def dequeue(self, timeout: float = 1.0) -> tuple[str, dict] | None:
        '''Pop the highest-priority item; return (member, payload) or None on timeout.'''

    @abstractmethod
    async def acquire_lock(self, bundle_key: str) -> bool:
        '''Try to acquire the per-bundle execution lock; return True if acquired.'''

    @abstractmethod
    async def release_lock(self, bundle_key: str) -> None:
        '''Release the per-bundle execution lock.'''

    @abstractmethod
    async def store_result(self, request_id: str, result: dict) -> None:
        '''Persist a fetch result so it can be retrieved by request_id.'''

    @abstractmethod
    async def get_result(self, request_id: str) -> dict | None:
        '''Return the stored result dict, or None if not yet ready.'''
