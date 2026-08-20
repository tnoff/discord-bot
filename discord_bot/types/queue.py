from asyncio import Queue as asyncio_queue, QueueFull
import random
from time import time
from typing import Generic, TypeVar

T = TypeVar('T')

class PutsBlocked(Exception):
    '''
    Puts Blocked on Queue
    '''

# How a submit rejection crosses the bot -> worker-pod HTTP seam.
#
# submit() raising PutsBlocked or QueueFull in-process is normal control flow,
# not a fault: cogs/music.py catches both at every submit site and cleans up
# (delete the bundle, push DISCARDED). Across the pod split the exception type
# cannot travel, so the worker pod encodes it as one of these statuses and the
# bot-side client decodes it back before the cog sees it. Without that, the
# rejection escaped _handle_submit as an unhandled exception -> aiohttp 500 ->
# `except PutsBlocked` never matched and the whole command failed.
#
# 4xx specifically, and never 5xx: async_retry_broker_command retries 5xx three
# times with exponential backoff, and a rejection is deterministic -- retrying
# only delays the identical answer (observed as ~7s added to a doomed /play).
SUBMIT_REJECTION_STATUS: dict[type[Exception], int] = {
    PutsBlocked: 409,
    QueueFull: 429,
}

SUBMIT_REJECTION_BY_STATUS: dict[int, type[Exception]] = {
    status: exception for exception, status in SUBMIT_REJECTION_STATUS.items()
}


def submit_rejection_status(exception: Exception) -> int | None:
    '''Status for a submit rejection, or None if it is not one.'''
    for exception_type, status in SUBMIT_REJECTION_STATUS.items():
        if isinstance(exception, exception_type):
            return status
    return None

class Queue(asyncio_queue[T], Generic[T]):
    '''
    Custom implementation of asyncio Queue
    '''
    def __init__(self, maxsize: int = 0):
        '''
        Custom implementation of Queue

        maxsize : Max size of queue
        '''
        self.shutdown: bool = False
        super().__init__(maxsize=maxsize)

    # Python 3.13 adds shutdown
    # https://docs.python.org/3/library/asyncio-queue.html#asyncio.Queue.shutdown
    def block(self):
        '''
        Block future puts, for when queue should be in shutdown
        '''
        self.shutdown = True

    def put_nowait(self, item: T) -> None:
        '''
        Put an item into the queue without blocking.
        Raises PutsBlocked if queue is shutdown.
        '''
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        super().put_nowait(item)

    async def put(self, item: T) -> None:
        '''
        Put an item into the queue, waiting if necessary.
        Raises PutsBlocked if queue is shutdown.
        '''
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        await super().put(item)

    def shuffle(self) -> bool:
        '''
        Shuffle queue
        '''
        random.seed(time())
        random.shuffle(self._queue)
        return True

    def size(self) -> int:
        '''
        Get size of queue
        '''
        return self.qsize()

    def clear(self) -> list[T]:
        '''
        Remove all items from queue
        '''
        items: list[T] = []
        while self.qsize():
            items.append(self._queue.popleft())
        return items

    def remove_item(self, queue_index: int) -> T | None:
        '''
        Remove item from queue

        queue_index : Index of item, counter starts at 1
        '''
        if queue_index < 1 or queue_index > self.qsize():
            return None
        # Rotate, remove top, then rotate back
        for _ in range(1, queue_index):
            self._queue.rotate(-1)
        item = self._queue.popleft()
        for _ in range(1, queue_index):
            self._queue.rotate(1)
        return item

    def bump_item(self, queue_index: int) -> T | None:
        '''
        Bump item to top of queue

        queue_index : Index of item, counter starts at 1
        '''
        item = self.remove_item(queue_index)
        if item is not None:
            self._queue.appendleft(item)
        return item

    def items(self) -> list[T]:
        '''
        Get a copy of all items in the queue
        '''
        items: list[T] = []
        for item in self._queue:
            items.append(item)
        return items
