from asyncio import Queue as asyncio_queue
from random import shuffle as random_shuffle

class PutsBlocked(Exception):
    '''
    Puts Blocked on Queue
    '''

class Queue(asyncio_queue):
    '''
    Custom implementation of asyncio Queue
    '''
    def __init__(self, maxsize: int = 0, num_shuffles: int = 5):
        '''
        Custom implementation of Queue

        maxsize : Max size of queue
        num_shuffles : Number of shuffles to use
        '''
        self.shutdown = False
        self.num_shuffles = num_shuffles
        super().__init__(maxsize=maxsize)

    def block(self):
        '''
        Block future puts, for when queue should be in shutdown
        '''
        self.shutdown = True

    def unblock(self):
        '''
        Unblock queue
        '''
        self.shutdown = False

    def put_nowait(self, item):
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        super().put_nowait(item)

    async def put(self, item):
        if self.shutdown:
            raise PutsBlocked('Puts Blocked on Queue')
        await super().put(item)

    def shuffle(self):
        '''
        Shuffle queue
        '''
        for _ in range(self.num_shuffles):
            random_shuffle(self._queue)
        return True

    def size(self):
        '''
        Get size of queue
        '''
        return self.qsize()

    def clear(self):
        '''
        Remove all items from queue
        '''
        items = []
        while self.qsize():
            items.append(self._queue.popleft())
        return items

    def remove_item(self, queue_index: int):
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

    def bump_item(self, queue_index: int):
        '''
        Bump item to top of queue

        queue_index : Index of item, counter starts at 1
        '''
        item = self.remove_item(queue_index)
        if item is not None:
            self._queue.appendleft(item)
        return item

    def items(self):
        '''
        Get a copy of all items in the queue
        '''
        items = []
        for item in self._queue:
            items.append(item)
        return items
