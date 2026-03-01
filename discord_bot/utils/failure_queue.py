from asyncio import QueueFull, QueueEmpty
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

from discord_bot.utils.queue import Queue


@dataclass
class FailureStatus:
    '''
    Track status of an operation for failure rate tracking
    '''
    success: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    exception_type: str = None
    exception_message: str = None


class FailureQueue:
    '''
    Failure Rate Tracking
    '''
    def __init__(self, max_size: int = 100, max_age_seconds: int = 300):
        '''
        Failure queue to track how often failures have been happening

        max_size : Track the last X items
        max_age_seconds : Maximum age of failures to keep (in seconds)
        '''
        self.queue: Queue[FailureStatus] = Queue(maxsize=max_size)
        self.max_age_seconds = max_age_seconds

    def add_item(self, new_item: FailureStatus) -> bool:
        '''
        Add new item and clean old entries
        '''
        self._clean_old_items()
        if new_item.success:
            # If we hit a success, get the last queue item to reset some of the counters
            try:
                self.queue.get_nowait()
            except QueueEmpty:
                pass
            return True
        while True:
            try:
                self.queue.put_nowait(new_item)
                return True
            except QueueFull:
                self.queue.get_nowait()

    def _clean_old_items(self):
        '''
        Remove items older than max_age_seconds
        '''
        if self.max_age_seconds <= 0:
            return

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.max_age_seconds)

        # Get all items, filter out old ones, and rebuild queue
        items = self.queue.clear()
        fresh_items = [item for item in items if item.created_at > cutoff]

        for item in fresh_items:
            try:
                self.queue.put_nowait(item)
            except QueueFull:
                break

    @property
    def size(self):
        '''
        Size of queue
        '''
        return self.queue.size()

    def get_status_summary(self) -> str:
        '''
        Get a summary string of the queue status for logging
        Returns format: "X failures in queue, oldest: <timestamp>"
        '''
        items = self.queue.items()
        if not items:
            return "0 failures in queue"

        oldest = min(items, key=lambda x: x.created_at)
        age_seconds = int((datetime.now(timezone.utc) - oldest.created_at).total_seconds())

        if age_seconds >= 60:
            age_str = f"{age_seconds // 60}m {age_seconds % 60}s ago"
        else:
            age_str = f"{age_seconds}s ago"

        return f"{len(items)} failures in queue, oldest: {age_str}"
