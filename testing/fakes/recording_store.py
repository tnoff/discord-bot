'''A store stand-in that records its calls and can fail on demand, for tests of the HTTP stores.'''


class RecordingStoreBase:
    '''Records each call as (name, *args) in ``calls``; raises ``error`` from every call if set.

    A subclass defines the async store methods, each of which calls ``self._record``.
    '''

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if self.error:
            raise self.error
