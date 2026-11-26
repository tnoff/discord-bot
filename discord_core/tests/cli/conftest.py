import signal

import pytest


@pytest.fixture
def captured_signals(monkeypatch):
    '''Replace signal.signal with a recorder so tests never touch the real handlers.'''
    handlers = {}
    monkeypatch.setattr(signal, 'signal', handlers.__setitem__)
    return handlers
