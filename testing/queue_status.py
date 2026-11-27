'''The status payload a queue worker's ``status_snapshot()`` returns, for tests of its consumers.'''


def status_snapshot(*, summary=None, backoff=None, queue_sizes=None, failure_count=0):
    '''A status_snapshot()-shaped dict. failure_count is required: queue_metrics reads it with a
    bare subscript, so a snapshot without it KeyErrors on the bot side.'''
    return {
        'failure_summary': summary or f'{failure_count} failures in queue',
        'failure_count': failure_count,
        'backoff_seconds_remaining': backoff,
        'queue_sizes': queue_sizes or {},
    }
