'''
Tell yt-dlp's harmless empty last HLS fragment apart from real fragment loss.

YouTube's HLS manifests (format 96, the fallback when the SABR experiment strips a
client's https formats) can end with a 0-byte segment (``#EXTINF:0.021``). yt-dlp's
HTTP downloader reports that empty body as ``ERROR: Did not get any data blocks`` and,
with ``skip_unavailable_fragments`` on, logs ``Skipping fragment N ...`` and carries on.
A yt-dlp maintainer's verdict (yt-dlp/yt-dlp#14997): when it is the final fragment
"it is safe to assume you're not missing any data". Setting
``skip_unavailable_fragments`` to False made every such download fail, and retrying
cannot help because each attempt gets the same manifest.

yt-dlp has no hook that says which fragment it skipped, but it narrates each skip to the
``logger`` it is given. ``FragmentSkipLogger`` wraps that logger, watches the narration,
and reports the skips that are NOT the empty final fragment, so the caller can still
reject a download that lost audio mid-stream.
'''
import re
import threading

# yt-dlp's own wording; to_screen() routes these through logger.debug().
EMPTY_BODY_ERROR = 'Did not get any data blocks'
_TOTAL_FRAGMENTS = re.compile(r'\[\w+\] Total fragments: (\d+)')
_SKIPPED_FRAGMENT = re.compile(r'Skipping fragment (\d+) \.\.\.')


class FragmentSkipLogger:
    '''
    A logger proxy for yt-dlp that records which fragments it skipped for a reason other
    than an empty final fragment.

    Everything is forwarded to the wrapped logger unchanged. The record is kept per
    thread: pool-mode downloads share this one logger object (the options dict is
    copied per exit, the logger inside it is not) and each runs on its own executor
    thread, so call ``reset()`` on the downloading thread before ``extract_info`` and
    read ``lost_fragments()`` on the same thread after it.
    '''

    def __init__(self, logger):
        self._logger = logger
        self._state = threading.local()

    def reset(self) -> None:
        '''Forget everything seen so far on this thread.'''
        self._state.streams = []
        self._state.last_error = ''

    def _streams(self) -> list:
        # A thread that never called reset() (metadata-only calls) still logs through here.
        if not hasattr(self._state, 'streams'):
            self.reset()
        return self._state.streams

    def lost_fragments(self) -> list[int]:
        '''
        Fragment numbers skipped on this thread since reset() that may hold real media:
        every skip except the final fragment skipped for an empty body.
        '''
        return sorted(index for stream in self._streams() for index in stream['lost'])

    def _observe(self, message) -> None:
        text = str(message)
        streams = self._streams()
        total = _TOTAL_FRAGMENTS.search(text)
        if total:
            # One entry per fragmented download; a merged video+audio fetch has two.
            streams.append({'total': int(total.group(1)), 'lost': []})
            return
        skipped = _SKIPPED_FRAGMENT.search(text)
        if not skipped:
            return
        if not streams:
            # A skip with no "Total fragments" line: the last fragment cannot be known.
            streams.append({'total': None, 'lost': []})
        stream = streams[-1]
        index = int(skipped.group(1))
        # Final fragment AND yt-dlp's last error was the empty-body one. A final fragment
        # that failed for any other reason (403, timeout) is genuinely missing audio.
        empty_trailer = index == stream['total'] and EMPTY_BODY_ERROR in self._state.last_error
        if not empty_trailer:
            stream['lost'].append(index)
        self._state.last_error = ''

    def debug(self, message, *args, **kwargs):
        '''yt-dlp's to_screen() output (progress, "Total fragments", "Skipping fragment").'''
        self._observe(message)
        self._logger.debug(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        '''yt-dlp's report_error() output; remembered so a following skip can be explained.'''
        self._state.last_error = str(message)
        self._logger.error(message, *args, **kwargs)

    def __getattr__(self, name):
        # info, warning, and anything else yt-dlp or a caller asks of a logger.
        return getattr(self._logger, name)
