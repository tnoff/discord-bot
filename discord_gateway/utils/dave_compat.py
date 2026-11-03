'''
Keep voice audible when davey 0.2 meets a discord.py that was written for davey 0.1.

davey 0.2.0 changed the DAVE session API in a way discord.py 2.7.1 does not follow
(Rapptz/discord.py#10548 is the open upstream fix):

* ``process_commit`` / ``process_welcome`` no longer install the new keys. The caller must
  ``prepare_transition(transition_id, protocol_version)`` afterwards, and the key used to encrypt
  only switches when the transition is executed with ``execute_transition``.
* After a ``reinit`` the session stays ``ready`` while it still has no key to encrypt with.

discord.py 2.7.1 does none of that, so the gateway joined voice, never got a key, and sent no
audio. This module closes the gap from our side, without touching discord.py's files:

* the session discord.py builds is wrapped, so ``process_commit`` / ``process_welcome`` prepare
  the transition they just processed (the websocket wrapper tells it which id that is);
* opcode 21 (prepare) and the execute step call into davey the way libdave's sample does;
* a frame is dropped, not sent in the clear, while the session has no key yet.

The user-id rosters davey 0.2 can check are left off (``None`` means "do not check"), which is
exactly what 0.1 did. This process only sends audio, it never decrypts anyone else's.

``apply`` does nothing when davey is older than 0.2 or when discord.py already calls
``prepare_transition`` itself, so this module can be deleted once a discord.py release supports
davey 0.2.
'''
import functools
import inspect
import logging
import struct

import davey
from discord import opus
from discord.gateway import DiscordVoiceWebSocket
from discord.voice_client import VoiceClient
from discord.voice_state import VoiceConnectionState

_log = logging.getLogger('main')

_APPLIED_FLAG = '_dave_compat_applied'
# discord.py's own name for it; reached by name so this module needs no protected access.
_EXECUTE_TRANSITION = '_execute_transition'
_CONNECTION = '_connection'
_TIMESTAMP_MAX = 4294967295


class DropFrame(Exception):
    '''The session has no key to encrypt with yet; this audio frame is dropped, not sent in the clear.'''


class CompatDaveSession:
    '''A davey 0.2 session that answers discord.py 2.7.1's davey 0.1 calls.

    Everything not defined here is delegated to the wrapped session.
    '''

    def __init__(self, session):
        self._session = session
        # Set by the websocket wrapper from the frame header just before discord.py hands us the
        # commit or welcome payload.
        self.transition_id: int | None = None

    def __getattr__(self, name):
        return getattr(self._session, name)

    def _prepare_transition(self) -> None:
        self._session.prepare_transition(self.transition_id, self._session.protocol_version)

    def process_commit(self, commit: bytes) -> bool:
        '''Process the commit, then prepare the transition it announced.'''
        accepted = self._session.process_commit(commit)
        if accepted:
            self._prepare_transition()
        else:
            _log.debug('DAVE commit ignored for transition id %s', self.transition_id)
        return accepted

    def process_welcome(self, welcome: bytes) -> None:
        '''Process the welcome, then prepare the transition it announced.'''
        self._session.process_welcome(welcome)
        self._prepare_transition()

    def encrypt_opus(self, packet: bytes) -> bytes:
        '''Encrypt a frame, or raise DropFrame while there is no key to encrypt with.'''
        if not self._session.can_encrypt:
            raise DropFrame()
        return self._session.encrypt_opus(packet)


def needs_compat(ws_cls=DiscordVoiceWebSocket, session_cls=davey.DaveSession) -> bool:
    '''True when davey is 0.2 or newer and discord.py has not been taught about it.'''
    if not hasattr(session_cls, 'prepare_transition'):
        return False
    return 'prepare_transition' not in inspect.getsource(ws_cls.received_message)


def _wrap_reinit(original):
    @functools.wraps(original)
    async def reinit_dave_session(self):
        await original(self)
        if self.dave_session is not None and not isinstance(self.dave_session, CompatDaveSession):
            self.dave_session = CompatDaveSession(self.dave_session)
    return reinit_dave_session


def _wrap_received_message(original, ws_cls):
    @functools.wraps(original)
    async def received_message(self, msg):
        session = getattr(self, _CONNECTION).dave_session
        if msg['op'] == ws_cls.DAVE_PREPARE_TRANSITION and isinstance(session, CompatDaveSession):
            data = msg['d']
            try:
                session.prepare_transition(data['transition_id'], data['protocol_version'])
            except Exception:
                _log.exception('Failed to prepare DAVE transition id %s', data['transition_id'])
        await original(self, msg)
    return received_message


def _wrap_received_binary_message(original, ws_cls):
    @functools.wraps(original)
    async def received_binary_message(self, msg):
        session = getattr(self, _CONNECTION).dave_session
        if (isinstance(session, CompatDaveSession) and len(msg) >= 5
                and msg[2] in (ws_cls.MLS_ANNOUNCE_COMMIT_TRANSITION, ws_cls.MLS_WELCOME)):
            session.transition_id = struct.unpack_from('>H', msg, 3)[0]
        await original(self, msg)
    return received_binary_message


def _wrap_execute_transition(original):
    @functools.wraps(original)
    async def execute_transition(self, transition_id):
        session = self.dave_session
        # Transition 0 is executed by davey itself.
        if (transition_id != 0 and isinstance(session, CompatDaveSession)
                and transition_id in self.dave_pending_transitions):
            try:
                executed = session.execute_transition(transition_id)
            except Exception:
                _log.exception('Failed to execute DAVE transition id %s', transition_id)
                executed = False
            if not executed:
                self.dave_pending_transitions.pop(transition_id, None)
                _log.warning('DAVE transition id %s was not executed by davey', transition_id)
                return
        await original(self, transition_id)
    return execute_transition


def _wrap_send_audio_packet(original):
    @functools.wraps(original)
    def send_audio_packet(self, data, *, encode=True):
        try:
            original(self, data, encode=encode)
        except DropFrame:
            # The sequence number already moved before the frame was encrypted; keep the
            # timestamp in step with it so the receiver sees a gap, not a stall.
            self.checked_add('timestamp', opus.Encoder.SAMPLES_PER_FRAME, _TIMESTAMP_MAX)
    return send_audio_packet


def apply(ws_cls=DiscordVoiceWebSocket, state_cls=VoiceConnectionState, client_cls=VoiceClient,
          session_cls=davey.DaveSession) -> bool:
    '''Patch discord.py's voice classes for davey 0.2. Returns True if it patched anything.

    Safe to call more than once. The classes are parameters so a test can patch stand-ins
    instead of discord.py's own.
    '''
    if getattr(state_cls, _APPLIED_FLAG, False) or not needs_compat(ws_cls, session_cls):
        return False
    state_cls.reinit_dave_session = _wrap_reinit(state_cls.reinit_dave_session)
    setattr(state_cls, _EXECUTE_TRANSITION,
            _wrap_execute_transition(getattr(state_cls, _EXECUTE_TRANSITION)))
    ws_cls.received_message = _wrap_received_message(ws_cls.received_message, ws_cls)
    ws_cls.received_binary_message = _wrap_received_binary_message(ws_cls.received_binary_message, ws_cls)
    client_cls.send_audio_packet = _wrap_send_audio_packet(client_cls.send_audio_packet)
    setattr(state_cls, _APPLIED_FLAG, True)
    _log.info('Main :: Applied the davey 0.2 voice compatibility patch to discord.py')
    return True
