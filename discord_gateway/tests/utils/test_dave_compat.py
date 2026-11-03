'''The davey 0.2 compatibility patch, against stand-ins for discord.py's voice classes.'''
import struct
from types import SimpleNamespace

import davey
import pytest

from discord_gateway.utils import dave_compat
from discord_gateway.utils.dave_compat import CompatDaveSession, DropFrame


class FakeSession:
    '''Records the davey 0.2 calls the patch makes.'''
    protocol_version = 1
    can_encrypt = True

    def __init__(self, commit_accepted=True, execute_result=True):
        self.calls = []
        self.commit_accepted = commit_accepted
        self.execute_result = execute_result
        self.fail_prepare = False

    def process_commit(self, commit):
        self.calls.append(('process_commit', commit))
        return self.commit_accepted

    def process_welcome(self, welcome):
        self.calls.append(('process_welcome', welcome))

    def prepare_transition(self, transition_id, protocol_version):
        if self.fail_prepare:
            raise RuntimeError('no pending group')
        self.calls.append(('prepare_transition', transition_id, protocol_version))

    def execute_transition(self, transition_id):
        self.calls.append(('execute_transition', transition_id))
        return self.execute_result

    def encrypt_opus(self, packet):
        return b'enc:' + packet

    def reset(self):
        self.calls.append(('reset',))


class OldWs:
    '''A voice websocket whose received_message does not mention the 0.2 calls.'''
    DAVE_PREPARE_TRANSITION = 21
    MLS_ANNOUNCE_COMMIT_TRANSITION = 29
    MLS_WELCOME = 30

    def __init__(self, session):
        self._connection = SimpleNamespace(dave_session=session)
        self.seen = []

    async def received_message(self, msg):
        self.seen.append(('message', msg['op']))

    async def received_binary_message(self, msg):
        self.seen.append(('binary', msg[2]))


class NewWs(OldWs):
    '''A voice websocket that already prepares transitions itself, like the upstream fix.'''
    async def received_message(self, msg):
        self._connection.dave_session.prepare_transition(msg['d']['transition_id'], 1)


class State:
    '''A voice connection state with the two methods the patch wraps.'''
    def __init__(self, session=None):
        self.dave_session = session
        self.dave_pending_transitions = {}
        self.executed = []

    async def reinit_dave_session(self):
        if self.dave_session is None:
            self.dave_session = FakeSession()

    async def _execute_transition(self, transition_id):
        self.executed.append(transition_id)


class Client:
    '''A voice client with the send path the patch wraps.'''
    def __init__(self, raises=None):
        self.raises = raises
        self.sent = []
        self.timestamp_steps = []

    def send_audio_packet(self, data, *, encode=True):
        if self.raises:
            raise self.raises
        self.sent.append((data, encode))

    def checked_add(self, attr, value, limit):
        self.timestamp_steps.append((attr, value, limit))


async def execute(state, transition_id):
    '''Run the state's execute step; reached by name, as the patch does'''
    await getattr(state, '_execute_transition')(transition_id)


def make_classes():
    '''Fresh stand-ins, because apply() patches the classes it is given.'''
    ws = type('Ws', (OldWs,), {})
    state = type('State', (State,), {})
    client = type('Client', (Client,), {})
    return ws, state, client


def apply_to(ws, state, client):
    return dave_compat.apply(ws_cls=ws, state_cls=state, client_cls=client,
                             session_cls=FakeSession)


def test_needs_compat_is_false_for_davey_without_the_new_calls():
    '''davey 0.1 has no prepare_transition, so there is nothing to bridge'''
    assert dave_compat.needs_compat(OldWs, object) is False


def test_needs_compat_is_false_once_discordpy_prepares_transitions_itself():
    '''A discord.py that already calls prepare_transition is left alone'''
    assert dave_compat.needs_compat(NewWs, FakeSession) is False


def test_needs_compat_is_true_for_old_discordpy_with_new_davey():
    '''The broken pairing: davey with prepare_transition, discord.py without the call'''
    assert dave_compat.needs_compat(OldWs, FakeSession) is True


def test_apply_patches_once():
    '''A second apply on the same classes does nothing'''
    ws, state, client = make_classes()
    assert apply_to(ws, state, client) is True
    assert apply_to(ws, state, client) is False


def test_apply_skips_when_not_needed():
    '''No patch for davey 0.1'''
    ws, state, client = make_classes()
    assert dave_compat.apply(ws_cls=ws, state_cls=state, client_cls=client, session_cls=object) is False


@pytest.mark.asyncio
async def test_reinit_wraps_the_session_once():
    '''The session discord.py builds is wrapped, and a second reinit does not wrap it again'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    state = state_cls()
    await state.reinit_dave_session()
    wrapped = state.dave_session
    assert isinstance(wrapped, CompatDaveSession)
    await state.reinit_dave_session()
    assert state.dave_session is wrapped


@pytest.mark.asyncio
async def test_reinit_with_no_session_leaves_none():
    '''A protocol version 0 reinit that builds no session wraps nothing'''
    ws, state_cls, client = make_classes()

    async def reinit_without_session(self):
        self.dave_session = None
    state_cls.reinit_dave_session = reinit_without_session
    apply_to(ws, state_cls, client)
    state = state_cls()
    await state.reinit_dave_session()
    assert state.dave_session is None


def test_commit_prepares_the_transition_it_announced():
    '''process_commit then prepare_transition with the id from the frame header'''
    fake = FakeSession()
    session = CompatDaveSession(fake)
    session.transition_id = 7
    assert session.process_commit(b'commit') is True
    assert fake.calls == [('process_commit', b'commit'), ('prepare_transition', 7, 1)]


def test_ignored_commit_prepares_nothing():
    '''A commit davey ignores has no transition to prepare'''
    fake = FakeSession(commit_accepted=False)
    session = CompatDaveSession(fake)
    session.transition_id = 7
    assert session.process_commit(b'commit') is False
    assert fake.calls == [('process_commit', b'commit')]


def test_welcome_prepares_the_transition_it_announced():
    '''process_welcome then prepare_transition'''
    fake = FakeSession()
    session = CompatDaveSession(fake)
    session.transition_id = 3
    session.process_welcome(b'welcome')
    assert fake.calls == [('process_welcome', b'welcome'), ('prepare_transition', 3, 1)]


def test_other_session_calls_pass_through():
    '''Anything the wrapper does not define reaches the real session'''
    fake = FakeSession()
    CompatDaveSession(fake).reset()
    assert fake.calls == [('reset',)]


def test_encrypt_opus_encrypts_when_there_is_a_key():
    '''With a key the frame is encrypted by the real session'''
    assert CompatDaveSession(FakeSession()).encrypt_opus(b'x') == b'enc:x'


def test_encrypt_opus_drops_the_frame_without_a_key():
    '''A real davey session that has no key refuses to send the frame in the clear'''
    session = CompatDaveSession(davey.DaveSession(davey.DAVE_PROTOCOL_VERSION, 1, 2))
    with pytest.raises(DropFrame):
        session.encrypt_opus(b'x')


@pytest.mark.asyncio
async def test_binary_frame_sets_the_transition_id_before_discordpy_reads_it():
    '''Commit and welcome frames carry the id in bytes 3-4; the session learns it first'''
    ws_cls, state, client = make_classes()
    apply_to(ws_cls, state, client)
    session = CompatDaveSession(FakeSession())
    ws = ws_cls(session)
    frame = struct.pack('>HB', 1, ws_cls.MLS_ANNOUNCE_COMMIT_TRANSITION) + struct.pack('>H', 42) + b'payload'
    await ws.received_binary_message(frame)
    assert session.transition_id == 42
    assert ws.seen == [('binary', ws_cls.MLS_ANNOUNCE_COMMIT_TRANSITION)]


@pytest.mark.asyncio
async def test_other_binary_frames_leave_the_transition_id_alone():
    '''Only commit and welcome frames name a transition'''
    ws_cls, state, client = make_classes()
    apply_to(ws_cls, state, client)
    session = CompatDaveSession(FakeSession())
    ws = ws_cls(session)
    await ws.received_binary_message(struct.pack('>HB', 1, 25) + b'xx')
    assert session.transition_id is None


@pytest.mark.asyncio
async def test_prepare_op_prepares_the_transition_in_davey():
    '''Opcode 21 prepares the transition in davey, then discord.py handles it as before'''
    ws_cls, state, client = make_classes()
    apply_to(ws_cls, state, client)
    fake = FakeSession()
    ws = ws_cls(CompatDaveSession(fake))
    await ws.received_message({'op': 21, 'd': {'transition_id': 5, 'protocol_version': 1}})
    assert ('prepare_transition', 5, 1) in fake.calls
    assert ws.seen == [('message', 21)]


@pytest.mark.asyncio
async def test_prepare_op_failure_is_logged_and_discordpy_still_runs():
    '''davey refusing the prepare must not stop discord.py handling the message'''
    ws_cls, state, client = make_classes()
    apply_to(ws_cls, state, client)
    fake = FakeSession()
    fake.fail_prepare = True
    ws = ws_cls(CompatDaveSession(fake))
    await ws.received_message({'op': 21, 'd': {'transition_id': 5, 'protocol_version': 1}})
    assert ws.seen == [('message', 21)]


@pytest.mark.asyncio
async def test_other_ops_are_untouched():
    '''Only opcode 21 reaches davey'''
    ws_cls, state, client = make_classes()
    apply_to(ws_cls, state, client)
    fake = FakeSession()
    ws = ws_cls(CompatDaveSession(fake))
    await ws.received_message({'op': 6, 'd': {}})
    assert not fake.calls
    assert ws.seen == [('message', 6)]


@pytest.mark.asyncio
async def test_execute_switches_the_key_then_discordpy_records_it():
    '''A pending transition is executed in davey before discord.py updates its version'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    fake = FakeSession()
    state = state_cls(CompatDaveSession(fake))
    state.dave_pending_transitions[9] = 1
    await execute(state, 9)
    assert ('execute_transition', 9) in fake.calls
    assert state.executed == [9]


@pytest.mark.asyncio
async def test_execute_of_transition_zero_is_left_to_davey():
    '''Transition 0 is executed by davey itself'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    fake = FakeSession()
    state = state_cls(CompatDaveSession(fake))
    state.dave_pending_transitions[0] = 1
    await execute(state, 0)
    assert not fake.calls
    assert state.executed == [0]


@pytest.mark.asyncio
async def test_execute_davey_refusal_drops_the_pending_transition():
    '''davey not executing the transition leaves discord.py's version unchanged'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    state = state_cls(CompatDaveSession(FakeSession(execute_result=False)))
    state.dave_pending_transitions[9] = 1
    await execute(state, 9)
    assert not state.executed
    assert 9 not in state.dave_pending_transitions


@pytest.mark.asyncio
async def test_execute_error_is_logged_and_treated_as_not_executed():
    '''An exception from davey is handled like a refusal'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    fake = FakeSession()

    def explode(_transition_id):
        raise RuntimeError('no transition')
    fake.execute_transition = explode
    state = state_cls(CompatDaveSession(fake))
    state.dave_pending_transitions[9] = 1
    await execute(state, 9)
    assert not state.executed
    assert 9 not in state.dave_pending_transitions


@pytest.mark.asyncio
async def test_execute_without_a_wrapped_session_is_untouched():
    '''No DAVE session (an unencrypted channel) means discord.py runs as before'''
    ws, state_cls, client = make_classes()
    apply_to(ws, state_cls, client)
    state = state_cls(None)
    await execute(state, 9)
    assert state.executed == [9]


def test_send_audio_packet_passes_through_when_the_frame_is_sent():
    '''A normal frame goes out unchanged'''
    ws, state, client_cls = make_classes()
    apply_to(ws, state, client_cls)
    client = client_cls()
    client.send_audio_packet(b'opus', encode=False)
    assert client.sent == [(b'opus', False)]
    assert not client.timestamp_steps


def test_send_audio_packet_drops_a_frame_that_cannot_be_encrypted_yet():
    '''The frame is dropped and the timestamp still advances one frame'''
    ws, state, client_cls = make_classes()
    apply_to(ws, state, client_cls)
    client = client_cls(raises=DropFrame())
    client.send_audio_packet(b'opus')
    assert not client.sent
    assert client.timestamp_steps == [('timestamp', 960, 4294967295)]
