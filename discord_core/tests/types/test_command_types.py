import pytest
from pydantic import ValidationError

from discord_core.types.command_args import COMMAND_ARGS, MarkovSpeakArgs, NoArgs, PlaylistItemAddArgs
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import (
    CommandAcceptedResponse, CommandRequestBody, UnknownCommand, parse_args,
)

FULL = CommandEnvelope(
    guild_id=1, channel_id=2, author_id=3, author_display_name='tyler', channel_name='music',
    author_voice_channel_id=4, author_voice_channel_name='vc', author_voice_guild_id=1,
    author_role_ids=(7, 8))


def test_an_envelope_survives_the_wire():
    '''To JSON and back is the same envelope: the role ids are a list on the wire, a tuple here.'''
    wire = FULL.model_dump_json()
    assert '"author_role_ids":[7,8]' in wire
    assert CommandEnvelope.model_validate_json(wire) == FULL
    assert CommandEnvelope.model_validate_json(wire).author_role_ids == (7, 8)


def test_an_envelope_is_immutable():
    with pytest.raises(ValidationError):
        FULL.guild_id = 99


def test_an_envelope_with_only_the_required_fields_gets_the_defaults():
    envelope = CommandEnvelope(guild_id=1, channel_id=2, author_id=3, author_display_name='tyler')
    assert not envelope.author_role_ids
    assert envelope.author_voice_channel_id is None


def test_every_command_takes_a_model_that_forbids_unknown_fields():
    """A stub sending an argument the pod does not know fails on the first call."""
    for model in COMMAND_ARGS.values():
        with pytest.raises(ValidationError):
            model.model_validate({'surprise': 1})


def test_the_commands_are_the_ones_the_gateway_registers():
    """Pinned: adding or renaming a command is a wire change and should be deliberate."""
    assert len(COMMAND_ARGS) == 36
    assert {name for name in COMMAND_ARGS if '.' in name} == {
        'markov.on', 'markov.off', 'markov.list-channels', 'markov.speak',
        'playlist.create', 'playlist.list', 'playlist.item-add', 'playlist.item-remove',
        'playlist.show', 'playlist.delete', 'playlist.rename', 'playlist.save-queue',
        'playlist.save-history', 'playlist.queue', 'playlist.merge',
        'role.list', 'role.users', 'role.available', 'role.add', 'role.remove'}


def test_parse_args_fills_declared_defaults():
    args = parse_args('markov.speak', {})
    assert isinstance(args, MarkovSpeakArgs)
    assert (args.first_word, args.sentence_length) == ('', 32)


def test_parse_args_coerces_like_the_dispatch_bodies():
    assert parse_args('playlist.show', {'playlist_index': '3'}).playlist_index == 3


def test_parse_args_keeps_string_indexes_as_strings():
    args = parse_args('playlist.item-add', {'playlist_index': '2', 'search': 'a song'})
    assert isinstance(args, PlaylistItemAddArgs)
    assert args.playlist_index == '2'


def test_parse_args_for_a_command_without_arguments():
    assert isinstance(parse_args('skip', {}), NoArgs)


def test_parse_args_rejects_a_missing_argument():
    with pytest.raises(ValidationError):
        parse_args('play', {})


def test_parse_args_rejects_an_unknown_command():
    with pytest.raises(UnknownCommand):
        parse_args('not-a-command', {})


def test_a_request_body_needs_an_envelope_and_a_request_id():
    with pytest.raises(ValidationError):
        CommandRequestBody(args={})  # pylint: disable=missing-kwoa
    body = CommandRequestBody(envelope=FULL, request_id='r-1')
    assert not body.args and body.span_context is None


def test_the_accepted_response_defaults_to_not_a_duplicate():
    assert CommandAcceptedResponse().model_dump() == {'status': 'ok', 'duplicate': False}
