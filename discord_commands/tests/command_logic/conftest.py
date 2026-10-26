import pytest

from discord_core.types.command_envelope import CommandEnvelope


@pytest.fixture
def envelope():
    return CommandEnvelope(guild_id=11, channel_id=22, author_id=33, author_display_name='Tyler')


@pytest.fixture
def sent():
    '''Records (guild_id, channel_id, content) for each reply.'''
    return []


@pytest.fixture
def delete_afters():
    '''The delete_after each reply was sent with, in the order of ``sent``.'''
    return []


@pytest.fixture
def reply(sent, delete_afters):  #pylint:disable=redefined-outer-name
    async def _reply(guild_id, channel_id, content, delete_after=None):
        sent.append((guild_id, channel_id, content))
        delete_afters.append(delete_after)
        return content
    return _reply
