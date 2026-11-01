'''load_cogs: instantiate each cog class, skipping the ones that opt out.'''
from discord_core.exceptions import CogMissingRequiredArg

from discord_gateway.cli._lib.cog_registry import POSSIBLE_COGS, load_cogs


class _Enabled:
    '''A cog that always loads, recording how it was built.'''

    def __init__(self, bot, settings, dispatcher, redis_manager=None):
        self.args = (bot, settings, dispatcher, redis_manager)


class _OptedOut:
    '''A cog that opts out the way a disabled cog does.'''

    def __init__(self, *_args, **_kwargs):
        raise CogMissingRequiredArg('not enabled')


def test_load_cogs_builds_each_cog_with_the_same_arguments():
    '''Every cog gets the bot, settings, dispatcher and redis manager, and nothing else'''
    cogs = load_cogs('bot', [_Enabled, _Enabled], {'general': {}}, 'dispatcher', redis_manager='redis')
    assert [cog.args for cog in cogs] == [('bot', {'general': {}}, 'dispatcher', 'redis')] * 2


def test_load_cogs_skips_a_cog_that_opts_out():
    '''A cog raising CogMissingRequiredArg is left out and the rest still load'''
    cogs = load_cogs('bot', [_OptedOut, _Enabled], {}, 'dispatcher')
    assert len(cogs) == 1
    assert isinstance(cogs[0], _Enabled)


def test_the_registry_lists_every_gateway_cog():
    '''The five cogs the gateway can load'''
    assert sorted(cog.__name__ for cog in POSSIBLE_COGS) == [
        'General', 'Markov', 'Music', 'RoleAssignment', 'UrbanDictionary']
