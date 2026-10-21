from discord_core.types.command_args import COMMAND_ARGS

from discord_commands.handlers import build_handlers


def test_the_skeleton_serves_no_commands_yet():
    assert not build_handlers()


def test_every_handler_is_a_command_the_contract_knows():
    """Guards the day handlers arrive: a typo'd name would never be called, and never fail."""
    assert set(build_handlers()) <= set(COMMAND_ARGS)
