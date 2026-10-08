'''
The Discord context attribute names. (The function that reads them off a Context
lives in discord_gateway/utils/otel_command.py; see its tests.)

These replace two tests in test_otel.py that passed a Context into the span
wrappers and then only checked `span is not None` — so the attribute names they
existed to protect were never actually read. Asserting the dict directly is
strictly stronger, and it is what lets the enum live outside `otel.py` at all.
'''
from discord_core.utils.discord_context import DiscordContextNaming


def test_every_context_attribute_name_is_discord_scoped():
    '''
    All five names sit under a `discord.` prefix.

    They describe the chat platform rather than this project, and the prefix is
    what keeps them from colliding with an attribute of the same short name from
    anywhere else in the span.
    '''
    unscoped = sorted(m.value for m in DiscordContextNaming if not m.value.startswith('discord.'))
    assert not unscoped, f'context attributes outside the discord. namespace: {unscoped}'
