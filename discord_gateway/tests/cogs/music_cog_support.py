'''Build a Music cog over the in-process doubles, the way the cog tests all start.'''
from unittest.mock import AsyncMock, MagicMock

from discord_gateway.cogs.music import Music

from tests.helpers import attach_in_process_broker, attach_in_process_download, attach_in_process_search


def prepared_cog(mocker, fake_context, config, *, download=False, search=False):
    """A Music cog on the in-process broker (and, if asked, download and search), with no sleeping."""
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    if download:
        attach_in_process_download(cog)
    if search:
        attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    return cog


def forwarding_cog(fake_context, config):
    """A Music cog whose commands client is a mock, to check what a stub forwards."""
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.commands_client = AsyncMock()
    return cog
