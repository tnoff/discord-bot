"""The music commands run in the commands pod; the stubs on the cog only forward them."""
from unittest.mock import AsyncMock

import pytest

from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.cogs.music import Music
from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_engine, fake_context  # pylint: disable=unused-import
from tests.helpers import attach_in_process_broker


@pytest.mark.asyncio
@pytest.mark.parametrize('stub,args,kwargs,command,forwarded', [
    ('connect_', (), {}, 'join', {}),
    ('play_', (), {'search': 'a song'}, 'play', {'search': 'a song'}),
    ('skip_', (), {}, 'skip', {}),
    ('clear', (), {}, 'clear', {}),
    ('history_', (), {}, 'history', {}),
    ('shuffle_', (), {}, 'shuffle', {}),
    ('remove_item', (3,), {}, 'remove', {'queue_index': 3}),
    ('bump_item', ('3',), {}, 'bump', {'queue_index': '3'}),
    ('stop_', (), {}, 'stop', {}),
    ('move_messages_here', (), {}, 'move-messages', {}),
    ('playlist_random_play', (), {}, 'random-play', {}),
    ('music_stats', (), {}, 'music-stats', {}),
])
async def test_music_stubs_forward_to_the_commands_pod(fake_context, stub, args, kwargs, command, forwarded):  # pylint: disable=redefined-outer-name
    '''The behaviour of these commands is tested in discord_commands; this checks only that each
    stub forwards its command name and parsed arguments, and runs nothing itself.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.commands_client = AsyncMock()
    await getattr(cog, stub).callback(cog, fake_context['context'], *args, **kwargs)
    cog.commands_client.forward.assert_awaited_once()
    forward_args = cog.commands_client.forward.await_args.args
    assert forward_args[:3] == (command, envelope_from_ctx(fake_context['context']), forwarded)
