from unittest.mock import MagicMock, AsyncMock

import pytest
from discord_gateway.cogs.music import Music
from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG

from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import attach_in_process_broker


# ---------------------------------------------------------------------------
# Coverage gap tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_playlist_group_no_subcommand(fake_context):  #pylint:disable=redefined-outer-name
    """playlist group fires 'Invalid sub command' when invoked without a subcommand"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    fake_context['context'].invoked_subcommand = None
    await cog.playlist.callback(cog, fake_context['context'])
    assert 'Invalid sub command' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
@pytest.mark.parametrize('stub,args,kwargs,command,forwarded', [
    ('playlist_create', (), {'name': 'mine'}, 'playlist.create', {'name': 'mine'}),
    ('playlist_list', (), {}, 'playlist.list', {}),
    ('playlist_show', (3,), {}, 'playlist.show', {'playlist_index': 3}),
    ('playlist_delete', (3,), {}, 'playlist.delete', {'playlist_index': 3}),
    ('playlist_rename', (3,), {'playlist_name': 'new'}, 'playlist.rename', {'playlist_index': 3, 'playlist_name': 'new'}),
    ('playlist_merge', ('1', '2'), {}, 'playlist.merge', {'playlist_index_one': '1', 'playlist_index_two': '2'}),
    ('playlist_item_remove', (3, 4), {}, 'playlist.item-remove', {'playlist_index': 3, 'video_index': 4}),
    ('playlist_item_add', ('2',), {'search': 'a song'}, 'playlist.item-add', {'playlist_index': '2', 'search': 'a song'}),
    ('playlist_queue_save', (), {'name': 'mine'}, 'playlist.save-queue', {'name': 'mine'}),
    ('playlist_history_save', (), {'name': 'mine'}, 'playlist.save-history', {'name': 'mine'}),
    ('playlist_queue', (3, 'shuffle', 16), {}, 'playlist.queue', {'playlist_index': 3, 'args': ['shuffle', '16']}),
    ('playlist_queue', (3,), {}, 'playlist.queue', {'playlist_index': 3, 'args': []}),
])
async def test_playlist_stubs_forward_to_the_commands_pod(fake_context, stub, args, kwargs, command, forwarded):  #pylint:disable=redefined-outer-name
    '''The behaviour of these commands is tested in discord_commands; this checks only that each
    stub forwards its command name and parsed arguments, and runs nothing itself.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.commands_client = AsyncMock()
    await getattr(cog, stub).callback(cog, fake_context['context'], *args, **kwargs)
    cog.commands_client.forward.assert_awaited_once()
    forward_args = cog.commands_client.forward.await_args.args
    assert (forward_args[0], forward_args[1], forward_args[2]) == (
        command, envelope_from_ctx(fake_context['context']), forwarded)
