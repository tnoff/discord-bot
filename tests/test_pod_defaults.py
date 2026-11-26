'''
Defaults that two pods each carry a copy of, pinned together.

The broker CLI cannot import the gateway's `cogs.music` (that drags discord.py and yt-dlp into a slim
pod), so it keeps its own copy of a default the gateway defines. A test that reads both has to import
both pods, which is why it lives here, in the one tree that is allowed to, and not in either pod's own.
'''
from discord_broker.cli import broker as broker_cli
from discord_gateway.cogs.music import MusicGeneralConfig


def test_default_message_delete_after_matches_music_config():
    '''The broker CLI default is a hand-copy of MusicGeneralConfig.message_delete_after. Pin them together
    so the copy cannot drift.'''
    assert broker_cli.DEFAULT_MESSAGE_DELETE_AFTER == MusicGeneralConfig().message_delete_after
