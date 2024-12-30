import pytest

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict

from tests.helpers import FakeMessage

@pytest.mark.asyncio
async def test_source_dict_basics():
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    assert x.download_file is True
    assert not x.video_non_exist_callback_functions
    assert await x.edit_message('foo') is False
    assert await x.delete_message('') is False

    message = FakeMessage()
    x.set_message(message)
    assert x.message.content == 'fake message content that was typed by a real human'
    assert await x.edit_message('foo bar') is True
    assert await x.delete_message('') is True

    assert str(x) == 'foo bar video'
    x = SourceDict('1234', 'foobar', '2345', 'https://example.com', SearchType.SEARCH)
    assert str(x) == '<https://example.com>'
