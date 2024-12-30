from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict

def test_source_dict_basics():
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    assert x.download_file is True
    assert not x.video_non_exist_callback_functions
    x.set_message('foo message')
    assert x.message == 'foo message'
    assert str(x) == 'foo bar video'
