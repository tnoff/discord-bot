from functools import partial

from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.common import SearchType

from discord_bot.utils.common import retry_discord_message_command

from tests.helpers import FakeContext

def test_add_basic_queue_message():
    mq = MessageQueue()
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    c = FakeContext()
    func = partial(c.send, f'Sending message for source {str(x)}')
    mq.iterate_source_lifecycle(x, func)
    result = mq.get_source_lifecycle()
    assert result == func
    assert True == False