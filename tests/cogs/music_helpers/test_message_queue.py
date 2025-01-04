from functools import partial

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, SourceLifecycleStage
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.common import SearchType

from discord_bot.utils.common import retry_discord_message_command

from tests.helpers import FakeContext, FakeMessage

def test_message_send_to_edit_override():
    mq = MessageQueue()
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    result = mq.get_source_lifecycle()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_state == SourceLifecycleStage.SEND

def test_message_send_to_delete_override():
    mq = MessageQueue()
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_source_lifecycle()
    assert result is None

def test_message_send_to_edit_to_delete_override():
    mq = MessageQueue()
    x = SourceDict('1234', 'foobar', '2345', 'foo bar video', SearchType.SEARCH)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_source_lifecycle()
    assert result is None