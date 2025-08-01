from functools import partial

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, SourceLifecycleStage, MessageType

from tests.helpers import FakeContext, FakeMessage, fake_source_dict, generate_fake_context

def test_message_send_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    result = mq.get_source_lifecycle()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == SourceLifecycleStage.SEND

def test_message_send_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_source_lifecycle()
    assert result is None

def test_message_send_to_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'Original message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_source_lifecycle()
    assert result is None

def test_message_edit_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    mes = FakeMessage()
    x.set_message(mes)
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content', delete_after=5)
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Second edited content')
    result = mq.get_source_lifecycle()
    assert result.message_content == 'Second edited content'

def test_message_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    mes = FakeMessage()
    x.set_message(mes)
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content', delete_after=5)
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_source_lifecycle()
    assert result.message_content == ''
    assert result.lifecycle_stage == SourceLifecycleStage.DELETE

def test_single_message():
    mq = MessageQueue()
    c = FakeContext()
    func = partial(c.send, 'Sending test message')
    mq.iterate_single_message(func)
    result = mq.get_single_message()
    assert result == func

def test_multiple_send_messages_return_order():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    c = FakeContext()
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'First message content', delete_after=5)
    mq.iterate_source_lifecycle(y, SourceLifecycleStage.SEND, c.send, 'Second message content')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    result = mq.get_source_lifecycle()
    print(result)
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == SourceLifecycleStage.SEND
    result = mq.get_source_lifecycle()
    assert result.message_content == 'Second message content'
    assert result.lifecycle_stage == SourceLifecycleStage.SEND

def test_player_order():
    mq = MessageQueue()
    mq.iterate_play_order('1234')
    mq.iterate_play_order('2345')
    mq.iterate_play_order('1234')
    assert '1234' == mq.get_play_order()
    assert '2345' == mq.get_play_order()
    assert mq.get_play_order() is None

def test_return_order():
    mq = MessageQueue()
    mq.iterate_play_order('1234')
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    func = partial(c.send, 'Sending test message')
    mq.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, c.send, 'First message content', delete_after=5)
    mq.iterate_single_message([func])
    assert mq.get_next_message() == (MessageType.PLAY_ORDER, '1234')
    type, result = mq.get_next_message()
    assert type == MessageType.SOURCE_LIFECYCLE
    assert result.message_content == 'First message content'
    type, funcs = mq.get_next_message()
    assert type == MessageType.SINGLE_MESSAGE
    assert funcs == [func]
    type, item = mq.get_next_message()
    assert type is None
    assert item is None
