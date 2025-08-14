from functools import partial

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageLifecycleStage, MessageType

from tests.helpers import FakeContext, FakeMessage, fake_source_dict, generate_fake_context

def test_message_send_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.update_single_mutable(x, MessageLifecycleStage.SEND, c.send, 'Original message content')
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND

def test_message_send_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.update_single_mutable(x, MessageLifecycleStage.SEND, c.send, 'Original message content')
    mq.update_single_mutable(x, MessageLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None

def test_message_send_to_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    mq.update_single_mutable(x, MessageLifecycleStage.SEND, c.send, 'Original message content')
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    mq.update_single_mutable(x, MessageLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None

def test_message_edit_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    mes = FakeMessage()
    x.set_message(mes)
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Second edited content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Second edited content'

def test_message_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    mes = FakeMessage()
    x.set_message(mes)
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x, MessageLifecycleStage.DELETE, x.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result.message_content == ''
    assert result.lifecycle_stage == MessageLifecycleStage.DELETE

def test_single_message():
    mq = MessageQueue()
    c = FakeContext()
    func = partial(c.send, 'Sending test message')
    mq.send_single_immutable(func)
    result = mq.get_single_immutable()
    assert result == func

def test_multiple_send_messages_return_order():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    c = FakeContext()
    mq.update_single_mutable(x, MessageLifecycleStage.SEND, c.send, 'First message content', delete_after=5)
    mq.update_single_mutable(y, MessageLifecycleStage.SEND, c.send, 'Second message content')
    mq.update_single_mutable(x, MessageLifecycleStage.EDIT, x.edit_message, 'Edited message content')
    result = mq.get_next_single_mutable()
    print(result)
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Second message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND

def test_player_order():
    mq = MessageQueue()
    mq.update_multiple_mutable('1234')
    mq.update_multiple_mutable('2345')
    mq.update_multiple_mutable('1234')
    assert '1234' == mq.get_next_multiple_mutable()
    assert '2345' == mq.get_next_multiple_mutable()
    assert mq.get_next_multiple_mutable() is None

def test_return_order():
    mq = MessageQueue()
    mq.update_multiple_mutable('1234')
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    c = FakeContext()
    func = partial(c.send, 'Sending test message')
    mq.update_single_mutable(x, MessageLifecycleStage.SEND, c.send, 'First message content', delete_after=5)
    mq.send_single_immutable([func])
    assert mq.get_next_message() == (MessageType.MULTIPLE_MUTABLE, '1234')
    type, result = mq.get_next_message()
    assert type == MessageType.SINGLE_MUTABLE
    assert result.message_content == 'First message content'
    type, funcs = mq.get_next_message()
    assert type == MessageType.SINGLE_IMMUTABLE
    assert funcs == [func]
    type, item = mq.get_next_message()
    assert type is None
    assert item is None
