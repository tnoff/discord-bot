from functools import partial

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageLifecycleStage, MessageType

from tests.helpers import FakeContext, fake_source_dict, generate_fake_context

def test_message_send_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND

def test_message_send_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None

def test_message_send_to_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None

def test_message_edit_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Second edited content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Second edited content'

def test_message_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None

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
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    message2 = MessageContext(fake_context['guild'], fake_context['channel'])
    y.message_context = message2
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'First message content', delete_after=5)
    mq.update_single_mutable(y.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Second message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, partial(x.message_context.edit_message), 'Edited message content')
    result = mq.get_next_single_mutable()
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
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    func = partial(fake_context['context'].send, 'Sending test message')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'First message content', delete_after=5)
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
