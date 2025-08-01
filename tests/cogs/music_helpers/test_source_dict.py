import pytest

from tests.helpers import FakeMessage, fake_source_dict, generate_fake_context

@pytest.mark.asyncio
async def test_source_dict_basics():
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    assert x.download_file is True
    assert not x.video_non_exist_callback_functions
    assert await x.edit_message('foo') is False
    assert await x.delete_message('') is False

    message = FakeMessage()
    x.set_message(message)
    assert x.message.content == 'fake message content that was typed by a real human'
    assert await x.edit_message('foo bar') is True
    assert await x.delete_message('') is True

    assert str(x) == x.search_string
    x_direct = fake_source_dict(fake_context, is_direct_search=True)
    assert str(x_direct) == f'<{x_direct.search_string}>'
