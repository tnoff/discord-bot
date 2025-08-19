import pytest

from tests.helpers import fake_source_dict, generate_fake_context

@pytest.mark.asyncio
async def test_media_request_basics():
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    assert x.download_file is True
    assert not x.video_non_exist_callback_functions

    assert str(x) == x.search_string
    x_direct = fake_source_dict(fake_context, is_direct_search=True)
    assert str(x_direct) == f'<{x_direct.search_string}>'
