import pytest

from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage

from tests.helpers import fake_source_dict, generate_fake_context

@pytest.mark.asyncio
async def test_media_request_basics():
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    assert x.download_file is True

    assert str(x) == x.search_string
    x_direct = fake_source_dict(fake_context, is_direct_search=True)
    assert str(x_direct) == f'<{x_direct.search_string}>'

@pytest.mark.asyncio
async def test_media_request_bundle_single():
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id)
    b.add_media_request(x)
    assert b.print()[0] == f'Media request queued for download: "{x.original_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    assert b.print()[0] == f'Downloading and processing media request: "{x.original_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    assert not b.print()

@pytest.mark.asyncio
async def test_media_request_bundle():
    fake_context = generate_fake_context()
    multi_input_search_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)
    x.multi_input_search_string = multi_input_search_string
    y.multi_input_search_string = multi_input_search_string
    z.multi_input_search_string = multi_input_search_string


    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id)
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)

    assert x.bundle_uuid == b.uuid
    assert b.finished is False

    assert b.print()[0] == f'Downloading "<https://foo.example.com/playlist>"\n0/3 items downloaded successfully, 0 failed\nMedia request queued for download: "{x.original_search_string}"\nMedia request queued for download: "{y.original_search_string}"\nMedia request queued for download: "{z.original_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.IN_PROGRESS)
    assert b.print()[0] == f'Downloading "<https://foo.example.com/playlist>"\n0/3 items downloaded successfully, 0 failed\nDownloading and processing media request: "{x.original_search_string}"\nMedia request queued for download: "{y.original_search_string}"\nMedia request queued for download: "{z.original_search_string}"'

    b.update_request_status(x, MediaRequestLifecycleStage.COMPLETED)
    b.update_request_status(y, MediaRequestLifecycleStage.IN_PROGRESS)
    assert b.print()[0] == f'Downloading "<https://foo.example.com/playlist>"\n1/3 items downloaded successfully, 0 failed\nDownloading and processing media request: "{y.original_search_string}"\nMedia request queued for download: "{z.original_search_string}"'

    b.update_request_status(y, MediaRequestLifecycleStage.FAILED, failure_reason='cats ate the chords')
    b.update_request_status(z, MediaRequestLifecycleStage.IN_PROGRESS)
    assert b.print()[0] == f'Downloading "<https://foo.example.com/playlist>"\n1/3 items downloaded successfully, 1 failed\nMedia request failed download: "{y.original_search_string}", cats ate the chords\nDownloading and processing media request: "{z.original_search_string}"'

    b.update_request_status(z, MediaRequestLifecycleStage.COMPLETED)
    assert b.print()[0] == f'Downloading "<https://foo.example.com/playlist>"\n2/3 items downloaded successfully, 1 failed\nMedia request failed download: "{y.original_search_string}", cats ate the chords'
    assert b.finished is True


@pytest.mark.asyncio
async def test_media_request_bundle_multi_message():
    fake_context = generate_fake_context()
    multi_input_search_string = 'https://foo.example.com/playlist'
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    z = fake_source_dict(fake_context)
    x.multi_input_search_string = multi_input_search_string
    y.multi_input_search_string = multi_input_search_string
    z.multi_input_search_string = multi_input_search_string


    b = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, items_per_message=2)
    b.add_media_request(x)
    b.add_media_request(y)
    b.add_media_request(z)
    assert b.finished is False

    assert b.print()[0] == 'Downloading "<https://foo.example.com/playlist>"\n0/3 items downloaded successfully, 0 failed'
    assert b.print()[1] == f'Media request queued for download: "{x.original_search_string}"\nMedia request queued for download: "{y.original_search_string}"'
    assert b.print()[2] == f'Media request queued for download: "{z.original_search_string}"'
