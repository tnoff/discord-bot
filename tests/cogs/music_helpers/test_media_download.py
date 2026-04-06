from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from discord_bot.cogs.music_helpers.media_broker import MediaBroker

from tests.helpers import fake_media_download, generate_fake_context

@pytest.mark.asyncio
async def test_media_download_checkout_copies_to_guild_path():
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        with fake_media_download(tmp_dir, fake_context=fake_context) as x:
            original_file_path = x.file_path
            broker = MediaBroker()
            await broker.register_download(x)
            guild_path = Path(tmp_dir) / str(fake_context['guild'].id)
            guild_file_path = broker.checkout(x.media_request.uuid, fake_context['guild'].id, guild_path)
            assert str(x) == x.webpage_url  # pylint: disable=no-member
            assert guild_file_path is not None
            assert str(guild_file_path) != str(original_file_path)
            assert f'/{fake_context["guild"].id}/' in str(guild_file_path)
            assert x.file_path == original_file_path  # base path unchanged
            broker.release(x.media_request.uuid)
            assert not guild_file_path.exists()
            assert original_file_path.exists()
