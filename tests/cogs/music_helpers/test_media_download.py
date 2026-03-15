from pathlib import Path
from tempfile import TemporaryDirectory

from discord_bot.cogs.music_helpers.media_broker import MediaBroker

from tests.helpers import fake_media_download, generate_fake_context

def test_media_download_with_cache():
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        with fake_media_download(tmp_dir, fake_context=fake_context) as x:
            original_file_path = x.file_path
            broker = MediaBroker()
            broker.register_download(x)
            guild_path = Path(tmp_dir) / str(fake_context['guild'].id)
            broker.checkout(x.media_request.uuid, fake_context['guild'].id, guild_path)
            assert str(x) == x.webpage_url  # pylint: disable=no-member
            assert str(x.file_path) != str(original_file_path)
            assert f'/{fake_context["guild"].id}/' in str(x.file_path)
            x.delete()
            assert not x.file_path.exists()
            assert original_file_path.exists()
