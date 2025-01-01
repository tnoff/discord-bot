from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

def test_source_download_with_cache():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3') as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            y = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            x = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, y)
            x.ready_file()
            assert str(x) == 'https://foo.example'
            assert str(x.file_path) != str(file_path)
            assert '/123/' in str(x.file_path)
            x.delete()
            assert not x.file_path.exists()
            assert file_path.exists()

def test_source_download_without_cache():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, suffix='.mp3', delete=False) as tmp_file:
            file_path = Path(tmp_file.name)
            file_path.write_text('testing', encoding='utf-8')
            y = SourceDict('123', 'foo bar authr', '234', 'foo bar video', SearchType.SEARCH)
            x = SourceDownload(file_path, {'webpage_url': 'https://foo.example'}, y)
            x.ready_file(move_file=True)
            assert str(x) == 'https://foo.example'
            assert str(x.file_path) != str(file_path)
            assert '/123/' in str(x.file_path)
            x.delete()
            assert not x.file_path.exists()
            assert not file_path.exists()
