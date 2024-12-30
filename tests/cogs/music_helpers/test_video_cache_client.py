from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.database import BASE, VideoCache
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

def test_verify_cache():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_file:
            with NamedTemporaryFile(suffix='.sql') as temp_db:
                engine = create_engine(f'sqlite:///{temp_db.name}')
                BASE.metadata.create_all(engine)
                BASE.metadata.bind = engine
                session = sessionmaker(bind=engine)()

                x = VideoCacheClient(Path(tmp_dir), 10, session)
                x.verify_cache()

                assert not Path(tmp_file.name).exists()

def test_verify_cache_with_dir():
    with TemporaryDirectory() as tmp_dir:
        with TemporaryDirectory(ignore_cleanup_errors=True, dir=tmp_dir) as tmp_dir2:
            with NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp_file:
                with NamedTemporaryFile(suffix='.sql') as temp_db:
                    engine = create_engine(f'sqlite:///{temp_db.name}')
                    BASE.metadata.create_all(engine)
                    BASE.metadata.bind = engine
                    session = sessionmaker(bind=engine)()

                    x = VideoCacheClient(Path(tmp_dir), 10, session)
                    x.verify_cache()

                    assert not Path(tmp_file.name).exists()
                    assert not Path(tmp_dir2).exists()

def test_verify_cache_with_files_that_no_longer_exist():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(suffix='.mp3', delete=False) as file_path:
                with NamedTemporaryFile(suffix='.mp3', dir=tmp_dir, delete=False) as extra_file:
                    engine = create_engine(f'sqlite:///{temp_db.name}')
                    BASE.metadata.create_all(engine)
                    BASE.metadata.bind = engine
                    session = sessionmaker(bind=engine)()

                    x = VideoCacheClient(Path(tmp_dir), 10, session)
                    sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SEARCH)
                    s = SourceDownload(Path(file_path.name), {
                        'webpage_url': 'https://foo.example.com',
                        'title': 'Foo title',
                        'uploader': 'Foo uploader',
                        'id': '1234',
                        'extractor': 'foo extractor'
                    }, sd)
                    x.iterate_file(s)
                    Path(file_path.name).unlink()
                    x.verify_cache()
                    assert session.query(VideoCache).count() == 0
                    assert not Path(extra_file.name).exists()

def test_iterate_file_new_and_iterate():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(suffix='.mp3') as file_path:
                engine = create_engine(f'sqlite:///{temp_db.name}')
                BASE.metadata.create_all(engine)
                BASE.metadata.bind = engine
                session = sessionmaker(bind=engine)()

                x = VideoCacheClient(Path(tmp_dir), 10, session)
                sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SEARCH)
                s = SourceDownload(Path(file_path.name), {
                    'webpage_url': 'https://foo.example.com',
                    'title': 'Foo title',
                    'uploader': 'Foo uploader',
                    'id': '1234',
                    'extractor': 'foo extractor'
                }, sd)
                x.iterate_file(s)
                x.iterate_file(s)
                assert session.query(VideoCache).count() == 1
                query = session.query(VideoCache).first()
                assert query.count == 2

def test_webpage_get_source():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(suffix='.mp3') as file_path:
                engine = create_engine(f'sqlite:///{temp_db.name}')
                BASE.metadata.create_all(engine)
                BASE.metadata.bind = engine
                session = sessionmaker(bind=engine)()

                x = VideoCacheClient(Path(tmp_dir), 10, session)
                sd = SourceDict('123', 'requester name', '234', 'https://foo.example.com', SearchType.SEARCH)
                s = SourceDownload(Path(file_path.name), {
                    'webpage_url': 'https://foo.example.com',
                    'title': 'Foo title',
                    'uploader': 'Foo uploader',
                    'id': '1234',
                    'extractor': 'foo extractor'
                }, sd)
                x.iterate_file(s)
                result = x.get_webpage_url_item(sd)
                assert result.file_path
                assert result.webpage_url == 'https://foo.example.com' #pylint: disable=no-member

def test_webpage_get_source_non_existing():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            engine = create_engine(f'sqlite:///{temp_db.name}')
            BASE.metadata.create_all(engine)
            BASE.metadata.bind = engine
            session = sessionmaker(bind=engine)()

            x = VideoCacheClient(Path(tmp_dir), 10, session)
            sd = SourceDict('123', 'requester name', '234', 'https://foo.example.com', SearchType.SEARCH)
            result = x.get_webpage_url_item(sd)
            assert result is None

def test_remove():
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(suffix='.mp3', delete=False) as file_path:
                with NamedTemporaryFile(suffix='.mp3') as file_path2:
                    engine = create_engine(f'sqlite:///{temp_db.name}')
                    BASE.metadata.create_all(engine)
                    BASE.metadata.bind = engine
                    session = sessionmaker(bind=engine)()

                    x = VideoCacheClient(Path(tmp_dir), 1, session)
                    sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SEARCH)
                    s = SourceDownload(Path(file_path.name), {
                        'webpage_url': 'https://foo.example.com',
                        'title': 'Foo title',
                        'uploader': 'Foo uploader',
                        'id': '1234',
                        'extractor': 'foo extractor'
                    }, sd)
                    sd2 = SourceDict('123', 'requester name', '234', 'foo bar2', SearchType.SEARCH)
                    t2 = SourceDownload(Path(file_path2.name), {
                        'webpage_url': 'https://foo.example2.com',
                        'title': 'Foo title',
                        'uploader': 'Foo uploader',
                        'id': '1234',
                        'extractor': 'foo extractor'
                    }, sd2)
                    x.iterate_file(s)
                    x.iterate_file(t2)
                    x.ready_remove()

                    assert session.query(VideoCache).count() == 2
                    query = session.query(VideoCache).first()
                    assert query.video_url == 'https://foo.example.com'
                    assert query.ready_for_deletion is True

                    x.remove_video_cache([query.id])
                    assert session.query(VideoCache).count() == 1



def test_search_existing_file():
    test_id = '1234'
    test_extractor = 'foo-extractor'
    with TemporaryDirectory() as tmp_dir:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            with NamedTemporaryFile(prefix=f'{test_extractor}.{test_id}', suffix='.mp3') as file_path:
                engine = create_engine(f'sqlite:///{temp_db.name}')
                BASE.metadata.create_all(engine)
                BASE.metadata.bind = engine
                session = sessionmaker(bind=engine)()

                x = VideoCacheClient(Path(tmp_dir), 10, session)
                sd = SourceDict('123', 'requester name', '234', 'https://foo.example.com', SearchType.SEARCH)
                s = SourceDownload(Path(file_path.name), {
                    'webpage_url': 'https://foo.example.com',
                    'title': 'Foo title',
                    'uploader': 'Foo uploader',
                    'id': test_id,
                    'extractor': test_extractor,
                }, sd)
                x.iterate_file(s)
                result = x.search_existing_file(test_extractor, test_id)
                assert result.base_path == str(file_path.name)
                generated = x.generate_download_from_existing(sd, result)
                assert generated.webpage_url == 'https://foo.example.com' #pylint:disable=no-member
