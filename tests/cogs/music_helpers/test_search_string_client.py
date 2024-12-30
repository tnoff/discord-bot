from tempfile import NamedTemporaryFile

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.database import BASE, SearchString
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient

def test_search_string_iterate_invalid_type():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        session = sessionmaker(bind=engine)()

        x = SearchCacheClient(session, 10)
        sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SEARCH)
        s = SourceDownload(None, {
            'webpage_url': 'https://foo.example.com',
            'title': 'Foo title',
            'uploader': 'Foo uploader',
            'id': '1234',
            'extractor': 'foo extractor'
        }, sd)
        x.iterate(s)
        assert session.query(SearchString).count() == 0

def test_search_string_iterate_and_check():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        session = sessionmaker(bind=engine)()

        x = SearchCacheClient(session, 10)
        sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SPOTIFY)
        s = SourceDownload(None, {
            'webpage_url': 'https://foo.example.com',
            'title': 'Foo title',
            'uploader': 'Foo uploader',
            'id': '1234',
            'extractor': 'foo extractor'
        }, sd)
        x.iterate(s)
        x.iterate(s)
        assert session.query(SearchString).count() == 1

        result = x.check_cache(sd)
        assert result == 'https://foo.example.com'

def test_search_string_non_existing():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        session = sessionmaker(bind=engine)()

        x = SearchCacheClient(session, 10)
        sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SPOTIFY)
        result = x.check_cache(sd)
        assert result is None

def test_search_string_remove():
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        engine = create_engine(f'sqlite:///{temp_db.name}')
        BASE.metadata.create_all(engine)
        BASE.metadata.bind = engine
        session = sessionmaker(bind=engine)()

        x = SearchCacheClient(session, 1)
        sd = SourceDict('123', 'requester name', '234', 'foo bar', SearchType.SPOTIFY)
        s = SourceDownload(None, {
            'webpage_url': 'https://foo.example.com',
            'title': 'Foo title',
            'uploader': 'Foo uploader',
            'id': '1234',
            'extractor': 'foo extractor'
        }, sd)
        x.iterate(s)
        sd2 = SourceDict('123', 'requester name', '234', 'foo bar2', SearchType.SPOTIFY)
        s2 = SourceDownload(None, {
            'webpage_url': 'https://foo.example2.com',
            'title': 'Foo title',
            'uploader': 'Foo uploader',
            'id': '1234',
            'extractor': 'foo extractor'
        }, sd2)
        x.iterate(s)
        x.iterate(s2)
        assert session.query(SearchString).count() == 2
        x.remove()
        assert session.query(SearchString).count() == 1
