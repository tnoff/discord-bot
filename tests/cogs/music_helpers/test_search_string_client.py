from functools import partial
from tempfile import TemporaryDirectory


from discord_bot.database import SearchString
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient

from tests.helpers import mock_session, fake_source_dict, fake_source_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

def test_search_string_iterate_invalid_type(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    x = SearchCacheClient(partial(mock_session, fake_engine), 10)
    sd = fake_source_dict(fake_context)  # This creates SearchType.SEARCH which should not be cached
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, source_dict=sd) as s:
            x.iterate(s)
            with mock_session(fake_engine) as session:
                assert session.query(SearchString).count() == 0

def test_search_string_iterate_and_check(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    x = SearchCacheClient(partial(mock_session, fake_engine), 10)
    # Create a SourceDict with SPOTIFY type that should be cached
    sd = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'foo bar spotify track', SearchType.SPOTIFY)
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, source_dict=sd) as s:
            x.iterate(s)
            x.iterate(s)
            with mock_session(fake_engine) as session:
                assert session.query(SearchString).count() == 1

            result = x.check_cache(sd)
            assert result == s.webpage_url

def test_search_string_non_existing(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    x = SearchCacheClient(partial(mock_session, fake_engine), 10)
    sd = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'non existing spotify track', SearchType.SPOTIFY)
    result = x.check_cache(sd)
    assert result is None

def test_search_string_remove(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    x = SearchCacheClient(partial(mock_session, fake_engine), 1)
    sd = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'spotify track 1', SearchType.SPOTIFY)
    sd2 = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'spotify track 2', SearchType.SPOTIFY)
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, source_dict=sd) as s:
            with fake_source_download(tmp_dir, source_dict=sd2) as s2:
                x.iterate(s)
                x.iterate(s)
                x.iterate(s2)
                with mock_session(fake_engine) as session:
                    assert session.query(SearchString).count() == 2
                    x.remove()
                    assert session.query(SearchString).count() == 1
