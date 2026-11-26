'''Tests for from_row conversions, download serialisation and request helpers.'''
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from discord_core.types.dispatch_result import decode_emojis_result, decode_history_result
from discord_core.types.guild_analytics import GuildAnalyticsEntry
from discord_core.types.markov import MarkovChannelEntry
from discord_core.types.media_download import (
    MediaDownload,
    media_download_attributes,
    media_download_from_dict,
    media_download_to_dict,
)
from discord_core.types.media_request import MediaRequestLifecycleStage, media_request_attributes
from discord_core.types.playlist import PlaylistEntry, PlaylistItemEntry
from discord_core.types.search import SearchResult, SearchType
from discord_core.types.video_cache import VideoCacheEntry
from discord_core.utils.otel import MediaRequestNaming, MusicMediaDownloadNaming

from tests.helpers import fake_source_dict, generate_fake_context

WHEN = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def test_guild_analytics_entry_from_row_copies_every_column():
    '''The detached entry carries the row values as plain data'''
    row = SimpleNamespace(total_plays=5, cached_plays=2, total_duration_days=1,
                          total_duration_seconds=90, created_at=WHEN)
    entry = GuildAnalyticsEntry.from_row(row)
    assert entry.model_dump() == {'total_plays': 5, 'cached_plays': 2, 'total_duration_days': 1,
                                  'total_duration_seconds': 90, 'created_at': WHEN}


def test_markov_channel_entry_from_row_keeps_nullable_columns():
    '''Nullable ids stay None when the row has none'''
    entry = MarkovChannelEntry.from_row(SimpleNamespace(id=1, channel_id=2, server_id=None, last_message_id=None))
    assert (entry.id, entry.channel_id, entry.server_id, entry.last_message_id) == (1, 2, None, None)


def test_playlist_entries_from_row_copy_columns():
    '''Playlist and playlist item rows both convert'''
    playlist = PlaylistEntry.from_row(SimpleNamespace(id=1, name='mix', server_id=9, last_queued=WHEN,
                                                      created_at=WHEN, is_history=False))
    assert (playlist.name, playlist.server_id, playlist.is_history) == ('mix', 9, False)
    item = PlaylistItemEntry.from_row(SimpleNamespace(id=4, title='t', video_url='https://v.example/1',
                                                      uploader='u', playlist_id=1, created_at=WHEN))
    assert (item.id, item.video_url, item.playlist_id) == (4, 'https://v.example/1', 1)


def test_video_cache_entry_stringifies_a_path_base_and_allows_none():
    '''base_path becomes a str when set and stays None otherwise'''
    fields = {'id': 1, 'video_id': 'v', 'video_url': 'u', 'title': 't', 'uploader': 'up', 'duration': 10,
              'extractor': 'youtube', 'last_iterated_at': WHEN, 'created_at': WHEN, 'count': 2,
              'ready_for_deletion': False, 'file_size_bytes': 100, 'storage_type': 'local'}
    with_path = VideoCacheEntry.from_row(SimpleNamespace(base_path=Path('/cache/a'), **fields))
    assert with_path.base_path == '/cache/a'
    assert VideoCacheEntry.from_row(SimpleNamespace(base_path=None, **fields)).base_path is None


def test_decode_history_result_builds_messages_and_cursor():
    '''A history payload decodes into FetchedMessage objects and keeps the cursor'''
    payload = {'guild_id': 1, 'channel_id': 2, 'after_message_id': 30,
               'messages': [{'id': 30, 'content': 'hi', 'created_at': WHEN.isoformat(), 'author_bot': False}]}
    result = decode_history_result(payload)
    assert (result.guild_id, result.channel_id, result.after_message_id) == (1, 2, 30)
    assert result.messages[0].content == 'hi'
    assert result.messages[0].created_at == WHEN
    empty = decode_history_result({'guild_id': 1, 'channel_id': 2})
    assert not empty.messages
    assert empty.after_message_id is None


def test_decode_emojis_result_defaults_to_no_emojis():
    '''An emoji payload decodes, and a missing list becomes empty'''
    assert decode_emojis_result({'guild_id': 4, 'emojis': ['a']}).emojis == ['a']
    assert not decode_emojis_result({'guild_id': 4}).emojis


def test_media_download_round_trips_through_the_wire_dict(tmp_path):
    '''to_dict then from_dict rebuilds the metadata and file size on the caller's request'''
    request = fake_source_dict(generate_fake_context())
    ytdl = {'id': 'abc', 'title': 'T', 'webpage_url': 'https://foo.example/x', 'uploader': 'U',
            'duration': 61, 'extractor': 'youtube', 'ignored': 'dropped'}
    original = MediaDownload(tmp_path / 'a.mp3', ytdl, request, cache_hit=True)
    original.file_size_bytes = 1234
    wire = media_download_to_dict(original)
    assert wire['ytdl_data'] == {key: ytdl[key] for key in ('id', 'title', 'webpage_url', 'uploader', 'duration', 'extractor')}
    assert wire['file_path'] == str(tmp_path / 'a.mp3')
    assert wire['request']['guild_id'] == request.guild_id
    rebuilt = media_download_from_dict(wire, request)
    assert rebuilt.media_request is request
    assert (rebuilt.id, rebuilt.title, rebuilt.duration) == ('abc', 'T', 61)
    assert rebuilt.file_path == tmp_path / 'a.mp3'
    assert rebuilt.cache_hit is True
    assert rebuilt.file_size_bytes == 1234
    assert str(rebuilt) == 'https://foo.example/x'


def test_media_download_without_a_file_serialises_none_and_rebuilds_without_path():
    '''A download with no file path carries None both ways'''
    request = fake_source_dict(generate_fake_context())
    original = MediaDownload(None, {'webpage_url': 'https://foo.example/y'}, request)
    wire = media_download_to_dict(original)
    assert wire['file_path'] is None
    rebuilt = media_download_from_dict({'ytdl_data': wire['ytdl_data']}, request)
    assert rebuilt.file_path is None
    assert rebuilt.cache_hit is False
    assert rebuilt.file_size_bytes is None


def test_media_download_attributes_name_the_request_and_url():
    '''Span attributes map the request uuid and the webpage url'''
    request = fake_source_dict(generate_fake_context())
    download = MediaDownload(None, {'webpage_url': 'https://foo.example/z'}, request)
    assert media_download_attributes(download) == {
        MediaRequestNaming.UUID.value: str(request.uuid),
        MusicMediaDownloadNaming.VIDEO_URL.value: 'https://foo.example/z',
    }


def test_media_request_attributes_describe_the_request():
    '''Span attributes carry search string, requester, guild, type and uuid'''
    request = fake_source_dict(generate_fake_context())
    attributes = media_request_attributes(request)
    assert attributes[MediaRequestNaming.SEARCH_STRING.value] == request.search_result.raw_search_string
    assert attributes[MediaRequestNaming.REQUESTER.value] == request.requester_id
    assert attributes[MediaRequestNaming.GUILD.value] == request.guild_id
    assert attributes[MediaRequestNaming.SEARCH_TYPE.value] == 'search'
    assert attributes[MediaRequestNaming.UUID.value] == str(request.uuid)


def test_active_retry_information_follows_the_lifecycle_stage():
    '''RETRY_DOWNLOAD reads the download block; any other stage reads the ytmusic block'''
    request = fake_source_dict(generate_fake_context())
    request.download_retry_information.retry_count = 3
    request.youtube_music_retry_information.retry_count = 7
    request.lifecycle_stage = MediaRequestLifecycleStage.RETRY_DOWNLOAD
    assert request.active_retry_information.retry_count == 3
    request.lifecycle_stage = MediaRequestLifecycleStage.RETRY_SEARCH
    assert request.active_retry_information.retry_count == 7


def test_search_result_prefers_the_youtube_music_string_once_added():
    '''resolved_search_string falls back to the raw string until a result is added'''
    result = SearchResult(search_type=SearchType.SEARCH, raw_search_string='raw words')
    assert result.resolved_search_string == 'raw words'
    result.add_youtube_music_result('https://music.example/watch?v=1')
    assert result.youtube_music_search_string == 'https://music.example/watch?v=1'
    assert result.resolved_search_string == 'https://music.example/watch?v=1'
