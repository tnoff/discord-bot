'''Tests for the play-order message renderer, a port of MusicPlayer.get_queue_order_messages.'''
from pathlib import Path
from unittest.mock import Mock, patch

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult

from discord_broker.interfaces.broker_protocols import BrokerEntry, GuildQueue, PlayingTrack, Zone
from discord_broker.workers import play_order
from discord_broker.workers.play_order import play_order_messages

# The header and rule the gateway's own test (test_music_get_player_messages) pins. The renderer
# was moved, not redesigned, so these must not move.
HEADER = ('```Pos|| Wait Time|| Title                                           || Uploader\n'
          '-----------------------------------------------------------------------------\n')


def _download(title: str, duration=90, uploader: str | None = 'Someone', requester: str = 'TestUser'):
    request = MediaRequest(
        guild_id=1, channel_id=2, requester_name=requester, requester_id=3,
        search_result=SearchResult(search_type=SearchType.DIRECT, raw_search_string='x'))
    return MediaDownload(Path('f.mp3'), {
        'id': title, 'title': title, 'webpage_url': f'https://example.com/{title}',
        'uploader': uploader, 'duration': duration, 'extractor': 'youtube'}, request)


def _entry(download: MediaDownload | None, request=None) -> BrokerEntry:
    request = request or download.media_request
    return BrokerEntry(request=request, download=download, zone=Zone.AVAILABLE)


def _queue(*queued, playing=None) -> GuildQueue:
    return GuildQueue(
        items=[_entry(item) for item in queued],
        playing=PlayingTrack(uuid='u', started_at=1.0, gateway_id='gw', entry=_entry(playing))
        if playing else None,
    )


def _row(position: int, wait: str, title: str, uploader: str) -> str:
    return f'{position:<3}|| {wait:<9}|| {title:<48}|| {uploader}'.rstrip()


def test_an_idle_guild_renders_nothing():
    '''No content is how the dispatcher is told to take the message down.'''
    assert not play_order_messages(GuildQueue())


def test_one_queued_track_matches_the_gateways_table():
    '''Same header, rule and row layout the gateway produced.'''
    [message] = play_order_messages(_queue(_download('One')))
    assert message == HEADER + _row(1, '00:00', 'One', 'Someone') + '```'


def test_now_playing_alone_is_a_line_with_no_table():
    '''The first song playing with an empty queue still shows what is playing.'''
    messages = play_order_messages(_queue(playing=_download('Now', requester='Alice')))
    assert messages == ['Now playing https://example.com/Now requested by Alice']


def test_now_playing_comes_first_then_the_table():
    '''The playing line is its own message, so the video embed lands directly under it.'''
    messages = play_order_messages(_queue(_download('One'), playing=_download('Now', 65)))
    assert messages[0] == 'Now playing https://example.com/Now requested by TestUser'
    assert len(messages) == 2
    assert messages[1].startswith(HEADER)


def test_wait_time_starts_after_the_playing_track():
    '''The first queued track waits for what is playing; later ones add up.'''
    [_, table] = play_order_messages(_queue(_download('One', 90), _download('Two', 3700),
                                            _download('Three'), playing=_download('Now', 65)))
    rows = table.split('\n')[2:]
    assert rows[0].startswith('1  || 01:05    ||')
    assert rows[1].startswith('2  || 02:35    ||')
    # Past an hour the hour stays; only a leading "0:" is dropped.
    assert rows[2].startswith('3  || 1:04:15  ||')


def test_wait_time_without_a_playing_track_starts_at_zero():
    '''Nothing playing, nothing to wait for.'''
    [table] = play_order_messages(_queue(_download('One', 90), _download('Two')))
    assert table.split('\n')[3].startswith('2  || 01:30    ||')


def test_a_track_with_no_duration_adds_nothing_to_the_wait():
    '''Unknown durations count as zero, like the gateway.'''
    [table] = play_order_messages(_queue(_download('One', None), _download('Two')))
    assert table.split('\n')[3].startswith('2  || 00:00    ||')


def test_a_playing_track_with_no_duration_starts_the_wait_at_zero():
    '''Same for the playing track.'''
    messages = play_order_messages(_queue(_download('One'), playing=_download('Now', None)))
    assert messages[1].split('\n')[2].startswith('1  || 00:00    ||')


def test_a_missing_uploader_renders_empty():
    '''No uploader is a blank cell, not the word None.'''
    [table] = play_order_messages(_queue(_download('One', uploader=None)))
    assert 'None' not in table


def test_entries_without_a_download_are_left_out():
    '''Nothing playable to show, so they take no row and no number.'''
    queue = GuildQueue(items=[_entry(None, request=_download('Ghost').media_request),
                              _entry(_download('Real'))])
    [table] = play_order_messages(queue)
    assert 'Real' in table
    assert 'Ghost' not in table
    assert table.split('\n')[2].startswith('1  ||')
    assert len(table.split('\n')) == 3  # header, rule and the one real row


def test_a_playing_track_whose_entry_is_gone_shows_no_line():
    '''If the broker lost the entry there is nothing to describe.'''
    queue = GuildQueue(playing=PlayingTrack(uuid='u', started_at=1.0, gateway_id='gw', entry=None))
    assert not play_order_messages(queue)


def test_a_long_queue_paginates_within_discords_limit():
    '''A queue too long for one message comes back as several, each short enough to send.'''
    messages = play_order_messages(_queue(*[_download(f'Track {i}') for i in range(60)]))
    assert len(messages) > 1
    assert all(len(message) <= DISCORD_MAX_MESSAGE_LENGTH for message in messages)


def test_a_table_that_renders_as_one_string_is_wrapped_in_a_list():
    '''DapperTable returns a bare string for a short table; callers always get a list.'''
    with patch.object(play_order, 'DapperTable') as table_cls:
        table_cls.return_value = Mock(render=Mock(return_value='rendered string'))
        assert play_order_messages(_queue(_download('One'))) == ['rendered string']
