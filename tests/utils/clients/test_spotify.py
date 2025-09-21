from unittest.mock import patch, MagicMock

import pytest
from spotipy.exceptions import SpotifyException

from discord_bot.utils.clients.spotify import SpotifyClient

class MockSpotify():
    def __init__(self, auth_manager = None):
        self.auth_manager = auth_manager

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument #pylint:disable=unused-argument
        next_value = 'https://example.com/foo/next'
        if offset > 0:
            next_value = None
        return {
            'items': [
                {
                    'track': {
                        'name': 'Example Song',
                        'artists': [
                            {
                                'name': 'artist1',
                            },
                            {
                                'name': 'artist2',
                            }
                        ]
                    }
                }
            ],
            'next': next_value,
        }


    def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument #pylint:disable=unused-argument
        next_value = 'https://example.com/foo/next'
        if offset > 0:
            next_value = None
        return {
            'items': [
                {
                    'name': 'Example Song',
                    'artists': [
                        {
                            'name': 'artist1',
                        },
                        {
                            'name': 'artist2',
                        }
                    ]
                }
            ],
            'next': next_value,
        }

    def playlist(self, _playlist_id):
        return {
            'name': 'Example Playlist'
        }

    def album(self, _album_id):
        return {
            'name': 'Example Album',
            'artists': [{'name': 'Example Artist'}]
        }

    def track(self, _track_id):
        return {
            'name': 'Example Song',
            'artists': [
                {
                    'name': 'artist1',
                },
                {
                    'name': 'artist2',
                }
            ]
        }


def test_spotify_playlist_get(mocker):
    # Mock only the spotipy Spotify constructor, not the entire class
    mock_spotify_instance = MockSpotify()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)

    s = SpotifyClient('foo', 'bar')
    result, name = s.playlist_get('foo')
    assert len(result) == 2
    assert result[0]['track_artists'] == 'artist1 artist2'
    assert result[0]['track_name'] == 'Example Song'
    assert name == 'Example Playlist'

def test_spotify_album_get(mocker):
    mock_spotify_instance = MockSpotify()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)

    s = SpotifyClient('foo', 'bar')
    result, name = s.album_get('foo')
    assert len(result) == 2
    assert result[0]['track_artists'] == 'artist1 artist2'
    assert result[0]['track_name'] == 'Example Song'
    assert name == 'Example Artist - Example Album'

def test_spotify_track_get(mocker):
    mock_spotify_instance = MockSpotify()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)

    s = SpotifyClient('foo', 'bar')
    result = s.track_get('foo')
    assert result[0]['track_artists'] == 'artist1 artist2'
    assert result[0]['track_name'] == 'Example Song'


class MockSpotifyWith404():
    """Mock Spotify client that raises 404 errors"""
    def __init__(self, auth_manager=None):
        self.auth_manager = auth_manager

    def playlist(self, _playlist_id):
        exc = SpotifyException(404, -1, "Not found")
        exc.http_status = 404
        raise exc

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument
        exc = SpotifyException(404, -1, "Not found")
        exc.http_status = 404
        raise exc

    def album(self, _album_id):
        exc = SpotifyException(404, -1, "Not found")
        exc.http_status = 404
        raise exc

    def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument
        exc = SpotifyException(404, -1, "Not found")
        exc.http_status = 404
        raise exc


class MockSpotifyWithNon404():
    """Mock Spotify client that raises non-404 errors"""
    def __init__(self, auth_manager=None):
        self.auth_manager = auth_manager

    def playlist(self, _playlist_id):
        exc = SpotifyException(403, -1, "Forbidden")
        exc.http_status = 403
        raise exc

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument
        exc = SpotifyException(500, -1, "Server Error")
        exc.http_status = 500
        raise exc

    def album(self, _album_id):
        exc = SpotifyException(403, -1, "Forbidden")
        exc.http_status = 403
        raise exc

    def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument
        exc = SpotifyException(500, -1, "Server Error")
        exc.http_status = 500
        raise exc


class MockSpotifyWithMissingKeys():
    """Mock Spotify client that returns responses missing expected keys"""
    def __init__(self, auth_manager=None):
        self.auth_manager = auth_manager

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument
        # Missing 'next' key to trigger KeyError path
        return {
            'items': [
                {
                    'track': {
                        'name': 'Example Song',
                        'artists': [{'name': 'artist1'}]
                    }
                }
            ]
            # Missing 'next' key intentionally
        }

    def playlist(self, _playlist_id):
        return {'name': 'Example Playlist'}


class MockSpotifyWithDirectTracks():
    """Mock Spotify client that returns track responses without 'track' wrapper"""
    def __init__(self, auth_manager=None):
        self.auth_manager = auth_manager

    def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument
        # Return tracks directly without 'track' wrapper (album tracks format)
        return {
            'items': [
                {
                    'name': 'Direct Song',
                    'artists': [{'name': 'direct_artist1'}, {'name': 'direct_artist2'}]
                }
            ],
            'next': None
        }

    def album(self, _album_id):
        return {
            'name': 'Direct Album',
            'artists': [{'name': 'Direct Artist'}]
        }


def test_spotify_playlist_get_404_on_playlist_info(mocker):
    """Test playlist_get when playlist() call raises 404"""
    mock_spotify_instance = MockSpotifyWith404()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)

    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.playlist_get('foo')

    assert exc_info.value.http_status == 404


def test_spotify_playlist_get_404_on_playlist_tracks(mocker):
    """Test playlist_get when playlist_tracks() call raises 404"""
    class MockSpotifyPlaylistOkTracksError():
        def __init__(self, auth_manager=None):
            self.auth_manager = auth_manager

        def playlist(self, _playlist_id):
            return {'name': 'Example Playlist'}

        def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument
            exc = SpotifyException(404, -1, "Not found")
            exc.http_status = 404
            raise exc

    mock_spotify_instance = MockSpotifyPlaylistOkTracksError()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.playlist_get('foo')

    assert exc_info.value.http_status == 404


def test_spotify_playlist_get_non_404_error(mocker):
    """Test playlist_get when non-404 SpotifyException is raised - should propagate without setting OK status"""
    mock_spotify_instance = MockSpotifyWithNon404()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.playlist_get('foo')

    assert exc_info.value.http_status == 403


def test_spotify_playlist_get_missing_next_key(mocker):
    """Test playlist_get when response is missing 'next' key - should trigger KeyError path"""
    mock_spotify_instance = MockSpotifyWithMissingKeys()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    result = s.playlist_get('foo')
    # Should return just items list without playlist_name when KeyError occurs
    assert isinstance(result, list)  # Returns list instead of tuple
    assert len(result) == 1  # Should have one track
    assert result[0]['track_name'] == 'Example Song'


def test_spotify_album_get_404_on_album_info(mocker):
    """Test album_get when album() call raises 404"""
    mock_spotify_instance = MockSpotifyWith404()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.album_get('foo')

    assert exc_info.value.http_status == 404


def test_spotify_album_get_404_on_album_tracks(mocker):
    """Test album_get when album_tracks() call raises 404"""
    class MockSpotifyAlbumOkTracksError():
        def __init__(self, auth_manager=None):
            self.auth_manager = auth_manager

        def album(self, _album_id):
            return {
                'name': 'Example Album',
                'artists': [{'name': 'Example Artist'}]
            }

        def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument
            exc = SpotifyException(404, -1, "Not found")
            exc.http_status = 404
            raise exc

    mock_spotify_instance = MockSpotifyAlbumOkTracksError()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.album_get('foo')

    assert exc_info.value.http_status == 404


def test_spotify_album_get_non_404_error(mocker):
    """Test album_get when non-404 SpotifyException is raised"""
    mock_spotify_instance = MockSpotifyWithNon404()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    with pytest.raises(SpotifyException) as exc_info:
        s.album_get('foo')

    assert exc_info.value.http_status == 403


def test_spotify_track_parsing_without_track_wrapper(mocker):
    """Test __get_response_items when items don't have 'track' wrapper (KeyError path)"""
    mock_spotify_instance = MockSpotifyWithDirectTracks()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    result, name = s.album_get('foo')
    assert len(result) == 1
    assert result[0]['track_name'] == 'Direct Song'
    assert result[0]['track_artists'] == 'direct_artist1 direct_artist2'
    assert name == 'Direct Artist - Direct Album'


class MockSpotifyCustomPagination():
    """Mock Spotify for testing pagination limits"""
    def __init__(self, auth_manager=None):
        self.auth_manager = auth_manager

    def playlist(self, _playlist_id):
        return {'name': 'Paginated Playlist'}

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint:disable=unused-argument
        # Test custom pagination limit
        if offset == 0:
            return {
                'items': [{'track': {'name': f'Song {i}', 'artists': [{'name': 'artist'}]}} for i in range(limit)],
                'next': 'https://api.spotify.com/v1/playlists/123/tracks?offset=10&limit=10'
            }
        return {
            'items': [{'track': {'name': f'Song {i}', 'artists': [{'name': 'artist'}]}} for i in range(limit, limit * 2)],
            'next': None
        }

    def album(self, _album_id):
        return {
            'name': 'Paginated Album',
            'artists': [{'name': 'Paginated Artist'}]
        }

    def album_tracks(self, _album_id, limit=None, offset=None): #pylint:disable=unused-argument
        # Test custom pagination limit
        if offset == 0:
            return {
                'items': [{'name': f'Album Song {i}', 'artists': [{'name': 'album_artist'}]} for i in range(limit)],
                'next': 'https://api.spotify.com/v1/albums/123/tracks?offset=10&limit=10'
            }
        return {
            'items': [{'name': f'Album Song {i}', 'artists': [{'name': 'album_artist'}]} for i in range(limit, limit * 2)],
            'next': None
        }


def test_spotify_playlist_get_custom_pagination_limit(mocker):
    """Test playlist_get with custom pagination limit"""
    mock_spotify_instance = MockSpotifyCustomPagination()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    result, name = s.playlist_get('foo', pagination_limit=10)
    assert len(result) == 20  # 2 pages * 10 items each
    assert name == 'Paginated Playlist'
    assert result[0]['track_name'] == 'Song 0'
    assert result[10]['track_name'] == 'Song 10'


def test_spotify_album_get_custom_pagination_limit(mocker):
    """Test album_get with custom pagination limit"""
    mock_spotify_instance = MockSpotifyCustomPagination()
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', return_value=mock_spotify_instance)
    s = SpotifyClient('foo', 'bar')

    result, name = s.album_get('foo', pagination_limit=10)
    assert len(result) == 20  # 2 pages * 10 items each
    assert name == 'Paginated Artist - Paginated Album'
    assert result[0]['track_name'] == 'Album Song 0'
    assert result[10]['track_name'] == 'Album Song 10'


# Integration tests that actually execute SpotifyClient code
# to improve coverage by avoiding complete mocking

def test_spotify_client_integration_playlist_success():
    """Test SpotifyClient.playlist_get with successful response"""
    mock_spotify_instance = MagicMock()
    mock_spotify_instance.playlist.return_value = {'name': 'Test Playlist'}
    mock_spotify_instance.playlist_tracks.return_value = {
        'items': [
            {
                'track': {
                    'name': 'Test Song',
                    'artists': [{'name': 'Test Artist'}]
                }
            }
        ],
        'next': None
    }

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        # This will actually execute SpotifyClient code
        client = SpotifyClient('test_id', 'test_secret')
        result, name = client.playlist_get('test_playlist_id')

        assert len(result) == 1
        assert result[0]['track_name'] == 'Test Song'
        assert result[0]['track_artists'] == 'Test Artist'
        assert name == 'Test Playlist'


def test_spotify_client_integration_album_success():
    """Test SpotifyClient.album_get with successful response"""
    mock_spotify_instance = MagicMock()
    mock_spotify_instance.album.return_value = {
        'name': 'Test Album',
        'artists': [{'name': 'Test Album Artist'}]
    }
    mock_spotify_instance.album_tracks.return_value = {
        'items': [
            {
                'name': 'Album Song',
                'artists': [{'name': 'Album Artist'}]
            }
        ],
        'next': None
    }

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')
        result, name = client.album_get('test_album_id')

        assert len(result) == 1
        assert result[0]['track_name'] == 'Album Song'
        assert result[0]['track_artists'] == 'Album Artist'
        assert name == 'Test Album Artist - Test Album'


def test_spotify_client_integration_track_success():
    """Test SpotifyClient.track_get with successful response"""
    mock_spotify_instance = MagicMock()
    mock_spotify_instance.track.return_value = {
        'name': 'Single Track',
        'artists': [{'name': 'Single Artist'}]
    }

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')
        result = client.track_get('test_track_id')

        assert len(result) == 1
        assert result[0]['track_name'] == 'Single Track'
        assert result[0]['track_artists'] == 'Single Artist'


def test_spotify_client_integration_playlist_404_error():
    """Test SpotifyClient.playlist_get with 404 error"""
    mock_spotify_instance = MagicMock()
    # Configure mock to raise 404 error
    mock_spotify_instance.playlist.side_effect = SpotifyException(404, -1, "Not found")
    mock_spotify_instance.playlist.side_effect.http_status = 404

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')

        with pytest.raises(SpotifyException) as exc_info:
            client.playlist_get('test_playlist_id')

        assert exc_info.value.http_status == 404


def test_spotify_client_integration_album_404_error():
    """Test SpotifyClient.album_get with 404 error"""
    mock_spotify_instance = MagicMock()
    # Configure mock to raise 404 error
    mock_spotify_instance.album.side_effect = SpotifyException(404, -1, "Not found")
    mock_spotify_instance.album.side_effect.http_status = 404

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')

        with pytest.raises(SpotifyException) as exc_info:
            client.album_get('test_album_id')

        assert exc_info.value.http_status == 404


def test_spotify_client_integration_playlist_tracks_404_error():
    """Test SpotifyClient.playlist_get with 404 error on tracks call"""
    mock_spotify_instance = MagicMock()
    # Configure mock to have successful playlist() but failing playlist_tracks()
    mock_spotify_instance.playlist.return_value = {'name': 'Test Playlist'}
    mock_spotify_instance.playlist_tracks.side_effect = SpotifyException(404, -1, "Not found")
    mock_spotify_instance.playlist_tracks.side_effect.http_status = 404

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')

        with pytest.raises(SpotifyException) as exc_info:
            client.playlist_get('test_playlist_id')

        assert exc_info.value.http_status == 404


def test_spotify_client_integration_album_tracks_404_error():
    """Test SpotifyClient.album_get with 404 error on tracks call"""
    mock_spotify_instance = MagicMock()
    # Configure mock to have successful album() but failing album_tracks()
    mock_spotify_instance.album.return_value = {
        'name': 'Test Album',
        'artists': [{'name': 'Test Album Artist'}]
    }
    mock_spotify_instance.album_tracks.side_effect = SpotifyException(404, -1, "Not found")
    mock_spotify_instance.album_tracks.side_effect.http_status = 404

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')

        with pytest.raises(SpotifyException) as exc_info:
            client.album_get('test_album_id')

        assert exc_info.value.http_status == 404


def test_spotify_client_integration_non_404_error():
    """Test SpotifyClient error handling for non-404 errors"""
    mock_spotify_instance = MagicMock()
    # Configure mock to raise non-404 error
    mock_spotify_instance.playlist.side_effect = SpotifyException(403, -1, "Forbidden")
    mock_spotify_instance.playlist.side_effect.http_status = 403

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')

        with pytest.raises(SpotifyException) as exc_info:
            client.playlist_get('test_playlist_id')

        assert exc_info.value.http_status == 403


def test_spotify_client_integration_keyerror_handling():
    """Test SpotifyClient handling of missing 'next' key in response"""
    mock_spotify_instance = MagicMock()
    # Configure mock to return response without 'next' key
    mock_spotify_instance.playlist.return_value = {'name': 'Test Playlist'}
    mock_spotify_instance.playlist_tracks.return_value = {
        'items': [
            {
                'track': {
                    'name': 'Test Song',
                    'artists': [{'name': 'Test Artist'}]
                }
            }
        ]
        # Missing 'next' key intentionally
    }

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')
        result = client.playlist_get('test_playlist_id')

        # Should return just items list when KeyError occurs (no playlist name)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]['track_name'] == 'Test Song'


def test_spotify_client_integration_track_without_wrapper():
    """Test SpotifyClient handling of album tracks without 'track' wrapper"""
    mock_spotify_instance = MagicMock()
    # Configure mock for album tracks without 'track' wrapper
    mock_spotify_instance.album.return_value = {
        'name': 'Test Album',
        'artists': [{'name': 'Test Album Artist'}]
    }
    mock_spotify_instance.album_tracks.return_value = {
        'items': [
            {
                'name': 'Direct Song',
                'artists': [{'name': 'Direct Artist'}]
            }
        ],
        'next': None
    }

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')
        result, name = client.album_get('test_album_id')

        assert len(result) == 1
        assert result[0]['track_name'] == 'Direct Song'
        assert result[0]['track_artists'] == 'Direct Artist'
        assert name == 'Test Album Artist - Test Album'


def test_spotify_client_integration_pagination():
    """Test SpotifyClient pagination with custom limit"""
    mock_spotify_instance = MagicMock()

    # Configure mock for pagination test
    call_count = 0
    def playlist_tracks_side_effect(*args, **kwargs): #pylint:disable=unused-argument
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return {
                'items': [
                    {
                        'track': {
                            'name': f'Song {i}',
                            'artists': [{'name': 'Artist'}]
                        }
                    } for i in range(10)
                ],
                'next': 'https://api.spotify.com/next'
            }
        return {
            'items': [
                {
                    'track': {
                        'name': f'Song {i}',
                        'artists': [{'name': 'Artist'}]
                    }
                } for i in range(10, 15)
            ],
            'next': None
        }

    mock_spotify_instance.playlist.return_value = {'name': 'Paginated Playlist'}
    mock_spotify_instance.playlist_tracks.side_effect = playlist_tracks_side_effect

    with patch('discord_bot.utils.clients.spotify.Spotify') as mock_spotify_class:
        mock_spotify_class.return_value = mock_spotify_instance

        client = SpotifyClient('test_id', 'test_secret')
        result, name = client.playlist_get('test_playlist_id', pagination_limit=10)

        assert len(result) == 15  # 10 + 5 items from two pages
        assert name == 'Paginated Playlist'
        assert result[0]['track_name'] == 'Song 0'
        assert result[14]['track_name'] == 'Song 14'
