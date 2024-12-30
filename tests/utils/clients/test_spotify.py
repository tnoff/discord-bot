from discord_bot.utils.clients.spotify import SpotifyClient

class MockSpotify():
    def __init__(self, auth_manager = None):
        self.auth_manager = auth_manager

    def playlist_tracks(self, _playlist_id, limit=None, offset=None): #pylint: disable=unused-argument
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


    def album_tracks(self, _album_id, limit=None, offset=None): #pylint: disable=unused-argument
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
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', side_effect=MockSpotify)
    s = SpotifyClient('foo', 'bar')
    result = s.playlist_get('foo')
    assert len(result) == 2
    assert result[0]['track_artists'] == 'artist1, artist2'
    assert result[0]['track_name'] == 'Example Song'

def test_spotify_album_get(mocker):
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', side_effect=MockSpotify)
    s = SpotifyClient('foo', 'bar')
    result = s.album_get('foo')
    assert len(result) == 2
    assert result[0]['track_artists'] == 'artist1, artist2'
    assert result[0]['track_name'] == 'Example Song'

def test_spotify_track_get(mocker):
    mocker.patch('discord_bot.utils.clients.spotify.Spotify', side_effect=MockSpotify)
    s = SpotifyClient('foo', 'bar')
    result = s.track_get('foo')
    assert result[0]['track_artists'] == 'artist1, artist2'
    assert result[0]['track_name'] == 'Example Song'
