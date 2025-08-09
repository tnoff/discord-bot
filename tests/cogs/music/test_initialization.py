from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

import pytest

from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.music import Music, VideoEditing

from tests.helpers import mock_session, fake_source_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

def test_video_editing_postprocessor():
    """Test VideoEditing post-processor with successful edit"""
    # Test successful edit
    processor = VideoEditing()
    mock_path = MagicMock()
    mock_path.__str__ = MagicMock(return_value='/path/to/file.mp3')

    with patch('discord_bot.cogs.music.Path', return_value=mock_path), \
         patch('discord_bot.cogs.music.edit_audio_file', return_value='/path/to/edited.mp3'):

        info = {'_filename': '/path/to/file.mp3'}
        result_list, result_info = processor.run(info)

        assert not result_list
        assert result_info['_filename'] == '/path/to/edited.mp3'
        assert result_info['filepath'] == '/path/to/edited.mp3'

def test_video_editing_postprocessor_no_edit():
    """Test VideoEditing post-processor when edit fails/returns None"""
    processor = VideoEditing()
    mock_path = MagicMock()
    mock_path.__str__ = MagicMock(return_value='/path/to/file.mp3')

    with patch('discord_bot.cogs.music.Path', return_value=mock_path), \
         patch('discord_bot.cogs.music.edit_audio_file', return_value=None):

        info = {'_filename': '/path/to/file.mp3'}
        result_list, result_info = processor.run(info)

        assert not result_list
        assert result_info['_filename'] == '/path/to/file.mp3'
        assert result_info['filepath'] == '/path/to/file.mp3'

def test_music_init_with_spotify_credentials(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with Spotify credentials configured"""
    config = {
        'music': {
            'download': {
                'spotify_credentials': {
                    'client_id': 'test_client_id',
                    'client_secret': 'test_client_secret'
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    with patch('discord_bot.cogs.music.SpotifyClient') as mock_spotify:
        cog = Music(fake_context['bot'], config, None)
        mock_spotify.assert_called_once_with('test_client_id', 'test_client_secret')
        assert cog.spotify_client is not None

def test_music_init_with_youtube_api_key(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with YouTube API key configured"""
    config = {
        'music': {
            'download': {
                'youtube_api_key': 'test_api_key'
            }
        }
    } | BASE_MUSIC_CONFIG

    with patch('discord_bot.cogs.music.YoutubeClient') as mock_youtube:
        cog = Music(fake_context['bot'], config, None)
        mock_youtube.assert_called_once_with('test_api_key')
        assert cog.youtube_client is not None

def test_music_init_with_youtube_music_disabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with YouTube Music search disabled"""
    config = {
        'music': {
            'download': {
                'enable_youtube_music_search': False
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, None)
    assert cog.youtube_music_client is None

def test_music_init_server_queue_priority(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with server queue priority configuration"""
    config = {
        'music': {
            'download': {
                'server_queue_priority': [
                    {'server_id': '123456789', 'priority': 1},
                    {'server_id': '987654321', 'priority': 2}
                ]
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, None)
    assert cog.server_queue_priority[123456789] == 1
    assert cog.server_queue_priority[987654321] == 2

def test_music_init_creates_download_directory(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization creates download directory when specified"""
    with TemporaryDirectory() as tmp_dir:
        download_path = Path(tmp_dir) / 'music_downloads'
        config = {
            'music': {
                'download': {
                    'cache': {
                        'download_dir_path': str(download_path)
                    }
                }
            }
        } | BASE_MUSIC_CONFIG

        cog = Music(fake_context['bot'], config, None)
        assert cog.download_dir == download_path
        assert download_path.exists()

def test_music_init_music_not_enabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization fails when music is not enabled"""
    config = {
        'general': {
            'include': {
                'music': False
            }
        }
    }

    with pytest.raises(CogMissingRequiredArg, match='Music not enabled'):
        Music(fake_context['bot'], config, None)

def test_music_callback_methods(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test metric callback methods read from checkfiles correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the Path.read_text method directly
    mock_read_text = mocker.patch('pathlib.Path.read_text')

    # Set up side effect to return different values based on calls
    mock_read_text.side_effect = ['1', '2', '3', '4']

    # Test each callback method
    result = cog._Music__playlist_history_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1

    result = cog._Music__download_file_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 2

    result = cog._Music__send_message_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 3

    result = cog._Music__cleanup_player_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 4

def test_music_init_with_cache_enabled(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with cache enabled"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 100
                }
            }
        }
    }

    with patch('discord_bot.cogs.music.VideoCacheClient') as mock_video_cache:

        cog = Music(fake_context['bot'], config, fake_engine)

        # Verify cache client was created
        assert mock_video_cache.called
        assert cog.video_cache is not None

def test_music_init_with_backup_storage_options(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with backup storage options"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'test-bucket'
                }
            }
        }
    }

    cog = Music(fake_context['bot'], config, None)
    assert cog.backup_storage_options['backend'] == 's3'
    assert cog.backup_storage_options['bucket_name'] == 'test-bucket'

@pytest.mark.asyncio
async def test_cog_unload_basic(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test basic cog unload functionality"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the tasks to None (default state)
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_task = None  # pylint: disable=protected-access
    cog._cache_cleanup_task = None  # pylint: disable=protected-access
    cog._message_task = None  # pylint: disable=protected-access
    cog._history_playlist_task = None  # pylint: disable=protected-access

    # Mock file operations at pathlib level
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    await cog.cog_unload()

    # Verify bot shutdown flag is set
    assert cog.bot_shutdown is True

@pytest.mark.asyncio
async def test_cog_unload_with_players(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test cog unload with active players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Simplify - just test that bot_shutdown flag gets set
    # Mock everything else to avoid complex async mocking
    mocker.patch.object(cog, 'cleanup')
    mocker.patch.object(cog.bot, 'fetch_guild')
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None to avoid cancellation
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_task = None  # pylint: disable=protected-access
    cog._cache_cleanup_task = None  # pylint: disable=protected-access
    cog._message_task = None  # pylint: disable=protected-access
    cog._history_playlist_task = None  # pylint: disable=protected-access

    # Add fake players
    cog.players[123] = 'player1'
    cog.players[456] = 'player2'

    await cog.cog_unload()

    # Verify bot shutdown flag is set
    assert cog.bot_shutdown is True