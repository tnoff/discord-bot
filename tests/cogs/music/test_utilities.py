from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from discord_bot.cogs.music import Music, VideoEditing

from tests.helpers import fake_context #pylint:disable=unused-import

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

def test_video_editing_postprocessor():
    """Test VideoEditing postprocessor success path"""
    # Mock pathlib operations at module level to avoid read-only attribute issues
    with patch('pathlib.Path.unlink') as mock_unlink, \
         patch('pathlib.Path.exists', return_value=True):
        
        pp = VideoEditing(None)
        info = {'filepath': '/tmp/test.webm'}
        
        # Should edit the file and return success
        files_to_delete, info_dict = pp.run(info)
        
        assert files_to_delete == []
        assert info_dict['filepath'] == '/tmp/test.webm'
        mock_unlink.assert_called_once()

def test_video_editing_postprocessor_no_edit():
    """Test VideoEditing postprocessor when file doesn't exist"""
    with patch('pathlib.Path.unlink') as mock_unlink, \
         patch('pathlib.Path.exists', return_value=False):
        
        pp = VideoEditing(None)
        info = {'filepath': '/tmp/test.webm'}
        
        # Should not edit the file since it doesn't exist
        files_to_delete, info_dict = pp.run(info)
        
        assert files_to_delete == []
        assert info_dict['filepath'] == '/tmp/test.webm'
        mock_unlink.assert_not_called()

def test_music_callback_methods(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test music telemetry callback methods"""
    # Mock disk_usage system call
    mock_disk_usage = mocker.patch('discord_bot.cogs.music.disk_usage')
    mock_disk_usage.return_value = (1000000, 500000, 500000)  # total, used, free
    
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    
    # Test music_active_players_callback
    result = cog.music_active_players_callback()
    assert result == 0  # No players active initially
    
    # Test music_cache_count_callback  
    result = cog.music_cache_count_callback()
    assert result == 0  # No cache items initially
    
    # Test music_cache_filestats_callbacks
    result = cog.music_cache_filestats_callbacks()
    # Should return disk usage info
    assert result is not None

def test_music_cache_count_callback(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test cache count callback with database"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    
    result = cog.music_cache_count_callback()
    assert result == 0  # No cache items in fresh database

def test_music_cache_filestats_callbacks(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test cache file stats callback"""
    # Mock pathlib operations
    mock_exists = mocker.patch('pathlib.Path.exists', return_value=True)
    mock_disk_usage = mocker.patch('discord_bot.cogs.music.disk_usage')
    mock_disk_usage.return_value = (2000000, 1000000, 1000000)  # total, used, free
    
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    
    result = cog.music_cache_filestats_callbacks()
    
    # Should return tuple with disk stats
    assert isinstance(result, tuple)
    assert len(result) == 3  # total, used, free
    assert result == (2000000, 1000000, 1000000)

def test_music_active_players_callback(fake_context):  #pylint:disable=redefined-outer-name
    """Test active players callback"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    
    result = cog.music_active_players_callback()
    assert result == 0  # No players initially
    
    # Add a fake player
    cog.players[123] = MagicMock()
    
    result = cog.music_active_players_callback()
    assert result == 1  # One player now

@pytest.mark.asyncio()
async def test_update_download_lockfile_method(fake_context):  #pylint:disable=redefined-outer-name
    """Test __update_download_lockfile private method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    
    # Mock pathlib operations
    with patch('pathlib.Path.exists', return_value=False), \
         patch('pathlib.Path.touch') as mock_touch:
        
        # Call the private method
        cog._Music__update_download_lockfile('test_filename')  # pylint: disable=protected-access
        
        # Verify lockfile was created
        mock_touch.assert_called_once()