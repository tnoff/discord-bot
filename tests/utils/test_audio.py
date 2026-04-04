from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest

from moviepy.audio.AudioClip import AudioClip
from numpy import sin, pi, zeros

from discord_bot.utils.audio import get_editing_path, get_finished_path, edit_audio_file, AudioProcessingError

@contextmanager
def temp_audio_file(duration=2):
    # logic taken from https://zulko.github.io/moviepy/ref/AudioClip.html?highlight=sin
    try:
        with NamedTemporaryFile(suffix='.mp3') as temp_file:
            audio_frames = lambda t: 2 *[sin(404 * 2 * pi * t)]
            audioclip = AudioClip(audio_frames, duration=duration)
            audioclip.write_audiofile(temp_file.name, fps=44100, logger=None)

            yield temp_file.name
    except FileNotFoundError:
        pass

def test_file_paths():
    with NamedTemporaryFile(suffix='.mp3') as temp_file:
        assert 'edited.pcm' in str(get_editing_path(Path(temp_file.name)).resolve())
        assert '.pcm' in str(get_finished_path(Path(temp_file.name)).resolve())

def test_edit_audio_file():
    with temp_audio_file() as tmp_audio:
        new_path = edit_audio_file(Path(tmp_audio))
        assert new_path is not None
        assert new_path.suffix == '.pcm'
        assert new_path.stat().st_size > 0

def test_edit_audio_file_converts_to_pcm(mocker, tmp_path):
    '''Successful conversion writes pcm, renames editing file, deletes original'''
    mock_clip = MagicMock()
    mock_edited = MagicMock()
    mock_clip.with_effects.return_value = mock_edited
    mock_edited.to_soundarray.return_value = zeros((100, 2))
    mocker.patch('discord_bot.utils.audio.AudioFileClip', return_value=mock_clip)

    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()

    result = edit_audio_file(audio_file)

    assert result is not None
    assert result.suffix == '.pcm'
    assert result.exists()
    assert not audio_file.exists()
    mock_edited.to_soundarray.assert_called_once_with(fps=48000)

def test_edit_audio_file_key_error(mocker, tmp_path):
    '''AudioFileClip raises KeyError raises AudioProcessingError and logs a warning'''
    mocker.patch('discord_bot.utils.audio.AudioFileClip', side_effect=KeyError('format'))
    mock_logger = mocker.patch('discord_bot.utils.audio.logger')
    audio_file = tmp_path / 'test.mp3'
    audio_file.touch()
    with pytest.raises(AudioProcessingError):
        edit_audio_file(audio_file)
    mock_logger.warning.assert_called_once()

def test_edit_audio_file_empty_array(mocker, tmp_path):
    '''to_soundarray returning empty array raises AudioProcessingError'''
    mock_clip = MagicMock()
    mock_edited = MagicMock()
    mock_clip.with_effects.return_value = mock_edited
    mock_edited.to_soundarray.return_value = zeros((0, 2))
    mocker.patch('discord_bot.utils.audio.AudioFileClip', return_value=mock_clip)

    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()

    with pytest.raises(AudioProcessingError, match='empty output'):
        edit_audio_file(audio_file)


def test_edit_audio_file_size_not_divisible_by_4(mocker, tmp_path):
    '''File on disk with size not divisible by 4 raises AudioProcessingError'''
    mock_clip = MagicMock()
    mock_edited = MagicMock()
    mock_clip.with_effects.return_value = mock_edited
    mock_edited.to_soundarray.return_value = zeros((100, 2))
    mocker.patch('discord_bot.utils.audio.AudioFileClip', return_value=mock_clip)

    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()

    mock_stat = MagicMock()
    mock_stat.st_size = 3  # not divisible by 4
    mocker.patch.object(Path, 'stat', return_value=mock_stat)

    with pytest.raises(AudioProcessingError, match='not divisible by 4'):
        edit_audio_file(audio_file)


def test_edit_audio_file_size_mismatch(mocker, tmp_path):
    '''File on disk with wrong byte count raises AudioProcessingError'''
    mock_clip = MagicMock()
    mock_edited = MagicMock()
    mock_clip.with_effects.return_value = mock_edited
    mock_edited.to_soundarray.return_value = zeros((100, 2))
    mocker.patch('discord_bot.utils.audio.AudioFileClip', return_value=mock_clip)

    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()

    # Divisible by 4 but wrong total size (8 bytes = 2 frames, expected 100 * 4 = 400)
    mock_stat = MagicMock()
    mock_stat.st_size = 8
    mocker.patch.object(Path, 'stat', return_value=mock_stat)

    with pytest.raises(AudioProcessingError, match='does not match expected'):
        edit_audio_file(audio_file)


def test_edit_audio_file_key_error_records_otel_span(mocker, tmp_path):
    '''KeyError is recorded on the otel span and span status set to ERROR'''
    mocker.patch('discord_bot.utils.audio.AudioFileClip', side_effect=KeyError('format'))
    mock_span = MagicMock()
    with patch('discord_bot.utils.audio.otel_span_wrapper') as mock_wrapper:
        mock_wrapper.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_wrapper.return_value.__exit__ = MagicMock(return_value=False)
        audio_file = tmp_path / 'test.mp3'
        audio_file.touch()
        with pytest.raises(AudioProcessingError):
            edit_audio_file(audio_file)
    mock_span.record_exception.assert_called_once()
    mock_span.set_status.assert_called_once()
