from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock

from moviepy.audio.AudioClip import AudioClip
from numpy import array, sin, pi

from discord_bot.utils.audio import get_editing_path, get_finished_path, edit_audio_file

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
        assert 'edited.mp3' in str(get_editing_path(Path(temp_file.name)).resolve())
        assert 'finished.mp3' in str(get_finished_path(Path(temp_file.name)).resolve())

def test_edit_audio_file():
    with temp_audio_file() as tmp_audio:
        new_path = edit_audio_file(Path(tmp_audio))
        assert 'finished.mp3' in str(new_path)
        assert new_path.stat().st_size > 0

def test_edit_audio_file_already_exists():
    with NamedTemporaryFile(delete=False) as tmp_file:
        path = Path(tmp_file.name)
        path.write_text('testing', encoding='utf-8')
        path.rename(get_finished_path(path))
        edit_audio_file(path)
        assert get_finished_path(path).read_text() == 'testing'
        get_finished_path(path).unlink()

def test_edit_audio_file_key_error(mocker, tmp_path):
    '''AudioFileClip raises KeyError returns None without processing'''
    mocker.patch('discord_bot.utils.audio.AudioFileClip', side_effect=KeyError('format'))
    audio_file = tmp_path / 'test.mp3'
    audio_file.touch()
    result = edit_audio_file(audio_file)
    assert result is None


def _make_mock_clip(volumes):
    '''Build a mock AudioFileClip whose subclipped() returns controlled volume data'''
    mock_clip = MagicMock()
    mock_clip.duration = float(len(volumes) + 1)

    def _sub(start, end=None):
        sub_clip = MagicMock()
        if end is not None and (end - start) == 1:
            # Volume-check call: return array with matching volume
            idx = int(start)
            vol = volumes[idx] if idx < len(volumes) else 1.0
            sub_clip.to_soundarray.return_value = array([[vol, vol]])
        else:
            # Final subclip call: produce a clip that writes the editing file
            edited = MagicMock()
            sub_clip.with_effects.return_value = edited
            def _write(path, **_kw):
                Path(path).touch()
            edited.write_audiofile.side_effect = _write
        return sub_clip

    mock_clip.subclipped.side_effect = _sub
    return mock_clip


def test_edit_audio_file_dead_start_and_end(mocker, tmp_path):
    '''Silence at both start and end trims start/end and applies buffer'''
    # volumes[0] = 0 (silent), volumes[1] = 1.0, volumes[2] = 0 (silent)
    mock_clip = _make_mock_clip([0.0, 1.0, 0.0])
    mocker.patch('discord_bot.utils.audio.AudioFileClip', return_value=mock_clip)
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    result = edit_audio_file(audio_file)
    assert result is not None
    assert 'finished' in result.name
