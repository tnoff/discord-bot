from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

from moviepy.audio.AudioClip import AudioClip
from numpy import sin, pi

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
