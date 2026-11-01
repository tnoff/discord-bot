import subprocess
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest

from discord_downloader.utils.audio import get_editing_path, get_finished_path, edit_audio_file, AudioProcessingError


@contextmanager
def temp_audio_file(duration=2):
    '''Generate a temporary MP3 using ffmpeg's sine wave source for integration tests.'''
    with NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
        temp_name = tmp.name
    try:
        subprocess.run(
            [
                'ffmpeg', '-y',
                '-f', 'lavfi',
                '-i', f'sine=frequency=440:duration={duration}',
                temp_name,
            ],
            capture_output=True,
            check=True,
        )
        yield temp_name
    except (FileNotFoundError, subprocess.CalledProcessError):
        yield temp_name
    finally:
        Path(temp_name).unlink(missing_ok=True)


def test_file_paths():
    '''get_editing_path and get_finished_path return correct suffixes.'''
    with NamedTemporaryFile(suffix='.mp3') as temp_file:
        assert 'edited.pcm' in str(get_editing_path(Path(temp_file.name)).resolve())
        assert '.pcm' in str(get_finished_path(Path(temp_file.name)).resolve())


def test_edit_audio_file():
    '''Integration test: real ffmpeg converts audio and produces a non-empty PCM file.'''
    with temp_audio_file() as tmp_audio:
        new_path = edit_audio_file(Path(tmp_audio), False, None)
        assert new_path is not None
        assert new_path.suffix == '.pcm'
        assert new_path.stat().st_size > 0


def test_edit_audio_file_with_normalize():
    '''Integration test: real ffmpeg normalises and converts audio.'''
    with temp_audio_file() as tmp_audio:
        new_path = edit_audio_file(Path(tmp_audio), True, None)
        assert new_path is not None
        assert new_path.suffix == '.pcm'
        assert new_path.stat().st_size > 0


def test_edit_audio_file_converts_to_pcm(mocker, tmp_path):
    '''Successful conversion writes pcm, renames editing file, deletes original.'''
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    def create_pcm(*_args, **_kwargs):
        editing_path.write_bytes(bytes(400))  # 400 bytes, divisible by 4

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=create_pcm)

    result = edit_audio_file(audio_file, False, None)

    assert result is not None
    assert result.suffix == '.pcm'
    assert result.exists()
    assert not audio_file.exists()


def test_edit_audio_file_normalize_includes_loudnorm(mocker, tmp_path):
    '''When normalize_audio=True, loudnorm filter is passed to ffmpeg.'''
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    captured = {}

    def capture_args(*args, **_kwargs):
        captured['args'] = args[0]
        editing_path.write_bytes(bytes(400))

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=capture_args)
    edit_audio_file(audio_file, True, None)

    assert 'loudnorm' in captured['args']
    assert '-af' in captured['args']


def test_edit_audio_file_no_normalize_excludes_loudnorm(mocker, tmp_path):
    '''When normalize_audio=False, loudnorm filter is not passed to ffmpeg.'''
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    captured = {}

    def capture_args(*args, **_kwargs):
        captured['args'] = args[0]
        editing_path.write_bytes(bytes(400))

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=capture_args)
    edit_audio_file(audio_file, False, None)

    assert 'loudnorm' not in captured['args']


def test_edit_audio_file_subprocess_error(mocker, tmp_path):
    '''subprocess.CalledProcessError raises AudioProcessingError and logs an error.'''
    error = subprocess.CalledProcessError(1, 'ffmpeg', stderr=b'Invalid data found')
    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=error)
    mock_logger = MagicMock()
    mocker.patch('discord_downloader.utils.audio.get_logger', return_value=mock_logger)
    audio_file = tmp_path / 'test.mp3'
    audio_file.touch()
    with pytest.raises(AudioProcessingError):
        edit_audio_file(audio_file, False, None)
    mock_logger.error.assert_called_once()


def test_edit_audio_file_empty_output(mocker, tmp_path):
    '''ffmpeg writing zero bytes raises AudioProcessingError.'''
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    def create_empty_pcm(*_args, **_kwargs):
        editing_path.write_bytes(b'')

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=create_empty_pcm)

    with pytest.raises(AudioProcessingError, match='empty output'):
        edit_audio_file(audio_file, False, None)


def test_edit_audio_file_size_not_divisible_by_4(mocker, tmp_path):
    '''File on disk with size not divisible by 4 raises AudioProcessingError.'''
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    def create_odd_pcm(*_args, **_kwargs):
        editing_path.write_bytes(bytes(3))

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=create_odd_pcm)

    with pytest.raises(AudioProcessingError, match='not divisible by 4'):
        edit_audio_file(audio_file, False, None)


def test_edit_audio_file_subprocess_error_records_otel_span(mocker, tmp_path):
    '''CalledProcessError is recorded on the otel span and span status set to ERROR.'''
    error = subprocess.CalledProcessError(1, 'ffmpeg', stderr=b'error')
    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=error)
    mocker.patch('discord_downloader.utils.audio.get_logger', return_value=MagicMock())
    mock_span = MagicMock()
    with patch('discord_downloader.utils.audio.otel_span_wrapper') as mock_wrapper:
        mock_wrapper.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_wrapper.return_value.__exit__ = MagicMock(return_value=False)
        audio_file = tmp_path / 'test.mp3'
        audio_file.touch()
        with pytest.raises(AudioProcessingError):
            edit_audio_file(audio_file, False, None)
    mock_span.record_exception.assert_called_once()
    mock_span.set_status.assert_called_once()


def run_with_decoded_seconds(mocker, tmp_path, seconds, expected_duration):
    '''
    Run edit_audio_file with a mocked ffmpeg that decodes to the given length.

    PCM_BYTES_PER_SECOND is patched to 4 (one stereo frame) so a "600 second"
    track is 2400 bytes rather than 115 MB.
    '''
    mocker.patch('discord_downloader.utils.audio.PCM_BYTES_PER_SECOND', 4)
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'

    def create_pcm(*_args, **_kwargs):
        editing_path.write_bytes(bytes(4 * seconds))

    mocker.patch('discord_downloader.utils.audio.subprocess.run', side_effect=create_pcm)
    return audio_file, edit_audio_file(audio_file, False, None, expected_duration)


def test_edit_audio_file_truncated_raises_and_removes_files(mocker, tmp_path):
    '''Audio decoding to well under the reported duration is rejected, source and output removed.'''
    mocker.patch('discord_downloader.utils.audio.PCM_BYTES_PER_SECOND', 4)
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'
    mocker.patch('discord_downloader.utils.audio.subprocess.run',
                 side_effect=lambda *_a, **_k: editing_path.write_bytes(bytes(4 * 100)))
    mock_logger = MagicMock()
    mocker.patch('discord_downloader.utils.audio.get_logger', return_value=mock_logger)

    with pytest.raises(AudioProcessingError, match='truncated'):
        edit_audio_file(audio_file, False, None, 600)

    assert not audio_file.exists()
    assert not editing_path.exists()
    assert not (tmp_path / 'audio.pcm').exists()
    mock_logger.error.assert_called_once()


def test_edit_audio_file_truncated_records_otel_span(mocker, tmp_path):
    '''A truncated download is recorded on the otel span and its status set to ERROR.'''
    mocker.patch('discord_downloader.utils.audio.PCM_BYTES_PER_SECOND', 4)
    audio_file = tmp_path / 'audio.mp3'
    audio_file.touch()
    editing_path = tmp_path / 'audio.edited.pcm'
    mocker.patch('discord_downloader.utils.audio.subprocess.run',
                 side_effect=lambda *_a, **_k: editing_path.write_bytes(bytes(4 * 100)))
    mocker.patch('discord_downloader.utils.audio.get_logger', return_value=MagicMock())
    mock_span = MagicMock()
    with patch('discord_downloader.utils.audio.otel_span_wrapper') as mock_wrapper:
        mock_wrapper.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_wrapper.return_value.__exit__ = MagicMock(return_value=False)
        with pytest.raises(AudioProcessingError):
            edit_audio_file(audio_file, False, None, 600)
    mock_span.record_exception.assert_called_once()
    mock_span.set_status.assert_called_once()


@pytest.mark.parametrize('seconds,expected', [
    (10, 10),    # exact
    (8, 10),     # 2s short: inside the 3s flat allowance
    (575, 600),  # 25s short: inside 5% of 600
    (300, 10),   # longer than reported is not truncation
])
def test_edit_audio_file_duration_within_tolerance(mocker, tmp_path, seconds, expected):
    '''Output at or near the reported duration, or longer, is accepted.'''
    audio_file, result = run_with_decoded_seconds(mocker, tmp_path, seconds, expected)
    assert result.suffix == '.pcm'
    assert result.exists()
    assert not audio_file.exists()


@pytest.mark.parametrize('seconds,expected', [
    (6, 10),     # 4s short: past the 3s flat allowance
    (560, 600),  # 40s short: past 5% of 600
])
def test_edit_audio_file_duration_beyond_tolerance(mocker, tmp_path, seconds, expected):
    '''Output short of the reported duration by more than the tolerance is rejected.'''
    with pytest.raises(AudioProcessingError, match='truncated'):
        run_with_decoded_seconds(mocker, tmp_path, seconds, expected)


@pytest.mark.parametrize('expected', [None, 0])
def test_edit_audio_file_no_expected_duration_skips_check(mocker, tmp_path, expected):
    '''A missing or zero reported duration (live streams) skips the completeness check.'''
    _, result = run_with_decoded_seconds(mocker, tmp_path, 1, expected)
    assert result.exists()
