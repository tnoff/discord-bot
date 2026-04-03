import logging
from pathlib import Path

from moviepy.audio.fx import AudioNormalize
from moviepy import AudioFileClip
from opentelemetry.trace.status import StatusCode

from discord_bot.utils.otel import otel_span_wrapper

logger = logging.getLogger(__name__)

class AudioProcessingError(Exception):
    '''Raised when audio conversion to PCM fails'''

def get_finished_path(path: Path) -> Path:
    '''
    Get 'editing path' for editing files

    path : Path of original file
    '''
    return path.parent / (path.stem + '.pcm')

def get_editing_path(path: Path) -> Path:
    '''
    Get 'editing path' for editing files

    path: Path of original file
    '''
    return path.parent / (path.stem + '.edited.pcm')

def edit_audio_file(file_path: Path) -> Path:
    '''
    Normalize audio for file

    file_path: Audio file to edit
    delete_old_file : Delete old file if it exists
    '''
    finished_path = get_finished_path(file_path)
    editing_path = get_editing_path(file_path)
    with otel_span_wrapper('audio.edit_file', attributes={'file_path': str(file_path)}) as span:
        try:
            audio_clip = AudioFileClip(str(file_path))
        except KeyError as error:
            # File cannot be opened as audio (codec/format issue)
            logger.warning('Could not open %s as audio, codec or format not supported', file_path)
            span.record_exception(error)
            span.set_status(StatusCode.ERROR)
            raise AudioProcessingError(f'Could not open {file_path} as audio') from error
        edited_audio = audio_clip.with_effects([AudioNormalize()]) #pylint:disable=no-member
        array = edited_audio.to_soundarray(fps=48000)
        if len(array) == 0:
            raise AudioProcessingError(f'Audio conversion produced empty output for {file_path}')
        array = (array * 32767).astype('<i2')
        array.tofile(str(editing_path))
        # s16le stereo: 2 bytes/sample * 2 channels = 4 bytes/frame
        expected_size = len(array) * 4
        actual_size = editing_path.stat().st_size
        if actual_size % 4 != 0:
            raise AudioProcessingError(
                f'PCM output {editing_path} size {actual_size} is not divisible by 4, file is corrupt'
            )
        if actual_size != expected_size:
            raise AudioProcessingError(
                f'PCM output {editing_path} size {actual_size} does not match expected {expected_size}'
            )
    file_path.unlink()
    editing_path.rename(finished_path)
    return finished_path
