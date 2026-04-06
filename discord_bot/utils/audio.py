import subprocess
from pathlib import Path

from opentelemetry.trace.status import StatusCode

from discord_bot.utils.otel import otel_span_wrapper
from discord_bot.utils.common import get_logger, LoggingConfig

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

def edit_audio_file(file_path: Path, normalize_audio: bool, logging_config: LoggingConfig) -> Path:
    '''
    Normalize audio for file and convert to PCM.

    Uses ffmpeg with the loudnorm filter (EBU R128) to normalise loudness,
    then writes raw s16le stereo PCM at 48 kHz suitable for Discord playback.

    file_path: Audio file to edit
    '''
    logger = get_logger('audio_editing', logging_config)
    finished_path = get_finished_path(file_path)
    editing_path = get_editing_path(file_path)
    with otel_span_wrapper('audio.edit_file', attributes={'file_path': str(file_path)}) as span:
        ffmpeg_args = [
            'ffmpeg', '-y',
            '-i', str(file_path),
            '-f', 's16le',
            '-ar', '48000',
            '-ac', '2',
            str(editing_path),
        ]
        if normalize_audio:
            ffmpeg_args.insert(4, '-af')
            ffmpeg_args.insert(5, 'loudnorm')
        try:
            subprocess.run(
                ffmpeg_args,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as error:
            logger.error(
                'Could not open %s as audio, ffmpeg failed: %s',
                file_path,
                error.stderr.decode(errors='replace'),
            )
            span.record_exception(error)
            span.set_status(StatusCode.ERROR)
            raise AudioProcessingError(f'Could not open {file_path} as audio') from error
        actual_size = editing_path.stat().st_size
        if actual_size == 0:
            raise AudioProcessingError(f'Audio conversion produced empty output for {file_path}')
        if actual_size % 4 != 0:
            raise AudioProcessingError(
                f'PCM output {editing_path} size {actual_size} is not divisible by 4, file is corrupt'
            )
    file_path.unlink()
    editing_path.rename(finished_path)
    return finished_path
