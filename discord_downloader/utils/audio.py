# bandit B404: subprocess is required to invoke ffmpeg; no shell, args passed as a list
import subprocess  # nosec B404
from pathlib import Path

from opentelemetry.trace.status import StatusCode

from discord_core.utils.common import get_logger, LoggingConfig
from discord_core.utils.otel import otel_span_wrapper

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

# s16le, 2 channels, 48 kHz: the format edit_audio_file asks ffmpeg for
PCM_BYTES_PER_SECOND = 48000 * 2 * 2
# A decoded file may be this much shorter than yt-dlp's reported duration before
# it counts as truncated: the larger of a flat allowance (yt-dlp reports whole
# seconds, containers pad and trim) and a fraction of the track.
DURATION_TOLERANCE_SECONDS = 3
DURATION_TOLERANCE_FRACTION = 0.05

def edit_audio_file(file_path: Path, normalize_audio: bool, logging_config: LoggingConfig,
                    expected_duration: float | None = None) -> Path:
    '''
    Normalize audio for file and convert to PCM.

    Uses ffmpeg with the loudnorm filter (EBU R128) to normalise loudness,
    then writes raw s16le stereo PCM at 48 kHz suitable for Discord playback.

    file_path: Audio file to edit
    expected_duration: Seconds yt-dlp reported for the track, or None to skip the
        completeness check (live streams and extractors that report no duration).
        A download that decodes cleanly but comes out shorter than this is a
        truncated one, which ffmpeg alone does not reject.
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
            # bandit B603: ffmpeg invocation with a controlled args list and shell=False (default)
            subprocess.run(  # nosec B603
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
        if expected_duration:
            actual_duration = actual_size / PCM_BYTES_PER_SECOND
            allowed_shortfall = max(DURATION_TOLERANCE_SECONDS, expected_duration * DURATION_TOLERANCE_FRACTION)
            if actual_duration < expected_duration - allowed_shortfall:
                # Drop the source too: yt-dlp treats a file already on disk as
                # downloaded, so the retry would otherwise reuse the truncated one.
                editing_path.unlink()
                file_path.unlink()
                error = AudioProcessingError(
                    f'Decoded audio is {actual_duration:.1f}s but yt-dlp reported {expected_duration}s for {file_path}, download is truncated'
                )
                logger.error(str(error))
                span.record_exception(error)
                span.set_status(StatusCode.ERROR)
                raise error
    file_path.unlink()
    editing_path.rename(finished_path)
    return finished_path
