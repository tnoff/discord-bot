from pathlib import Path

from moviepy.audio.fx import AudioNormalize
from moviepy import AudioFileClip
from numpy import sqrt

def get_finished_path(path: Path) -> Path:
    '''
    Get 'editing path' for editing files

    path : Path of original file
    '''
    return path.parent / (path.stem + '.finished.mp3')

def get_editing_path(path: Path) -> Path:
    '''
    Get 'editing path' for editing files

    path: Path of original file
    '''
    return path.parent / (path.stem + '.edited.mp3')

def edit_audio_file(file_path: Path) -> Path:
    '''
    Normalize audio for file

    file_path: Audio file to edit
    '''
    finished_path = get_finished_path(file_path)
    # If exists, assume it was already edited successfully
    if finished_path.exists():
        return finished_path
    editing_path = get_editing_path(file_path)
    try:
        audio_clip = AudioFileClip(str(file_path))
    except KeyError:
        # Need to treat like a video
        # Assume we cant do file processing at this point
        return None
    # Find dead audio at start and end of file
    cut = lambda i: audio_clip.subclipped(i, i+1).to_soundarray(fps=1)
    volume = lambda array: sqrt(((1.0 * array) ** 2).mean())
    volumes = [volume(cut(i)) for i in range(0, int(audio_clip.duration-1))]
    start = 0
    while True:
        if volumes[start] > 0:
            break
        start += 1
    end = len(volumes) - 1
    while True:
        if volumes[end] > 0:
            break
        end -= 1
    # From testing, it seems good to give this a little bit of a buffer, add 1 second to each end if possible
    if start > 0:
        start -= 1
    if end < audio_clip.duration - 1:
        end += 1
    audio_clip = audio_clip.subclipped(start, end + 1)
    # Normalize audio
    edited_audio = audio_clip.with_effects([AudioNormalize()]) #pylint:disable=no-member
    edited_audio.write_audiofile(str(editing_path))
    editing_path.rename(finished_path)
    return finished_path
