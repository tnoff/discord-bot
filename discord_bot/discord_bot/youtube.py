import asyncio
from functools import partial
import os

import discord
from moviepy.editor import AudioFileClip
from numpy import sqrt
from youtube_dl.utils import DownloadError

# Only trim audio if buffer exceeding in start or end
AUDIO_BUFFER = 30

class YTDLClient():
    '''
    Youtube DL Source
    '''
    def __init__(self, ytdl, logger):
        self.ytdl = ytdl
        self.logger = logger

    def __getitem__(self, item: str):
        '''
        Allows us to access attributes similar to a dict.

        This is only useful when you are NOT downloading.
        '''
        return self.__getattribute__(item)

    async def run_search(self, search: str, *, loop):
        '''
        Run search and return url
        '''
        loop = loop or asyncio.get_event_loop()

        # All official youtube music has this in the description
        # Add to the search to get better results
        search = f'{search}'

        to_run = partial(self.ytdl.extract_info, url=search, download=False)
        try:
            data = await loop.run_in_executor(None, to_run)
        except DownloadError:
            self.logger.error(f'Error downloading youtube search {search}')
            return None
        if 'entries' in data:
            data = data['entries'][0]
        return data

    async def create_source(self, ctx, search: str, *, loop, exact_match=False,
                            max_song_length=None, trim_audio=False):
        '''
        Create source from youtube search
        '''
        loop = loop or asyncio.get_event_loop()
        self.logger.info(f'{ctx.author} playing song with search {search}')

        # All official youtube music has this in the description
        # Add to the search to get better results
        if not exact_match:
            search = f'{search}'

        to_run = partial(self.prepare_file, search=search, max_song_length=max_song_length,
                         trim_audio=trim_audio)
        data, file_name = await loop.run_in_executor(None, to_run)
        source = None
        if file_name is not None:
            source = discord.FFmpegPCMAudio(file_name)
        return {
            'source': source,
            'data': data,
            'requester': ctx.author,
        }

    def prepare_file(self, search, max_song_length=None, trim_audio=False):
        '''
        Prepare file from youtube search
        Includes download and audio trim
        '''
        try:
            data = self.ytdl.extract_info(url=search, download=True)
        except DownloadError:
            self.logger.error(f'Error downloading youtube search {search}')
            return None, None
        if 'entries' in data:
            # take first item from a playlist
            data = data['entries'][0]
        if max_song_length:
            if data['duration'] > max_song_length:
                return data, None
        self.logger.info(f'Starting download of video {data["title"]}, '
                         f'url {data["webpage_url"]}')
        file_name = self.ytdl.prepare_filename(data)
        self.logger.info(f'Downloaded file {file_name} from youtube url {data["webpage_url"]}')
        if trim_audio:
            changed, file_name, length = self.trim_audio(video_file=file_name)
            if changed:
                self.logger.info(f'Created trimmed audio file to {file_name}')
                data['duraton'] = length
        self.logger.info(f'Music bot adding {data["title"]} to the queue {data["webpage_url"]}')
        return data, file_name

    def trim_audio(self, video_file): #pylint:disable=too-many-locals
        '''
        Trim dead audio from beginning and end of file
        video_file      :   Path to video file
        '''
        # Basic logic adapted from http://zulko.github.io/blog/2014/07/04/automatic-soccer-highlights-compilations-with-python/ pylint:disable=line-too-long
        # First get an array of volume at each second of file
        clip = AudioFileClip(video_file)
        total_length = clip.duration
        cut = lambda i: clip.subclip(i, i+1).to_soundarray(fps=1)
        volume = lambda array: sqrt(((1.0*array)**2).mean())
        volumes = [volume(cut(i)) for i in range(0, int(clip.duration-1))]

        # Get defaults
        volume_length = len(volumes)
        start_index = 0
        end_index = volume_length - 1

        # Remove all dead audio from front
        for (index, vol) in enumerate(volumes):
            if vol != 0:
                start_index = index
                break
        # Remove dead audio from back
        for (index, vol) in enumerate(reversed(volumes[start_index:])):
            if vol != 0:
                end_index = volume_length - index
                break

        # Remove one second from start, and add one to back, for safety
        if start_index != 0:
            start_index -= 1
        end_index += 1

        self.logger.debug(f'Audio trim: Total file length {total_length}, start {start_index}, end {end_index}')
        if start_index < AUDIO_BUFFER and end_index > (int(total_length) - AUDIO_BUFFER):
            self.logger.info('Not enough dead audio at beginning or end of file, skipping audio trim')
            return False, video_file, volume_length

        self.logger.info(f'Trimming file to start at {start_index} and end at {end_index}')
        # Write file with changes, then overwrite
        # Use mp3 by default for easier encoding
        file_name, _ext = os.path.splitext(video_file)
        new_path = f'{file_name}-edited.mp3'
        new_clip = clip.subclip(t_start=start_index, t_end=end_index)
        new_clip.write_audiofile(new_path)
        self.logger.info('Removing old file {video_file}')
        os.remove(video_file)
        return True, new_path, end_index - start_index
