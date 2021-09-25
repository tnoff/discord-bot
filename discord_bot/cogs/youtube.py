import asyncio
from functools import partial

import discord

from youtube_dl import YoutubeDL
from youtube_dl.utils import DownloadError

class YTDLClient():
    '''
    Youtube DL Source
    '''
    def __init__(self, ytdl_options, logger):
        self.ytdl = YoutubeDL(ytdl_options)
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
                            max_song_length=None):
        '''
        Create source from youtube search
        '''
        loop = loop or asyncio.get_event_loop()
        self.logger.info(f'{ctx.author} playing song with search {search}')

        # All official youtube music has this in the description
        # Add to the search to get better results
        if not exact_match:
            search = f'{search}'

        to_run = partial(self.prepare_file, search=search, max_song_length=max_song_length)
        data, file_name = await loop.run_in_executor(None, to_run)
        source = None
        if file_name is not None:
            source = discord.FFmpegPCMAudio(file_name)
        return {
            'source': source,
            'data': data,
            'requester': ctx.author,
        }

    def prepare_file(self, search, max_song_length=None):
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
        self.logger.info(f'Music bot adding {data["title"]} to the queue {data["webpage_url"]}')
        return data, file_name
