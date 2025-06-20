from enum import Enum
import functools
from contextlib import contextmanager

from discord.ext.commands import Context
from opentelemetry import trace
from opentelemetry.trace.status import StatusCode
from opentelemetry.metrics import get_meter_provider

TRACER = trace.get_tracer(__name__)
METER_PROVIDER = get_meter_provider().get_meter(__name__, '0.0.1')

COMMAND_COUNTER = METER_PROVIDER.create_counter('commands.counter', unit='number', description='Number of commands called')

class MetricNaming(Enum):
    '''
    Metric naming
    '''
    HEARTBEAT = 'heartbeat'
    ACTIVE_PLAYERS = 'active_players'
    VIDEOS_PLAYED = 'videos_played'
    CACHE_FILE_COUNT = 'cache_file_count'
    CACHE_FILESYSTEM_MAX = 'cache_filesystem_max'
    CACHE_FILESYSTEM_USED = 'cache_filesystem_used'

class AttributeNaming(Enum):
    '''
    More generic span attribute constants
    '''
    RETRY_COUNT = 'retry_count'
    BACKGROUND_JOB = 'background_job'

class DiscordContextNaming(Enum):
    '''
    Context attribute constants
    '''
    AUTHOR = 'discord.author'
    CHANNEL = 'discord.channel'
    GUILD = 'discord.guild'
    COMMAND = 'discord.context.command'
    MESSAGE = 'discord.context.message'

class ThirdPartyNaming(Enum):
    '''
    Third party client naming
    '''
    SPOTIFY_PLAYLIST = 'spotify.playlist.id'
    SPOTIFY_ALBUM = 'spotify.album.id'
    SPOTIFY_TRACK = 'spotify.track.id'
    YOUTUBE_PLAYLIST = 'youtube.playlist.id'
    YOUTUBE_MUSIC_SEARCH = 'youtube_music.search_string'

class MusicSourceDictNaming(Enum):
    '''
    Music source dict naming
    '''
    SEARCH_STRING = 'music.source_dict.search_string'
    REQUESTER = 'music.source_dict.requester'
    GUILD = 'music.source_dict.guild'
    SEARCH_TYPE = 'music.source_dict.search_type'
    UUID = 'music.source_dict.uuid'

class MusicSourceDownloadNaming(Enum):
    '''
    Music source download naming
    '''
    VIDEO_URL = 'music.source_download.video_url'
    VIDEO_ID = 'music.source_download.video_id'
    EXTRACTOR = 'music.source_download.extractor'

class MusicVideoCacheNaming(Enum):
    '''
    Music Video Cache Naming
    '''
    ID = 'music.video_cache.id'

def command_wrapper(function):
    '''
    Wrap a discord command function
    '''
    @functools.wraps(function)
    async def _wrapper(*args, **kwargs):
        ctx = None
        for arg in args:
            if isinstance(arg, Context):
                ctx = arg
                break
        span_name = 'unamed_command_wrapper'
        metric_attributes = {}
        if ctx:
            span_name = f'{ctx.command.cog.qualified_name.lower()}.{ctx.command.name}'
            metric_attributes = {
                DiscordContextNaming.AUTHOR.value: ctx.author.id,
                DiscordContextNaming.CHANNEL.value: ctx.channel.id,
                DiscordContextNaming.GUILD.value: ctx.guild.id,
                DiscordContextNaming.COMMAND.value: ctx.command.name,
            }
        with otel_span_wrapper(span_name, ctx=ctx, kind=trace.SpanKind.SERVER):
            COMMAND_COUNTER.add(1, attributes=metric_attributes)
            return await function(*args, **kwargs)
    return _wrapper

@contextmanager
def otel_span_wrapper(span_name: str, ctx: Context = None,
                      kind: trace.SpanKind = trace.SpanKind.INTERNAL,
                      attributes: dict = None):
    '''
    Wrap a generic span
    '''
    with TRACER.start_as_current_span(span_name, kind=kind) as span:
        if ctx:
            span.set_attributes({
                DiscordContextNaming.AUTHOR.value: ctx.author.id,
                DiscordContextNaming.CHANNEL.value: ctx.channel.id,
                DiscordContextNaming.GUILD.value: ctx.guild.id,
                DiscordContextNaming.COMMAND.value: ctx.command.name,
                DiscordContextNaming.MESSAGE.value: ' '.join(i for i in ctx.message.content.split(' ')[1:]),
            })
        if attributes:
            span.set_attributes(attributes)
        try:
            yield span
            span.set_status(StatusCode.OK)
        except Exception as e:
            span.set_status(StatusCode.ERROR)
            span.record_exception(e)
            raise e
        finally:
            pass
