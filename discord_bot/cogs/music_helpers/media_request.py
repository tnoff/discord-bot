from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from dappertable import DapperTable, PaginationRows
from discord import TextChannel

from discord_bot.cogs.music_helpers.common import SearchType, MediaRequestLifecycleStage
from discord_bot.utils.common import discord_format_string_embed
from discord_bot.utils.otel import MediaRequestNaming


class MediaRequest():
    '''
    Original source of play request
    '''
    def __init__(self, guild_id: int, channel_id: int, requester_name: str, requester_id: int, search_string: str,
                 raw_search_string: str, search_type: Literal[SearchType.SPOTIFY, SearchType.DIRECT, SearchType.SEARCH, SearchType.OTHER],
                 added_from_history: bool = False,
                 download_file: bool = True,
                 add_to_playlist: int = None,
                 history_playlist_item_id: int = None,
                 multi_input_string: str = None):
        '''
        Generate new media request options

        guild_id : Guild where video was requested
        channel_id : Channel where video was requested
        requester_name: Display name of original requester
        requester_id : User id of original requester
        search_string : Search string, after processing
        raw_search_string : Original search string
        multi_input_string : Input for playlist type searches
        search_type : Type of search it was
        added_from_history : Whether or not this was added from history
        download_file : Download file eventually
        add_to_playlist : Set to add to playlist after download
        history_playlist_item_id : Delete item from history playlist, pass in database id
        '''
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.requester_name =  requester_name
        self.requester_id = requester_id
        # Keep original search string for later
        # In these cases, original search is what was passed into the search and search string is often youtube url
        # For example raw_search_string can be 'foo title foo artist' and search_string can be the direct url after yt music search
        self.raw_search_string = raw_search_string
        self.search_string = search_string
        self.search_type = search_type
        # Optional values
        self.added_from_history = added_from_history
        self.download_file = download_file
        self.history_playlist_item_id = history_playlist_item_id
        self.add_to_playlist = add_to_playlist
        self.multi_input_string = multi_input_string
        # Message Contextr
        self.uuid = f'request.{uuid4()}'
        self.bundle_uuid = None


    def __str__(self):
        '''
        Expose as string
        Fix embed issues
        https://support.discord.com/hc/en-us/articles/206342858--How-do-I-disable-auto-embed
        '''
        return_string = self.raw_search_string or self.search_string
        return discord_format_string_embed(return_string)


def media_request_attributes(media_request: MediaRequest) -> dict:
    '''
    Return media request attributes for spans
    '''
    return {
        MediaRequestNaming.SEARCH_STRING.value: media_request.search_string,
        MediaRequestNaming.REQUESTER.value: media_request.requester_id,
        MediaRequestNaming.GUILD.value: media_request.guild_id,
        MediaRequestNaming.SEARCH_TYPE.value: media_request.search_type.value,
        MediaRequestNaming.UUID.value: str(media_request.uuid),
    }

# https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
def chunk_list(input_list, size):
    '''
    Split list into equal sized chunks
    '''
    size = max(1, size)
    return [input_list[i:i+size] for i in range(0, len(input_list), size)]

class MultiMediaRequestBundle():
    '''
    Bundle of multiple media requests
    '''
    def __init__(self, guild_id: int, channel_id: int, text_channel: TextChannel, items_per_message: int = 5):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.text_channel = text_channel
        self.uuid = f'request.bundle.{uuid4()}'

        self.items_per_message = max(1, min(items_per_message, 5))  # Enforce range of 1-5 items per message

        # Search options
        self.input_string = None
        self.search_finished = False
        self.search_error = None

        # General attributes
        self.media_requests = []
        self.total = 0
        self.completed = 0
        self.failed = 0
        self.discarded = 0

        # Set later to make sure we return nothing
        # Used in shutdowns
        self.is_shutdown = False

        # Timestamp info
        self.created_at = datetime.now(timezone.utc)
        self.finished_at = None

    def shutdown(self):
        '''
        Remove messages so we know to clear them
        '''
        self.is_shutdown = True

    def add_search_request(self, input_string: str):
        '''
        Add search request to show
        '''
        # Remove 'shuffle' from string
        self.input_string = input_string.replace(' shuffle', '')

    def finish_search_request(self, error_message: str = None, proper_name: str = None):
        '''
        Finish search request
        '''
        # Check if first result has better name
        if proper_name:
            self.input_string = proper_name
        self.search_finished = True
        self.search_error = error_message

    def add_media_request(self, media_request: MediaRequest, stage: MediaRequestLifecycleStage = MediaRequestLifecycleStage.SEARCHING):
        '''
        Add new media request
        '''
        search_string = discord_format_string_embed(media_request.raw_search_string)
        self.media_requests.append({
            'search_string': search_string,
            'status': stage,
            'uuid': media_request.uuid,
            'failed_reason': None,
            'override_message': None,
        })
        self.total += 1
        media_request.bundle_uuid = self.uuid

    def update_request_status(self, media_request: MediaRequest, stage: MediaRequestLifecycleStage, failure_reason: str = None,
                              override_message: str = None):
        '''
        Update the status of a media request in the bundle
        '''
        result = False
        for item in self.media_requests:
            if item['uuid'] != media_request.uuid:
                continue
            match stage:
                case MediaRequestLifecycleStage.COMPLETED:
                    if item['status'] != stage:
                        self.completed += 1
                case MediaRequestLifecycleStage.DISCARDED:
                    if item['status'] != stage:
                        self.discarded += 1
                case MediaRequestLifecycleStage.FAILED:
                    if item['status'] != stage:
                        self.failed += 1
                        if failure_reason:
                            item['failed_reason'] = failure_reason
            item['status'] = stage
            if override_message:
                item['override_message'] = override_message
            result = True
            break
        if self.finished:
            self.finished_at = datetime.now(timezone.utc)
        return result

    @property
    def finished(self):
        '''
        Check if we have finished processing
        '''
        if self.search_finished and self.search_error:
            return True
        if not self.search_finished:
            return False
        return (self.completed + self.failed + self.discarded) == self.total

    @property
    def finished_successfully(self):
        '''
        Check if we have finished processing with no errors
        '''
        return (self.completed + self.discarded) == self.total

    def print(self):
        '''
        Print out into multiple messages
        '''
        # If shutdown, exit completely
        if self.is_shutdown:
            return []
        table = DapperTable(pagination_options=PaginationRows(self.items_per_message))
        # Check if we're in search mode
        if not self.search_finished and self.input_string:
            table.add_row(f'Processing search "{discord_format_string_embed(self.input_string)}"')
        if self.search_finished and self.search_error:
            table.add_row(f'Error processing search "{discord_format_string_embed(self.input_string)}", {self.search_error}')
        # Else proceed as normal
        multi_input = discord_format_string_embed(self.input_string) if self.input_string else self.input_string
        if self.total > 1:
            if self.finished:
                table.add_row(f'Completed processing of "{multi_input}"')
            else:
                table.add_row(f'Processing "{multi_input}"')
            table.add_row(f'{self.completed}/{self.total - self.discarded} items processed successfully, {self.failed} failed')
        for item in self.media_requests:
            # If override set, use this
            if item['override_message']:
                table.add_row(item['override_message'])
                continue
            # Else match on status
            match item['status']:
                case MediaRequestLifecycleStage.COMPLETED:
                    table.add_row('')
                case MediaRequestLifecycleStage.FAILED:
                    x = f'Media request failed download: "{item["search_string"]}"'
                    if item['failed_reason']:
                        x = f'{x}, {item["failed_reason"]}'
                    table.add_row(x)
                case MediaRequestLifecycleStage.QUEUED | MediaRequestLifecycleStage.SEARCHING:
                    table.add_row(f'Media request queued for download: "{item["search_string"]}"')
                case MediaRequestLifecycleStage.IN_PROGRESS:
                    table.add_row(f'Downloading and processing media request: "{item["search_string"]}"')
                case MediaRequestLifecycleStage.BACKOFF:
                    table.add_row(f'Waiting for youtube backoff time before processing media request: "{item["search_string"]}"')
                case MediaRequestLifecycleStage.DISCARDED:
                    table.add_row('')
        result = table.print()
        if not isinstance(result, list):
            result = [result]
        # Remove blanks from output
        result = [i for i in result if i != '']
        return result
