from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from dappertable import DapperTable, PaginationLength, shorten_string
from discord import TextChannel

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.music_helpers.common import SearchType, MediaRequestLifecycleStage
from discord_bot.utils.common import discord_format_string_embed
from discord_bot.utils.otel import MediaRequestNaming


@dataclass
class MediaRequest():
    '''
    Original source of play request

    guild_id : Guild where video was requested
    channel_id : Channel where video was requested
    requester_name: Display name of original requester
    requester_id : User id of original requester
    search_string : Search string, after processing
    raw_search_string : Original search string
    search_type : Type of search it was
    added_from_history : Whether or not this was added from history
    download_file : Download file eventually
    add_to_playlist : Set to add to playlist after download
    history_playlist_item_id : Delete playlist item from history playlist, pass in database id
    display_name_override : Only used in media request bundles, overrides search strings
    '''
    # Required fields
    guild_id: int
    channel_id: int
    requester_name: str
    requester_id: int
    search_string: str
    # Keep original search string for later
    # In these cases, original search is what was passed into the search and search string is often youtube url
    # For example raw_search_string can be 'foo title foo artist' and search_string can be the direct url after yt music search
    raw_search_string: str
    search_type: Literal[SearchType.SPOTIFY, SearchType.DIRECT, SearchType.SEARCH, SearchType.OTHER]
    # Optional values
    added_from_history: bool = False
    download_file: bool = True
    add_to_playlist: int = None
    history_playlist_item_id: int = None
    display_name_override: str = None
    # Generated fields
    uuid: str = field(default_factory=lambda: f'request.{uuid4()}')
    bundle_uuid: str = None

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
    def __init__(self, guild_id: int, channel_id: int, text_channel: TextChannel, pagination_length: int = DISCORD_MAX_MESSAGE_LENGTH):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.text_channel = text_channel
        self.uuid = f'request.bundle.{uuid4()}'
        self.pagination_length = pagination_length

        self.table = DapperTable(pagination_options=PaginationLength(pagination_length))
        self.row_collections = []

        # Search options
        self.input_string = None
        self.search_finished = False
        self.search_error = None
        self.has_search_banner = False  # Track if search banner exists

        # General attributes
        self.media_requests = []
        self.total = 0
        self.completed = 0
        self.failed = 0
        self.discarded = 0

        # Check if all expected requests have been added
        self.all_requests_enqueued = False

        # Set later to make sure we return nothing
        # Used in shutdowns
        self.is_shutdown = False

        # Timestamp info
        self.created_at = datetime.now(timezone.utc)
        self.finished_at = None

    def all_requests_added(self):
        '''
        Mark all requests as added
        '''
        self.all_requests_enqueued = True
        # Is probably rare but sometimes everything hits the cache
        # and update was never called but 'add' was
        self._check_finished()
        # Remove double search if multiple requests not queued
        if self.total == 1:
            self.table.remove_row(0)
            self.media_requests[0]['table_index'] = 0

        self.row_collections = self.table.get_paginated_rows()

        # Build mapping from table_index to (collection_idx, row_idx)
        # DapperTable.get_paginated_rows() returns a list of lists of DapperRow objects
        # We need to map each table_index to its position in the paginated structure

        # First, build a flat list of all row indices across all collections
        table_index_to_position = {}
        current_table_index = 0

        for collection_idx, row_collection in enumerate(self.row_collections):
            for row_idx in range(len(row_collection)):
                table_index_to_position[current_table_index] = (collection_idx, row_idx)
                current_table_index += 1

        # Now map each media request's table_index to its position
        for media_request in self.media_requests:
            if media_request['table_index'] is None:
                continue  # Discarded media_requests have no table_index

            if media_request['table_index'] in table_index_to_position:
                collection_idx, row_idx = table_index_to_position[media_request['table_index']]
                media_request['row_collection_index'] = collection_idx
                media_request['row_index_in_collection'] = row_idx

        # If we have multiple media_requests and a search string, add status header
        if self.total > 1 and self.input_string:
            multi_input = discord_format_string_embed(self.input_string) if self.input_string else self.input_string
            top_line = f'Processing "{multi_input}"\n{self.completed}/{self.total - self.discarded} media_requests processed successfully, {self.failed} failed'
            self._edit_search_banner(top_line)

    def shutdown(self):
        '''
        Remove messages so we know to clear them
        '''
        self.is_shutdown = True

    def set_initial_search(self, input_string: str):
        '''
        Add search request to show
        '''
        # Remove 'shuffle' from string
        # Shorten string down to 256 at most to be safe
        self.input_string = shorten_string(input_string.replace(' shuffle', ''), 256)
        self.table.add_row(f'Processing search "{discord_format_string_embed(self.input_string)}"')

    def set_multi_input_request(self, error_message: str = None, proper_name: str = None):
        '''
        Mark request as having multiple media requests
        '''
        # Check if first result has better name
        self.search_finished = True
        if error_message:
            self.search_error = error_message
            # Edit row 0 directly (created by set_initial_search) with error message
            if self.input_string:
                self.table.edit_row(0, f'Error processing search "{discord_format_string_embed(self.input_string)}", {error_message}')
                self.has_search_banner = True
            return
        if proper_name:
            # Shorten string down to 256 at most to be safe
            self.input_string = shorten_string(proper_name, 256)
        self.has_search_banner = True
        multi_input = discord_format_string_embed(self.input_string) if self.input_string else self.input_string
        self._edit_search_banner(f'Processing "{multi_input}"')

    def add_media_request(self, media_request: MediaRequest, stage: MediaRequestLifecycleStage = MediaRequestLifecycleStage.SEARCHING):
        '''
        Add new media request
        '''
        search_string = discord_format_string_embed(media_request.display_name_override or media_request.raw_search_string)
        # Generally upon add only discard, searching, and queued are used
        # Ignore discarded requests in terms of showing to user
        # But keep track of numbers for later calculations
        table_index = None
        if stage == MediaRequestLifecycleStage.DISCARDED:
            self.discarded +=1
        elif stage in [MediaRequestLifecycleStage.QUEUED, MediaRequestLifecycleStage.SEARCHING]:
            table_index = self.table.add_row(f'Media request queued for download: "{search_string}"')
        elif stage in [MediaRequestLifecycleStage.COMPLETED]:
            self.completed += 1
        self.media_requests.append({
            'search_string': search_string,
            'status': stage,
            'uuid': media_request.uuid,
            'table_index': table_index,
            'row_collection_index': None,
            'row_index_in_collection': None,
        })
        self.total += 1
        media_request.bundle_uuid = self.uuid

    def _edit_row_data(self, media_request: dict, message: str):
        '''
        Edit the row data based on indexes
        '''
        # Always edit the table to keep it as source of truth
        if media_request['table_index'] is not None:
            self.table.edit_row(media_request['table_index'], message)

        # Also edit row_collections if they've been populated
        if (self.row_collections and
            media_request['row_collection_index'] is not None and
            media_request['row_index_in_collection'] is not None):
            self.row_collections[media_request['row_collection_index']][media_request['row_index_in_collection']].edit(message)

        return media_request['table_index'] is not None

    def _edit_search_banner(self, message: str):
        '''
        Edit row 0 (search banner/status line) in both table and row_collections
        Only edits if a search banner actually exists
        '''
        if not self.has_search_banner:
            return

        self.table.edit_row(0, message)

        # Also update in row_collections if built (row 0 is always in collection 0, index 0)
        if self.row_collections and len(self.row_collections) > 0:
            # Assuming row_collections[0] is indexable and has row 0
            self.row_collections[0][0].edit(message)

    def _check_finished(self):
        '''
        Check if all requests finished
        '''
        multi_input = discord_format_string_embed(self.input_string) if self.input_string else self.input_string
        if self.total > 1:
            top_line = f'Processing "{multi_input}"'
            if self.finished:
                top_line = f'Completed processing of "{multi_input}"'
            top_line = f'{top_line}\n{self.completed}/{self.total - self.discarded} media_requests processed successfully, {self.failed} failed'
            self._edit_search_banner(top_line)
        return True

    def update_request_status(self, media_request: MediaRequest, stage: MediaRequestLifecycleStage, failure_reason: str = None,
                              override_message: str = None):
        '''
        Update the status of a media request in the bundle
        '''
        result = False
        for media_request_dict in self.media_requests:
            if media_request_dict['uuid'] != media_request.uuid:
                continue
            match stage:
                case MediaRequestLifecycleStage.QUEUED:
                    # Keep the existing "queued for download" message, don't update
                    pass
                case MediaRequestLifecycleStage.IN_PROGRESS:
                    if media_request_dict['status'] != stage:
                        if media_request_dict['table_index'] is not None:
                            self._edit_row_data(media_request_dict, f'Downloading and processing media request: "{media_request_dict["search_string"]}"')
                case MediaRequestLifecycleStage.BACKOFF:
                    if media_request_dict['status'] != stage:
                        if media_request_dict['table_index'] is not None:
                            self._edit_row_data(media_request_dict, f'Waiting for youtube backoff time before processing media request: "{media_request_dict["search_string"]}"')
                case MediaRequestLifecycleStage.COMPLETED:
                    if media_request_dict['status'] != stage:
                        if media_request_dict['table_index'] is not None:
                            self._edit_row_data(media_request_dict, '')
                        self.completed += 1
                case MediaRequestLifecycleStage.DISCARDED:
                    if media_request_dict['status'] != stage:
                        if media_request_dict['table_index'] is not None:
                            self._edit_row_data(media_request_dict, '')
                        self.discarded += 1
                case MediaRequestLifecycleStage.FAILED:
                    if media_request_dict['status'] != stage:
                        self.failed += 1
                        x = f'Media request failed download: "{media_request_dict["search_string"]}"'
                        if failure_reason:
                            x = f'{x}, {failure_reason}'
                        if media_request_dict['table_index'] is not None:
                            self._edit_row_data(media_request_dict, x)
            media_request_dict['status'] = stage
            if override_message:
                if media_request_dict['table_index'] is not None:
                    self._edit_row_data(media_request_dict, override_message)
            result = True
            break
        # If not already in media requests, lets go ahead and add
        if not result:
            self.add_media_request(media_request, stage=stage)
        if self.finished:
            self.finished_at = datetime.now(timezone.utc)
        self._check_finished()
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

        # If row_collections hasn't been built yet, we're still in search phase
        if not self.row_collections:
            return self.table.print()

        # Use cached row_collections for stable pagination
        result_strings = [self.table.print_rows(rc) for rc in self.row_collections]
        # Remove blanks from output
        result_strings = [i for i in result_strings if i != '']
        return result_strings
