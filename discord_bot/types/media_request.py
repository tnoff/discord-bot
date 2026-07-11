from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, PrivateAttr

from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.types.search import SearchResult
from discord_bot.utils.discord_utils import discord_format_string_embed
from discord_bot.utils.otel import MediaRequestNaming, AttributeNaming


class RetryInformation(BaseModel):
    '''
    Retry information for media requests
    '''
    retry_reason: str | None = None
    retry_count: int = 0
    retry_backoff_seconds: int = 0
    retry_reason_sent: bool = False


class MediaRequest(BaseModel):
    '''
    Original source of play request

    guild_id : Guild where video was requested
    channel_id : Channel where video was requested
    requester_name: Display name of original requester
    requester_id : User id of original requester
    search_result : Search result
    added_from_history : Whether or not this was added from history
    history_playlist_item_id : Delete playlist item from history playlist, pass in database id
    '''
    # Required fields
    guild_id: int
    channel_id: int
    requester_name: str
    requester_id: int
    search_result: SearchResult

    # Optional values
    added_from_history: bool = False
    history_playlist_item_id: int | None = None
    download_file: Literal[True] = True
    # Generated fields
    uuid: str = Field(default_factory=lambda: f'request.{uuid4()}')
    # Track bundle uuid later on
    bundle_uuid: str | None = None

    # Serialised span context captured at submit() time for trace link propagation.
    # Dict keys: trace_id (int), span_id (int), trace_flags (int).
    span_context: dict | None = None

    # Track lifecycle stage
    lifecycle_stage: MediaRequestLifecycleStage = MediaRequestLifecycleStage.SEARCHING
    # Retry info tracker — separate counters for each phase
    download_retry_information: RetryInformation = Field(default_factory=RetryInformation)
    youtube_music_retry_information: RetryInformation = Field(default_factory=RetryInformation)
    # Failure reason tracking
    failure_reason: str | None = None

    _state_machine: Any = PrivateAttr(default=None)

    def model_post_init(self, __context: Any) -> None:  # pylint: disable=arguments-differ
        '''Attach state machine after Pydantic initializes all fields.'''
        self._state_machine = MediaRequestStateMachine(self)

    @property
    def state_machine(self) -> 'MediaRequestStateMachine':
        '''State machine managing lifecycle transitions for this request.'''
        return self._state_machine

    def serialize(self) -> str:
        '''
        Serialize to a JSON string for external queue storage (e.g. Redis).

        The state_machine is a PrivateAttr and is excluded automatically.
        A fresh one is attached by model_post_init on deserialization.
        '''
        return self.model_dump_json()

    @classmethod
    def deserialize(cls, data: str) -> 'MediaRequest':
        '''
        Deserialize from a JSON string produced by serialize().

        A fresh MediaRequestStateMachine is attached by model_post_init.
        '''
        return cls.model_validate_json(data)

    def __str__(self):
        '''
        Expose as string
        Fix embed issues
        https://support.discord.com/hc/en-us/articles/206342858--How-do-I-disable-auto-embed
        '''
        return discord_format_string_embed(self.search_result.resolved_search_string)

    @property
    def display_name(self):
        '''
        Show proper display name for UI functions
        '''
        return self.search_result.proper_name or discord_format_string_embed(self.search_result.raw_search_string)

    @property
    def active_retry_information(self) -> RetryInformation:
        '''The retry-info block for the request's current retry phase.

        RETRY_DOWNLOAD reads ``download_retry_information``; every other stage
        (RETRY_SEARCH, and any non-retry stage) reads the youtube-music block.
        Only meaningful while the request is in a retry stage.
        '''
        if self.lifecycle_stage == MediaRequestLifecycleStage.RETRY_DOWNLOAD:
            return self.download_retry_information
        return self.youtube_music_retry_information

class MediaRequestStateMachine:
    '''
    Encapsulates lifecycle stage transitions for a MediaRequest.
    Use the named mark_* methods rather than setting lifecycle_stage directly.

    Bundle UI rendering is now driven by the broker (it re-renders on each
    lifecycle update); the state machine only records the stage.
    '''
    def __init__(self, request: MediaRequest):
        self._request = request

    def _transition(self, stage: MediaRequestLifecycleStage):
        self._request.lifecycle_stage = stage

    def mark_searching(self):
        '''Transition to SEARCHING'''
        self._transition(MediaRequestLifecycleStage.SEARCHING)

    def mark_queued(self):
        '''Transition to QUEUED'''
        self._transition(MediaRequestLifecycleStage.QUEUED)

    def mark_in_progress(self):
        '''Transition to IN_PROGRESS'''
        self._transition(MediaRequestLifecycleStage.IN_PROGRESS)

    def mark_backoff(self):
        '''Transition to BACKOFF'''
        self._transition(MediaRequestLifecycleStage.BACKOFF)

    def mark_retry_download(self, reason: str, backoff_seconds: int | None = None,
                            retry_count: int | None = None):
        '''Transition to RETRY_DOWNLOAD and record retry details.

        retry_count is the worker's authoritative attempt number; when provided
        it is stored so the "attempt N/M" summary renders the real count instead
        of this copy's stale zero. retry_reason_sent is reset so a later retry
        re-renders the message with the bumped count.
        '''
        info = self._request.download_retry_information
        info.retry_reason = reason
        info.retry_backoff_seconds = backoff_seconds
        if retry_count is not None:
            info.retry_count = retry_count
        info.retry_reason_sent = False
        self._transition(MediaRequestLifecycleStage.RETRY_DOWNLOAD)

    def mark_retry_search(self, reason: str, backoff_seconds: int | None = None,
                          retry_count: int | None = None):
        '''Transition to RETRY_SEARCH and record retry details.

        See mark_retry_download for retry_count / retry_reason_sent semantics.
        '''
        info = self._request.youtube_music_retry_information
        info.retry_reason = reason
        info.retry_backoff_seconds = backoff_seconds
        if retry_count is not None:
            info.retry_count = retry_count
        info.retry_reason_sent = False
        self._transition(MediaRequestLifecycleStage.RETRY_SEARCH)

    def mark_failed(self, reason: str | None = None):
        '''Transition to FAILED, optionally recording a failure reason'''
        if reason is not None:
            self._request.failure_reason = reason
        self._transition(MediaRequestLifecycleStage.FAILED)

    def mark_completed(self):
        '''Transition to COMPLETED'''
        self._transition(MediaRequestLifecycleStage.COMPLETED)

    def mark_discarded(self):
        '''Transition to DISCARDED'''
        self._transition(MediaRequestLifecycleStage.DISCARDED)


def media_request_attributes(media_request: MediaRequest) -> dict:
    '''
    Return media request attributes for spans
    '''
    return {
        MediaRequestNaming.SEARCH_STRING.value: media_request.search_result.raw_search_string,
        MediaRequestNaming.REQUESTER.value: media_request.requester_id,
        MediaRequestNaming.GUILD.value: media_request.guild_id,
        MediaRequestNaming.SEARCH_TYPE.value: media_request.search_result.search_type.value,
        MediaRequestNaming.UUID.value: str(media_request.uuid),
        AttributeNaming.RETRY_COUNT.value: media_request.download_retry_information.retry_count,
    }

# https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
def chunk_list(input_list, size):
    '''
    Split list into equal sized chunks
    '''
    size = max(1, size)
    return [input_list[i:i+size] for i in range(0, len(input_list), size)]
