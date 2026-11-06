'''
The broker seam: every route BrokerHttpServer serves, described and proved.

Response bodies are the models in ``discord_core/types/broker_responses.py``. Request bodies are
the models the handlers validate with where there is one (a media request, a download result, a
player session) and otherwise a model written here, because the handlers read those bodies field
by field. ``test_openapi_bodies.py`` sends each model's example and drops each required field to
check the description against what the handler really accepts.
'''
from unittest.mock import AsyncMock

from pydantic import BaseModel, RootModel

from discord_core.routes import broker as broker_routes
from discord_core.routes import cache_corrupt as corrupt_routes
from discord_core.routes import gateway_intents as intent_routes
from discord_core.routes import guild_queue as queue_routes
from discord_core.types import broker_responses as out
from discord_core.types.download import DownloadResult, DownloadStatus, LifecycleStatusUpdate
from discord_core.types.gateway_intents import IntentKind
from discord_core.types.player_session import PlayerSession
from discord_core.types.playlist_add_request import AnyMediaRequest
from discord_core.types.search_resolution import SearchResolution

from tests.fakes.asyncio_queues import make_broker_http_server, make_guild_queue_broker
from tests.fakes.worker_requests import direct_request, register_track
from tests.routes.specs._model import Call, Operation, Query, reply, serving

GUILD = 5
CHANNEL = 2
GATEWAY = 'gw-1'


# ---------------------------------------------------------------------------
# Request bodies the handlers read field by field
# ---------------------------------------------------------------------------

class MediaRequestBody(RootModel[AnyMediaRequest]):
    '''A media request, as the broker parses it: a plain request or a playlist-add one.'''


class ReportCorruptBody(BaseModel):
    '''POST /cache/report-corrupt'''
    key: str
    media_request: AnyMediaRequest


class CheckoutBody(BaseModel):
    '''POST /requests/{uuid}/checkout'''
    guild_id: int


class RegisterDownloadDirectBody(BaseModel):
    '''POST /downloads/register: a download the gateway built itself.'''
    request: AnyMediaRequest
    file_path: str | None = None
    ytdl_data: dict = {}
    cache_hit: bool = False
    file_size_bytes: int | None = None


class CreateBundleBody(BaseModel):
    '''POST /bundles'''
    guild_id: int
    channel_id: int
    input_string: str | None = None
    has_search_banner: bool = False


class TrackBody(BaseModel):
    '''A body that names one queued request.'''
    uuid: str


class EnqueueBody(TrackBody):
    '''POST /guilds/{guild_id}/queue. A max_size of 0 means no cap.'''
    max_size: int = 0


class ClaimBody(BaseModel):
    '''POST /guilds/{guild_id}/claim'''
    gateway_id: str


class FinishBody(TrackBody):
    '''POST /guilds/{guild_id}/finish'''
    skipped: bool
    history_cap: int


class OpenGuildBody(BaseModel):
    '''POST /guilds/{guild_id}/open'''
    text_channel_id: int


class SubmitIntentBody(BaseModel):
    '''POST /guilds/{guild_id}/intents'''
    kind: IntentKind
    params: dict = {}


class CompleteIntentBody(BaseModel):
    '''POST /intents/{intent_id}/result'''
    ok: bool
    error: str | None = None


# ---------------------------------------------------------------------------
# Environment and arrangements
# ---------------------------------------------------------------------------

def environment():
    '''A broker server over the real guild queue and intent registry on fakeredis.'''
    queue = make_guild_queue_broker()
    server = make_broker_http_server(queue.broker, guild_queue=queue)
    return serving(server, queue=queue, broker=queue.broker, intents=server._gateway_intents)  # pylint: disable=protected-access


def media_json(guild_id: int = GUILD) -> dict:
    '''A direct media request as it travels.'''
    return direct_request(guild_id, CHANNEL).model_dump(mode='json')


def download_result_json(guild_id: int = GUILD) -> dict:
    '''A finished download as the downloader reports it.'''
    return DownloadResult(
        status=DownloadStatus(success=False, user_message='nope'),
        media_request=direct_request(guild_id, CHANNEL), ytdlp_data=None, file_name=None,
    ).model_dump(mode='json')


async def registered(env) -> dict:
    '''A request the broker knows about, with no download yet.'''
    request = direct_request(GUILD, CHANNEL)
    await env.parts.broker.register_request(request)
    return {'uuid': str(request.uuid), 'request': request}


async def downloaded(env) -> dict:
    '''A request with a finished download.'''
    return {'uuid': await register_track(env.parts.broker, GUILD, 'a')}


async def queued(env) -> dict:
    '''A downloaded track waiting in the guild's queue.'''
    ctx = await downloaded(env)
    await env.parts.queue.enqueue(GUILD, ctx['uuid'])
    return ctx


async def queued_at_version(env) -> dict:
    '''A queued track, with the version the queue is at.'''
    ctx = await queued(env)
    ctx['version'], _skip = await env.parts.queue.poll(GUILD)
    return ctx


async def playing(env) -> dict:
    '''A queued track claimed by a gateway, so it is the one playing.'''
    ctx = await queued(env)
    await env.parts.queue.claim_next(GUILD, GATEWAY)
    return ctx


async def two_queued(env) -> dict:
    '''Two tracks in the queue, for the routes that reorder it.'''
    ctx = await queued(env)
    ctx['second'] = await register_track(env.parts.broker, GUILD, 'b')
    await env.parts.queue.enqueue(GUILD, ctx['second'])
    return ctx


async def result_waiting(env) -> dict:
    '''A download result on the bot-ready queue.'''
    resp = await env.client.post(broker_routes.REGISTER_DOWNLOAD.template, json=download_result_json())
    assert resp.status == 202
    return {}


async def search_result_waiting(env) -> dict:
    '''A search resolution on the bot-ready queue.'''
    resp = await env.client.post(broker_routes.REGISTER_SEARCH_RESULT.template,
                                 json={'media_request': media_json()})
    assert resp.status == 202
    return {}


async def cache_hit(env) -> dict:
    '''A broker whose cache holds the track, so /cache/check can answer with it.'''
    ctx = await downloaded(env)
    broker = env.parts.broker
    entry = await broker.get_entry(ctx['uuid'])
    broker.check_cache = AsyncMock(return_value=entry.download)
    ctx['request'] = entry.download.media_request
    return ctx


async def bundle(env) -> dict:
    '''A bundle the broker is holding for the guild.'''
    return {'uuid': await env.parts.broker.create_bundle(GUILD, CHANNEL)}


async def session(env) -> dict:
    '''A stored player session.'''
    await env.parts.broker.save_player_session(player_session())
    return {}


async def intent(env) -> dict:
    '''A join intent submitted and not yet completed.'''
    return {'id': await env.parts.intents.submit(GUILD, IntentKind.JOIN.value, {})}


async def claimed_intent(env) -> dict:
    '''A join intent the gateway has taken.'''
    ctx = await intent(env)
    await env.parts.intents.claim(GATEWAY)
    return ctx


async def completed_intent(env) -> dict:
    '''A join intent the gateway has finished.'''
    ctx = await claimed_intent(env)
    await env.parts.intents.complete(ctx['id'], True, None)
    return ctx


def player_session() -> PlayerSession:
    '''A session for the test guild.'''
    return PlayerSession(guild_id=GUILD, voice_channel_id=3, text_channel_id=CHANNEL, was_playing=True)


def uuid_of(ctx):
    '''Path value for a route that names a request.'''
    return ctx['uuid']


UUID = {'uuid': uuid_of}
GUILD_PATH = {'guild_id': GUILD}
BAD_GUILD = Call(422, path={'guild_id': 'abc'})
EMPTY_BODY = {}


def tracks(*names):
    '''A body naming a queued request, from the arrangement.'''
    return lambda ctx: {'uuid': ctx[names[0] if names else 'uuid']}


OPERATIONS = {
    # --- request lifecycle -------------------------------------------------
    broker_routes.REGISTER_REQUEST: Operation(
        'Register a media request with the broker.', request=MediaRequestBody,
        replies=(reply(201, out.RegisterRequestResponse), reply(422, doc='Not a media request.')),
        calls=(Call(201, body=media_json(), path={'uuid': 'request.1'}),
               Call(422, body=EMPTY_BODY, path={'uuid': 'request.1'}))),
    broker_routes.UPDATE_STATUS: Operation(
        'Record a lifecycle event for a request.', request=LifecycleStatusUpdate,
        replies=(reply(200, out.UpdateStatusResponse), reply(422, doc='Not a status update.')),
        calls=(Call(200, body={'event': 'in_progress'}, path=UUID, arrange=registered),
               Call(422, body=EMPTY_BODY, path=UUID, arrange=registered))),
    broker_routes.CHECKOUT: Operation(
        'Hand a downloaded track to a guild, or answer empty when there is none.', request=CheckoutBody,
        replies=(reply(200, out.CheckoutS3Response, out.CheckoutEmptyResponse,
                       doc='The S3 key to fetch, or an empty body when nothing can be handed out.'),
                 reply(422, doc='guild_id missing or not an integer.')),
        calls=(Call(200, body={'guild_id': GUILD}, path=UUID, arrange=downloaded, variant=out.CheckoutS3Response),
               Call(200, body={'guild_id': GUILD}, path={'uuid': 'request.unknown'},
                    variant=out.CheckoutEmptyResponse),
               Call(422, body=EMPTY_BODY, path={'uuid': 'request.unknown'}))),
    broker_routes.RELEASE: Operation(
        'Release a checked-out track.',
        replies=(reply(200, out.ReleaseResponse),),
        calls=(Call(200, path=UUID, arrange=downloaded),)),
    broker_routes.REMOVE: Operation(
        'Remove a request and its download.',
        replies=(reply(200, out.RemoveResponse),),
        calls=(Call(200, path=UUID, arrange=downloaded),)),
    broker_routes.DISCARD: Operation(
        'Discard a request that will not be downloaded.',
        replies=(reply(200, out.DiscardResponse),),
        calls=(Call(200, path=UUID, arrange=registered),)),
    # --- download and search results ---------------------------------------
    broker_routes.REGISTER_DOWNLOAD: Operation(
        'Report a finished download.', request=DownloadResult,
        replies=(reply(202, out.RegisterDownloadResponse), reply(422, doc='Not a download result.')),
        calls=(Call(202, body=download_result_json()), Call(422, body=EMPTY_BODY))),
    broker_routes.REGISTER_DOWNLOAD_DIRECT: Operation(
        'Register a download the gateway built itself.', request=RegisterDownloadDirectBody,
        replies=(reply(200, out.RegisterDownloadDirectResponse), reply(422, doc='No request in the body.')),
        calls=(Call(200, body=lambda ctx: {'request': media_json(), 'file_path': 'a.mp3',
                                           'ytdl_data': {'id': 'a', 'title': 'a'}}),
               Call(422, body=EMPTY_BODY))),
    broker_routes.NEXT_RESULT: Operation(
        'Take the next download result for the gateway, if there is one.',
        replies=(reply(200, DownloadResult), reply(204, doc='Nothing is waiting.')),
        calls=(Call(200, arrange=result_waiting), Call(204))),
    broker_routes.REGISTER_SEARCH_RESULT: Operation(
        'Report a resolved search.', request=SearchResolution,
        replies=(reply(202, out.RegisterSearchResultResponse), reply(422, doc='Not a search resolution.')),
        calls=(Call(202, body={'media_request': media_json()}), Call(422, body=EMPTY_BODY))),
    broker_routes.NEXT_SEARCH_RESULT: Operation(
        'Take the next resolved search for the gateway, if there is one.',
        replies=(reply(200, SearchResolution), reply(204, doc='Nothing is waiting.')),
        calls=(Call(200, arrange=search_result_waiting), Call(204))),
    # --- video cache -------------------------------------------------------
    broker_routes.CHECK_CACHE: Operation(
        'Ask whether the video cache already holds a request.', request=MediaRequestBody,
        replies=(reply(200, out.CheckCacheHitResponse, out.CheckCacheMissResponse),
                 reply(422, doc='Not a media request.')),
        calls=(Call(200, body=lambda ctx: ctx['request'].model_dump(mode='json'), arrange=cache_hit,
                    variant=out.CheckCacheHitResponse),
               Call(200, body=media_json(), variant=out.CheckCacheMissResponse),
               Call(422, body=EMPTY_BODY))),
    broker_routes.CACHE_CLEANUP: Operation(
        'Evict cache entries nothing is using.',
        replies=(reply(200, out.CacheCleanupResponse),),
        calls=(Call(200),)),
    corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT: Operation(
        'Report a cached object whose bytes failed their digest; the broker re-reads it before evicting.',
        request=ReportCorruptBody,
        replies=(reply(200, out.ReportCorruptCacheObjectResponse),
                 reply(422, doc='key or media_request missing, or not a media request.')),
        calls=(Call(200, body={'key': 'cache/a.sha256-0123456789abcdef0123456789abcdef.pcm',
                               'media_request': media_json()}),
               Call(422, body=EMPTY_BODY))),
    broker_routes.CACHE_COUNT: Operation(
        'Count the cache entries.',
        replies=(reply(200, out.GetCacheCountResponse),),
        calls=(Call(200),)),
    # --- bundles -----------------------------------------------------------
    broker_routes.LIST_BUNDLES: Operation(
        "List the bundles a guild has.", query=(Query('guild_id', int, required=True),),
        replies=(reply(200, out.ListBundlesForGuildResponse), reply(422, doc='guild_id missing or not an integer.')),
        calls=(Call(200, query={'guild_id': GUILD}, arrange=bundle), Call(422))),
    broker_routes.CREATE_BUNDLE: Operation(
        'Open a bundle for a guild.', request=CreateBundleBody,
        replies=(reply(201, out.CreateBundleResponse), reply(422, doc='guild_id or channel_id missing.')),
        calls=(Call(201, body={'guild_id': GUILD, 'channel_id': CHANNEL}), Call(422, body=EMPTY_BODY))),
    broker_routes.FINALIZE_BUNDLE: Operation(
        'Close a bundle to new requests.',
        replies=(reply(200, out.FinalizeBundleResponse),),
        calls=(Call(200, path=UUID, arrange=bundle),)),
    broker_routes.DELETE_BUNDLE: Operation(
        'Delete a bundle.',
        replies=(reply(200, out.DeleteBundleResponse),),
        calls=(Call(200, path=UUID, arrange=bundle),)),
    # --- player sessions ---------------------------------------------------
    broker_routes.LIST_SESSIONS: Operation(
        'List the stored player sessions.',
        replies=(reply(200, out.ListPlayerSessionsResponse),),
        calls=(Call(200, arrange=session),)),
    broker_routes.SAVE_SESSION: Operation(
        "Store a guild's player session.", request=PlayerSession, path={'guild_id': int},
        replies=(reply(201, out.SavePlayerSessionResponse),
                 reply(422, doc='Not a session, or its guild_id differs from the path.')),
        calls=(Call(201, body=player_session().model_dump(mode='json'), path=GUILD_PATH),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    broker_routes.DELETE_SESSION: Operation(
        "Delete a guild's player session.", path={'guild_id': int},
        replies=(reply(200, out.DeletePlayerSessionResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=session), BAD_GUILD)),
    # --- guild queue -------------------------------------------------------
    queue_routes.ENQUEUE_TRACK: Operation(
        "Add a downloaded track to a guild's queue.", request=EnqueueBody, path={'guild_id': int},
        replies=(reply(200, out.EnqueueTrackResponse), reply(422, doc='No uuid, or guild_id is not an integer.')),
        calls=(Call(200, body=tracks(), path=GUILD_PATH, arrange=downloaded),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.GET_GUILD_QUEUE: Operation(
        "Read a guild's queue, what is playing, and the markers a poller watches.", path={'guild_id': int},
        replies=(reply(200, out.GuildQueueResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=playing), BAD_GUILD)),
    queue_routes.REMOVE_QUEUED_TRACK: Operation(
        'Take a track out of the queue.', request=TrackBody, path={'guild_id': int},
        replies=(reply(200, out.RemoveQueuedTrackResponse), reply(422, doc='No uuid, or guild_id is not an integer.')),
        calls=(Call(200, body=tracks(), path=GUILD_PATH, arrange=queued),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.BUMP_QUEUED_TRACK: Operation(
        'Move a queued track to the front.', request=TrackBody, path={'guild_id': int},
        replies=(reply(200, out.BumpQueuedTrackResponse), reply(422, doc='No uuid, or guild_id is not an integer.')),
        calls=(Call(200, body=tracks('second'), path=GUILD_PATH, arrange=two_queued),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.SHUFFLE_QUEUE: Operation(
        "Shuffle a guild's queue.", path={'guild_id': int},
        replies=(reply(200, out.ShuffleQueueResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=two_queued), BAD_GUILD)),
    queue_routes.CLEAR_QUEUE: Operation(
        "Drop every queued track for a guild.", path={'guild_id': int},
        replies=(reply(200, out.ClearQueueResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=queued), BAD_GUILD)),
    queue_routes.POLL_GUILD_QUEUE: Operation(
        'The cheap change check: the queue version and any pending skip.', path={'guild_id': int},
        query=(Query('since', int, description='The version the caller last saw.'),),
        replies=(reply(200, out.PollGuildQueueResponse),
                 reply(204, doc='The queue is still at `since` and no skip is pending.'),
                 reply(422, doc='guild_id or since is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=queued, query={'since': -1}),
               Call(204, path=GUILD_PATH, arrange=queued_at_version,
                    query={'since': lambda ctx: ctx['version']}),
               Call(422, path=GUILD_PATH, query={'since': 'abc'}))),
    queue_routes.CLAIM_TRACK: Operation(
        'Take the next queued track as the one playing.', request=ClaimBody, path={'guild_id': int},
        replies=(reply(200, out.ClaimTrackHitResponse, out.ClaimTrackMissResponse),
                 reply(422, doc='No gateway_id, or guild_id is not an integer.')),
        calls=(Call(200, body={'gateway_id': GATEWAY}, path=GUILD_PATH, arrange=queued,
                    variant=out.ClaimTrackHitResponse),
               Call(200, body={'gateway_id': GATEWAY}, path=GUILD_PATH, variant=out.ClaimTrackMissResponse),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.PLAYING_HEARTBEAT: Operation(
        'Say the playing track is still playing.', request=TrackBody, path={'guild_id': int},
        replies=(reply(200, out.PlayingHeartbeatResponse), reply(422, doc='No uuid, or guild_id is not an integer.')),
        calls=(Call(200, body=tracks(), path=GUILD_PATH, arrange=playing),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.SKIP_TRACK: Operation(
        'Mark the playing track to be skipped.', request=TrackBody, path={'guild_id': int},
        replies=(reply(200, out.SkipTrackResponse), reply(422, doc='No uuid, or guild_id is not an integer.')),
        calls=(Call(200, body=tracks(), path=GUILD_PATH, arrange=playing),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.FINISH_TRACK: Operation(
        'Record that the playing track ended.', request=FinishBody, path={'guild_id': int},
        replies=(reply(200, out.FinishTrackResponse), reply(422, doc='A field is missing, or guild_id is not an integer.')),
        calls=(Call(200, body=lambda ctx: {'uuid': ctx['uuid'], 'skipped': False, 'history_cap': 50},
                    path=GUILD_PATH, arrange=playing),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    queue_routes.GET_GUILD_HISTORY: Operation(
        'The tracks that played to the end.', path={'guild_id': int},
        replies=(reply(200, out.GuildHistoryResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH), BAD_GUILD)),
    queue_routes.CLOSE_GUILD: Operation(
        "Shut down a guild's player state.", path={'guild_id': int},
        replies=(reply(200, out.CloseGuildResponse), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(200, path=GUILD_PATH, arrange=queued), BAD_GUILD)),
    queue_routes.OPEN_GUILD: Operation(
        "Open a guild's player state, recovering a track the last owner left unfinished.",
        request=OpenGuildBody, path={'guild_id': int},
        replies=(reply(200, out.OpenGuildResponse), reply(422, doc='No text_channel_id, or guild_id is not an integer.')),
        calls=(Call(200, body={'text_channel_id': CHANNEL}, path=GUILD_PATH),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    # --- gateway intents ---------------------------------------------------
    intent_routes.SUBMIT_INTENT: Operation(
        'Ask the gateway to do something in a guild.', request=SubmitIntentBody, path={'guild_id': int},
        replies=(reply(200, out.SubmitIntentResponse), reply(422, doc='Unknown kind, or guild_id is not an integer.')),
        calls=(Call(200, body={'kind': 'join'}, path=GUILD_PATH),
               Call(422, body=EMPTY_BODY, path=GUILD_PATH))),
    intent_routes.CLAIM_INTENTS: Operation(
        'Take the intents waiting for this gateway.',
        replies=(reply(200, out.ClaimIntentsResponse), reply(204, doc='Nothing is waiting.')),
        calls=(Call(200, path={'gateway_id': GATEWAY}, arrange=intent),
               Call(204, path={'gateway_id': GATEWAY}))),
    intent_routes.COMPLETE_INTENT: Operation(
        'Report how an intent went.', request=CompleteIntentBody,
        replies=(reply(200, out.CompleteIntentResponse), reply(422, doc='ok is missing.')),
        calls=(Call(200, body={'ok': True}, path={'intent_id': lambda ctx: ctx['id']}, arrange=claimed_intent),
               Call(422, body=EMPTY_BODY, path={'intent_id': 'unknown'}))),
    intent_routes.GET_INTENT_RESULT: Operation(
        "Read an intent's outcome.",
        replies=(reply(200, out.IntentResultResponse),),
        calls=(Call(200, path={'intent_id': lambda ctx: ctx['id']}, arrange=completed_intent),)),
}
