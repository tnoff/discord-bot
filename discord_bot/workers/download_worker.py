"""
HA Download Worker — standalone process that consumes MediaRequest items from
Redis Streams, downloads them via yt-dlp, and streams the result back to the
bot's BrokerHTTPServer.

Usage:
    discord-download-worker --config /path/to/config.yaml [--worker-id worker-0]

The config file uses the same YAML format as the bot.  Only the music.download
and music.ha sections are required.

Environment variable override:
    DISCORD_BOT_CONFIG — path to config file (overridden by --config flag)
"""
import asyncio  # pylint: disable=duplicate-code
import hashlib
import json
import logging
import os
import signal
import uuid
from asyncio import get_running_loop
from pathlib import Path
from tempfile import TemporaryDirectory

import click
from pyaml_env import parse_config
from yt_dlp import YoutubeDL

from discord_bot.cogs.music_helpers.download_client import DownloadClient, match_generator
from discord_bot.cogs.music import MusicConfig
from discord_bot.types.media_request import MediaRequest

logger = logging.getLogger('download_worker')

CHUNK_SIZE = 65536  # 64 KB
REDIS_STREAM_PREFIX = 'music:download_queue'
REDIS_BLOCKED_SET = 'music:blocked_guilds'

# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------


async def process_task(
    media_request: MediaRequest,
    broker_url: str,
    download_client: DownloadClient,
    max_retries: int,
) -> None:
    '''
    Download a single media request and POST the file (or an error) to the broker.
    '''
    import aiohttp  # pylint: disable=import-outside-toplevel

    request_uuid = str(media_request.uuid)
    guild_id = media_request.guild_id

    loop = get_running_loop()
    result = await download_client.create_source(media_request, max_retries, loop)

    if not result.status.success:
        # Report failure to broker
        error_msg = result.status.user_message or result.status.error_detail or 'Download failed'
        await _post_error(broker_url, request_uuid, guild_id, error_msg)
        return

    file_path = result.file_name
    if not file_path or not Path(file_path).exists():
        await _post_error(broker_url, request_uuid, guild_id, 'File not found after download')
        return

    ytdlp_data = result.ytdlp_data or {}

    # First pass: compute MD5
    md5 = hashlib.md5()
    with open(file_path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b''):
            md5.update(chunk)
    md5_hex = md5.hexdigest()

    file_size = os.path.getsize(file_path)

    headers = {
        'X-Request-Id': request_uuid,
        'X-Guild-Id': str(guild_id),
        'X-Content-MD5': md5_hex,
        'X-Ytdlp-Data': json.dumps(ytdlp_data),
        'Content-Type': 'application/octet-stream',
        'Content-Length': str(file_size),
    }

    async def _streamer():
        with open(file_path, 'rb') as fh:
            while True:
                chunk = fh.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f'{broker_url}/upload',
                data=_streamer(),
                headers=headers,
            )
            if resp.status != 200:
                body = await resp.text()
                logger.error(f'Broker rejected upload for {request_uuid}: {resp.status} {body}')
            else:
                logger.info(f'Successfully uploaded {file_path} for request {request_uuid}')
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f'Failed to POST file to broker for {request_uuid}: {exc}')
    finally:
        # Clean up the local file regardless
        try:
            Path(file_path).unlink(missing_ok=True)
        except OSError:
            pass


async def _post_error(broker_url: str, request_uuid: str, guild_id: int, message: str) -> None:
    '''Report a download failure to the broker's /upload/error endpoint.'''
    import aiohttp  # pylint: disable=import-outside-toplevel

    body = message.encode('utf-8')
    headers = {
        'X-Request-Id': request_uuid,
        'X-Guild-Id': str(guild_id),
        'Content-Type': 'text/plain',
        'Content-Length': str(len(body)),
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(f'{broker_url}/upload/error', data=body, headers=headers)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(f'Failed to POST error to broker for {request_uuid}: {exc}')


def _parse_guild_id_from_stream(stream_name: str) -> int | None:
    '''Extract guild_id from stream key music:download_queue:{guild_id}.'''
    parts = stream_name.split(':')
    if len(parts) >= 3:
        try:
            return int(parts[-1])
        except ValueError:
            pass
    return None


async def worker_loop(
    redis_client,
    broker_url: str,
    download_client: DownloadClient,
    consumer_group: str,
    worker_id: str,
    max_retries: int,
    shutdown_event: asyncio.Event,
) -> None:
    '''
    Main loop: read from all guild streams in the consumer group, process each task.
    '''
    # XREADGROUP pattern for all guild streams — Redis doesn't support glob on XREADGROUP
    # directly, so we poll streams for all known guild IDs.  We also do a periodic
    # SCAN to discover new guild streams.
    known_streams: set[str] = set()

    async def _refresh_streams():
        '''Scan Redis for all music:download_queue:* keys.'''
        cursor = 0
        while True:
            cursor, keys = await redis_client.scan(cursor, match=f'{REDIS_STREAM_PREFIX}:*', count=100)
            for key in keys:
                if key not in known_streams:
                    known_streams.add(key)
                    # Create consumer group on this stream if not already present
                    try:
                        await redis_client.xgroup_create(key, consumer_group, id='0', mkstream=False)
                    except Exception:  # pylint: disable=broad-except
                        pass  # Group already exists
            if cursor == 0:
                break

    last_scan = 0.0

    while not shutdown_event.is_set():
        now = asyncio.get_event_loop().time()

        # Refresh known streams every 10 seconds
        if now - last_scan >= 10.0:
            await _refresh_streams()
            last_scan = now

        if not known_streams:
            await asyncio.sleep(1)
            continue

        # Build stream dict: {stream_key: '>'} for undelivered messages
        streams_arg = {s: '>' for s in known_streams}

        try:
            messages = await redis_client.xreadgroup(
                consumer_group,
                worker_id,
                streams_arg,
                count=1,
                block=5000,  # ms
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f'XREADGROUP error: {exc}')
            await asyncio.sleep(1)
            continue

        if not messages:
            continue

        for stream_name, msgs in messages:
            guild_id = _parse_guild_id_from_stream(stream_name)

            for msg_id, data in msgs:
                try:
                    # Check if guild is blocked before processing
                    if guild_id and await redis_client.sismember(REDIS_BLOCKED_SET, str(guild_id)):
                        logger.info(f'Guild {guild_id} is blocked, discarding message {msg_id}')
                        await redis_client.xack(stream_name, consumer_group, msg_id)
                        continue

                    raw = data.get('payload', '{}')
                    media_request = MediaRequest.deserialize(raw)

                    logger.info(
                        f'Processing request {media_request.uuid} '
                        f'for guild {media_request.guild_id}: {media_request}'
                    )

                    await process_task(media_request, broker_url, download_client, max_retries)

                except Exception as exc:  # pylint: disable=broad-except
                    logger.error(f'Error processing message {msg_id}: {exc}')
                    # Attempt to report error to broker if we have a request UUID
                    try:
                        item_data = json.loads(data.get('payload', '{}'))
                        req_uuid = item_data.get('uuid', 'unknown')
                        gid = guild_id or int(item_data.get('guild_id', 0))
                        await _post_error(broker_url, req_uuid, gid, str(exc))
                    except Exception:  # pylint: disable=broad-except
                        pass
                finally:
                    # Always acknowledge so messages don't pile up in PEL
                    try:
                        await redis_client.xack(stream_name, consumer_group, msg_id)
                    except Exception:  # pylint: disable=broad-except
                        pass

    logger.info('Worker loop exiting')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def worker_main(config_path: str, worker_id: str) -> None:
    '''Set up the worker and run the main loop.'''
    settings = parse_config(config_path)
    music_settings = settings.get('music', {})
    music_config = MusicConfig.model_validate(music_settings)
    ha_config = music_config.ha

    if not ha_config.enabled:
        logger.warning('music.ha.enabled is False — worker has nothing to do, exiting')
        return

    import redis.asyncio as aioredis  # pylint: disable=import-outside-toplevel

    redis_client = aioredis.from_url(ha_config.redis_url, decode_responses=True)

    # Build a download directory (temp, since we stream the file to the broker immediately)
    tmp_dir = TemporaryDirectory()  # pylint: disable=consider-using-with
    download_dir = Path(tmp_dir.name)
    temp_download_dir = Path(TemporaryDirectory().name)  # pylint: disable=consider-using-with
    temp_download_dir.mkdir(exist_ok=True)

    # Build yt-dlp instance matching the bot's configuration
    dl_cfg = music_config.download
    ytdlopts = {
        'format': 'bestaudio/best',
        'restrictfilenames': True,
        'noplaylist': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'logtostderr': False,
        'logger': logger,
        'default_search': 'auto',
        'source_address': '0.0.0.0',
        'outtmpl': str(temp_download_dir / '%(extractor)s.%(id)s.%(ext)s'),
    }
    for key, val in dl_cfg.extra_ytdlp_options.items():
        ytdlopts[key] = val
    if dl_cfg.max_video_length or dl_cfg.banned_videos_list:
        ytdlopts['match_filter'] = match_generator(dl_cfg.max_video_length, dl_cfg.banned_videos_list)

    ytdl = YoutubeDL(ytdlopts)
    download_client = DownloadClient(ytdl, download_dir)

    broker_url = f'http://{ha_config.broker_host}:{ha_config.broker_port}'
    # Allow override via env var if broker is on a different host
    broker_url = os.environ.get('BROKER_URL', broker_url)

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    logger.info(f'Worker {worker_id} starting — broker={broker_url} redis={ha_config.redis_url}')

    try:
        await worker_loop(
            redis_client=redis_client,
            broker_url=broker_url,
            download_client=download_client,
            consumer_group=ha_config.worker_consumer_group,
            worker_id=worker_id,
            max_retries=dl_cfg.max_download_retries,
            shutdown_event=shutdown_event,
        )
    finally:
        await redis_client.aclose()
        tmp_dir.cleanup()
        logger.info(f'Worker {worker_id} shut down cleanly')


@click.command()
@click.option('--config', envvar='DISCORD_BOT_CONFIG', required=True,
              type=click.Path(exists=True), help='Path to bot config YAML file')
@click.option('--worker-id', default=None, help='Unique worker identifier (auto-generated if omitted)')
def main(config: str, worker_id: str | None) -> None:
    '''HA download worker for the Discord music bot.'''
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    if worker_id is None:
        worker_id = f'worker-{uuid.uuid4().hex[:8]}'
    asyncio.run(worker_main(config, worker_id))
