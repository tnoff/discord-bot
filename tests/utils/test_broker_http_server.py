"""
Tests for BrokerHTTPServer.

Uses real asyncio TCP connections (same pattern as test_health_server.py).
Each test class uses a distinct port to avoid conflicts.
"""
import asyncio
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from discord_bot.utils.broker_http_server import BrokerHTTPServer, BrokerResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _post(port: int, path: str, headers: dict, body: bytes) -> str:
    """Send a raw HTTP POST and return the full response as a string."""
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    header_lines = f'POST {path} HTTP/1.1\r\nHost: localhost\r\n'
    for k, v in headers.items():
        header_lines += f'{k}: {v}\r\n'
    header_lines += f'Content-Length: {len(body)}\r\n\r\n'
    writer.write(header_lines.encode('utf-8') + body)
    await writer.drain()
    try:
        response = await asyncio.wait_for(reader.read(8192), timeout=3)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
    return response.decode('utf-8', errors='replace')


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


# ---------------------------------------------------------------------------
# Test: successful upload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_success():
    """Valid upload with correct MD5 → 200, BrokerResult queued."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18800, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'hello audio data'
            ytdlp = {'title': 'Test Song', 'ext': 'mp3', 'id': 'abc123',
                     'webpage_url': 'https://example.com/v', 'uploader': 'Artist',
                     'duration': 120, 'extractor': 'youtube'}
            headers = {
                'X-Request-Id': 'req-uuid-001',
                'X-Guild-Id': '42',
                'X-Content-MD5': _md5(body),
                'X-Ytdlp-Data': json.dumps(ytdlp),
                'Content-Type': 'application/octet-stream',
            }
            response = await _post(18800, '/upload', headers, body)
            assert '200' in response

            result: BrokerResult = result_queue.get_nowait()
            assert result.media_request_uuid == 'req-uuid-001'
            assert result.guild_id == 42
            assert result.file_path is not None
            assert result.file_path.exists()
            assert result.file_path.read_bytes() == body
            assert result.error_message is None
            assert result.ytdlp_data.get('title') == 'Test Song'
        finally:
            await server.stop()


@pytest.mark.asyncio
async def test_upload_file_atomically_renamed():
    """No temp file remains after a successful upload."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18801, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'audio bytes'
            headers = {
                'X-Request-Id': 'req-uuid-002',
                'X-Guild-Id': '1',
                'X-Content-MD5': _md5(body),
            }
            await _post(18801, '/upload', headers, body)
            result: BrokerResult = result_queue.get_nowait()
            # Confirm no .tmp. file lingers
            tmp_files = list(Path(tmp_dir).rglob('.tmp.*'))
            assert not tmp_files, f'Leftover temp files: {tmp_files}'
            assert result.file_path.exists()
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: MD5 mismatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_md5_mismatch_returns_400():
    """Corrupt body detected by MD5 check → 400, nothing queued, no file."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18802, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'real data'
            headers = {
                'X-Request-Id': 'req-uuid-003',
                'X-Guild-Id': '1',
                'X-Content-MD5': 'deadbeefdeadbeefdeadbeefdeadbeef',  # wrong
            }
            response = await _post(18802, '/upload', headers, body)
            assert '400' in response
            assert result_queue.empty()
            # No leftover files
            assert not list(Path(tmp_dir).rglob('*.mp3'))
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: missing required headers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_missing_request_id_returns_400():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18803, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'data'
            headers = {'X-Guild-Id': '1', 'X-Content-MD5': _md5(body)}
            response = await _post(18803, '/upload', headers, body)
            assert '400' in response
            assert result_queue.empty()
        finally:
            await server.stop()


@pytest.mark.asyncio
async def test_upload_missing_guild_id_returns_400():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18804, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'data'
            headers = {'X-Request-Id': 'req-001', 'X-Content-MD5': _md5(body)}
            response = await _post(18804, '/upload', headers, body)
            assert '400' in response
            assert result_queue.empty()
        finally:
            await server.stop()


@pytest.mark.asyncio
async def test_upload_missing_md5_returns_400():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18805, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'data'
            headers = {'X-Request-Id': 'req-001', 'X-Guild-Id': '1'}
            response = await _post(18805, '/upload', headers, body)
            assert '400' in response
            assert result_queue.empty()
        finally:
            await server.stop()


@pytest.mark.asyncio
async def test_upload_invalid_guild_id_returns_400():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18806, Path(tmp_dir), result_queue)
        await server.start()
        try:
            body = b'data'
            headers = {
                'X-Request-Id': 'req-001',
                'X-Guild-Id': 'not-an-int',
                'X-Content-MD5': _md5(body),
            }
            response = await _post(18806, '/upload', headers, body)
            assert '400' in response
            assert result_queue.empty()
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: /upload/error endpoint
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_endpoint_queues_broker_result():
    """Worker-reported failure creates a BrokerResult with error_message."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18807, Path(tmp_dir), result_queue)
        await server.start()
        try:
            error_body = b'Video unavailable'
            headers = {
                'X-Request-Id': 'req-err-001',
                'X-Guild-Id': '7',
            }
            response = await _post(18807, '/upload/error', headers, error_body)
            assert '200' in response

            result: BrokerResult = result_queue.get_nowait()
            assert result.media_request_uuid == 'req-err-001'
            assert result.guild_id == 7
            assert result.file_path is None
            assert 'Video unavailable' in result.error_message
        finally:
            await server.stop()


@pytest.mark.asyncio
async def test_error_endpoint_missing_headers_returns_400():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18808, Path(tmp_dir), result_queue)
        await server.start()
        try:
            response = await _post(18808, '/upload/error', {}, b'some error')
            assert '400' in response
            assert result_queue.empty()
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: unknown path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_path_returns_404():
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18809, Path(tmp_dir), result_queue)
        await server.start()
        try:
            response = await _post(18809, '/nonexistent', {}, b'')
            assert '404' in response
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: multi-chunk upload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_multi_chunk_body():
    """Large body (multiple read chunks) is assembled and MD5-verified correctly."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18810, Path(tmp_dir), result_queue)
        await server.start()
        try:
            # 200 KB — forces multiple 64 KB reads
            body = b'x' * (200 * 1024)
            headers = {
                'X-Request-Id': 'req-big-001',
                'X-Guild-Id': '99',
                'X-Content-MD5': _md5(body),
            }
            response = await _post(18810, '/upload', headers, body)
            assert '200' in response

            result: BrokerResult = result_queue.get_nowait()
            assert result.file_path.read_bytes() == body
        finally:
            await server.stop()


# ---------------------------------------------------------------------------
# Test: server start / stop lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stop_closes_server():
    """After stop(), the port is released and connections are refused."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18811, Path(tmp_dir), result_queue)
        await server.start()
        await server.stop()
        # Connecting should now fail
        with pytest.raises((ConnectionRefusedError, OSError)):
            await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', 18811),
                timeout=1,
            )


@pytest.mark.asyncio
async def test_stop_idempotent():
    """Calling stop() twice does not raise."""
    result_queue: asyncio.Queue[BrokerResult] = asyncio.Queue()
    with TemporaryDirectory() as tmp_dir:
        server = BrokerHTTPServer('127.0.0.1', 18812, Path(tmp_dir), result_queue)
        await server.start()
        await server.stop()
        await server.stop()  # should not raise
