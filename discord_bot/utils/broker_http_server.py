# pylint: disable=duplicate-code
"""
Minimal asyncio HTTP server that receives completed downloads from the HA worker.

The worker streams the downloaded file to POST /upload with MD5 verification.
On success the server places a BrokerResult into a result queue consumed by
the music cog's ha_download_result_loop.
"""
import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

CHUNK_SIZE = 65536  # 64 KB read/write chunks


@dataclass
class BrokerResult:
    '''
    Result produced by BrokerHTTPServer after a successful upload.

    media_request_uuid : The MediaRequest.uuid string (from X-Request-Id header)
    guild_id           : Guild that requested the download
    file_path          : Final path of the downloaded file on disk
    ytdlp_data         : yt-dlp metadata dict (parsed from X-Ytdlp-Data header)
    error_message      : Set when the worker reports a failure instead of a file
    '''
    media_request_uuid: str
    guild_id: int
    file_path: Path | None
    ytdlp_data: dict = field(default_factory=dict)
    error_message: str | None = None


class BrokerHTTPServer:
    '''
    Lightweight asyncio HTTP server for receiving worker uploads.

    Endpoints:
        POST /upload        — streamed file upload with MD5 verification
        POST /upload/error  — worker-reported download failure

    Files are written to a temp path first, then atomically renamed on MD5
    match, so partial uploads are never visible to the music cog.
    '''

    def __init__(self, host: str, port: int, download_dir: Path, result_queue: asyncio.Queue):
        self._host = host
        self._port = port
        self._download_dir = download_dir
        self._result_queue = result_queue
        self._server = None
        self.logger = logging.getLogger('broker_http_server')

    async def start(self) -> None:
        '''Start the asyncio TCP server.'''
        self._server = await asyncio.start_server(
            self._handle_connection, self._host, self._port
        )
        self.logger.info(f'Broker HTTP server listening on {self._host}:{self._port}')

    async def stop(self) -> None:
        '''Stop the server and wait for pending connections to close.'''
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            self.logger.info('Broker HTTP server stopped')

    # ------------------------------------------------------------------
    # Internal: connection handler
    # ------------------------------------------------------------------

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        '''Handle one HTTP connection (one request per connection).'''
        try:
            await self._process_request(reader, writer)
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.error(f'Broker server unhandled error: {exc}')
            await self._send_response(writer, 500, 'Internal Server Error')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # pylint: disable=broad-except
                pass

    async def _process_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        '''Parse the HTTP request and dispatch to the appropriate handler.'''
        # Read the request line
        request_line_bytes = await reader.readline()
        if not request_line_bytes:
            return
        request_line = request_line_bytes.decode('utf-8', errors='replace').strip()

        # Parse headers into a dict
        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            decoded = line.decode('utf-8', errors='replace').strip()
            if not decoded:
                break
            if ':' in decoded:
                key, _, value = decoded.partition(':')
                headers[key.strip().lower()] = value.strip()

        # Route by method + path
        parts = request_line.split(' ')
        if len(parts) < 2:
            await self._send_response(writer, 400, 'Bad Request')
            return

        method, path = parts[0], parts[1]

        if method == 'POST' and path == '/upload':
            await self._handle_upload(reader, writer, headers)
        elif method == 'POST' and path == '/upload/error':
            await self._handle_error(reader, writer, headers)
        else:
            await self._send_response(writer, 404, 'Not Found')

    # ------------------------------------------------------------------
    # Upload handler
    # ------------------------------------------------------------------

    async def _handle_upload(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> None:
        '''
        Receive a streamed file upload from the worker.

        Expected headers:
            X-Request-Id   : MediaRequest.uuid
            X-Guild-Id     : guild_id (int string)
            X-Content-MD5  : hex MD5 of the full file body
            X-Ytdlp-Data   : JSON-encoded yt-dlp metadata dict
            Content-Length : byte length of the body
        '''
        request_uuid = headers.get('x-request-id', '').strip()
        guild_id_str = headers.get('x-guild-id', '').strip()
        expected_md5 = headers.get('x-content-md5', '').strip()
        ytdlp_data_raw = headers.get('x-ytdlp-data', '{}').strip()
        content_length_str = headers.get('content-length', '0').strip()

        # Validate required headers
        if not request_uuid or not guild_id_str or not expected_md5:
            await self._send_response(writer, 400, 'Missing required headers')
            return

        try:
            guild_id = int(guild_id_str)
        except ValueError:
            await self._send_response(writer, 400, 'Invalid X-Guild-Id')
            return

        try:
            content_length = int(content_length_str)
        except ValueError:
            content_length = 0

        import json  # pylint: disable=import-outside-toplevel
        try:
            ytdlp_data = json.loads(ytdlp_data_raw) if ytdlp_data_raw else {}
        except (json.JSONDecodeError, TypeError):
            ytdlp_data = {}

        # Determine file name from yt-dlp data or uuid fallback
        file_ext = ''
        if ytdlp_data.get('ext'):
            file_ext = f'.{ytdlp_data["ext"]}'
        elif ytdlp_data.get('_filename'):
            file_ext = Path(ytdlp_data['_filename']).suffix

        guild_dir = self._download_dir / str(guild_id)
        guild_dir.mkdir(parents=True, exist_ok=True)

        temp_path = guild_dir / f'.tmp.{request_uuid}{file_ext}'
        final_path = guild_dir / f'{request_uuid}{file_ext}'

        md5 = hashlib.md5()
        bytes_written = 0

        try:
            with open(temp_path, 'wb') as fh:
                remaining = content_length if content_length > 0 else None
                while True:
                    if remaining is not None:
                        to_read = min(CHUNK_SIZE, remaining)
                        if to_read == 0:
                            break
                    else:
                        to_read = CHUNK_SIZE

                    chunk = await reader.read(to_read)
                    if not chunk:
                        break

                    fh.write(chunk)
                    md5.update(chunk)
                    bytes_written += len(chunk)

                    if remaining is not None:
                        remaining -= len(chunk)

        except OSError as exc:
            self.logger.error(f'Error writing upload for {request_uuid}: {exc}')
            temp_path.unlink(missing_ok=True)
            await self._send_response(writer, 500, 'Write error')
            return

        # Verify MD5
        actual_md5 = md5.hexdigest()
        if actual_md5 != expected_md5:
            self.logger.warning(
                f'MD5 mismatch for {request_uuid}: expected={expected_md5} actual={actual_md5}'
            )
            temp_path.unlink(missing_ok=True)
            await self._send_response(writer, 400, 'MD5 mismatch')
            return

        # Atomic rename
        try:
            os.rename(temp_path, final_path)
        except OSError as exc:
            self.logger.error(f'Rename failed for {request_uuid}: {exc}')
            temp_path.unlink(missing_ok=True)
            await self._send_response(writer, 500, 'Rename error')
            return

        self.logger.info(f'Received {bytes_written} bytes for request {request_uuid} → {final_path}')

        result = BrokerResult(
            media_request_uuid=request_uuid,
            guild_id=guild_id,
            file_path=final_path,
            ytdlp_data=ytdlp_data,
        )
        await self._result_queue.put(result)
        await self._send_response(writer, 200, 'OK')

    # ------------------------------------------------------------------
    # Error handler
    # ------------------------------------------------------------------

    async def _handle_error(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        headers: dict[str, str],
    ) -> None:
        '''
        Receive a worker-reported download failure.

        Expected headers:
            X-Request-Id : MediaRequest.uuid
            X-Guild-Id   : guild_id (int string)
            Content-Type : text/plain (body is the user-facing error message)
        '''
        request_uuid = headers.get('x-request-id', '').strip()
        guild_id_str = headers.get('x-guild-id', '').strip()
        content_length_str = headers.get('content-length', '0').strip()

        if not request_uuid or not guild_id_str:
            await self._send_response(writer, 400, 'Missing required headers')
            return

        try:
            guild_id = int(guild_id_str)
        except ValueError:
            await self._send_response(writer, 400, 'Invalid X-Guild-Id')
            return

        try:
            content_length = int(content_length_str)
        except ValueError:
            content_length = 0

        error_message = ''
        if content_length > 0:
            body = await reader.read(min(content_length, 4096))
            error_message = body.decode('utf-8', errors='replace').strip()

        self.logger.warning(f'Worker reported error for {request_uuid}: {error_message}')

        result = BrokerResult(
            media_request_uuid=request_uuid,
            guild_id=guild_id,
            file_path=None,
            error_message=error_message or 'Worker reported an unknown error',
        )
        await self._result_queue.put(result)
        await self._send_response(writer, 200, 'OK')

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _send_response(writer: asyncio.StreamWriter, status: int, message: str) -> None:
        '''Write a minimal HTTP response and flush.'''
        body = message.encode('utf-8')
        response = (
            f'HTTP/1.1 {status} {message}\r\n'
            f'Content-Length: {len(body)}\r\n'
            f'Content-Type: text/plain\r\n'
            f'Connection: close\r\n'
            f'\r\n'
        ).encode('utf-8') + body
        writer.write(response)
        await writer.drain()
