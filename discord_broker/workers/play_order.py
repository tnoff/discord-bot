'''
The play-order message: what a guild's channel shows of its queue.

A port of MusicPlayer.get_queue_order_messages, moved here when the queue moved to the broker, so
the broker can render the message whenever the queue changes instead of the gateway re-rendering
it after every command.  The output is meant to be byte-for-byte what the gateway produced:
the same "Now playing" line, the same table, the same wait-time arithmetic.

Pure on purpose: it reads a GuildQueue and returns the message contents, so it is testable
without Redis or a dispatcher.
'''
from datetime import timedelta
from re import sub

from dappertable import DapperTable, Column, Columns, PaginationLength

from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.types.media_download import MediaDownload

from discord_broker.interfaces.broker_protocols import GuildQueue


def _playing_download(queue: GuildQueue) -> MediaDownload | None:
    '''The playing track's download, if the broker still has it.'''
    if queue.playing and queue.playing.entry and queue.playing.entry.download:
        return queue.playing.entry.download
    return None


def play_order_messages(queue: GuildQueue) -> list[str]:
    '''
    The message contents for a guild's queue: a "Now playing" line (when something is), then the
    queue as a table with the wait time before each track.

    Empty when nothing is playing and nothing is queued, which the dispatcher reads as "take the
    message down".
    '''
    playing = _playing_download(queue)
    # The now playing line is its own message: the video embed lands right under it, before the
    # rest of the queue.
    messages = []
    if playing:
        messages.append(f'Now playing {playing.webpage_url} requested by '
                        f'{playing.media_request.requester_name}')
    queued = [entry.download for entry in queue.items if entry.download]
    if not queued:
        return messages

    table = DapperTable(
        columns=Columns([
            Column('Pos', 3, zero_pad=True),
            Column('Wait Time', 9),
            Column('Title', 48),
            Column('Uploader', 48),
        ]),
        pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
        enclosure_start='```', enclosure_end='```',
    )
    wait = int(playing.duration) if playing and playing.duration else 0
    for count, download in enumerate(queued):
        delta_string = sub(r'^0:(?=\d{2}:\d{2})', '', str(timedelta(seconds=wait)))
        wait += int(download.duration) if download.duration else 0
        table.add_row([f'{count + 1}', delta_string, f'{download.title}', f'{download.uploader or ""}'])
    rendered = table.render()
    if not isinstance(rendered, list):
        rendered = [rendered]
    return messages + rendered
