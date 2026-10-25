'''
The arguments each command takes, as the gateway's stub parsed them.

discord.py has already done the parsing, converters and usage errors by the time a stub runs, so
these are the typed values (`int`, `str`, the `*rest` string) and nothing is parsed again on the
other side. `COMMAND_ARGS` maps a command's dotted name to its model; it is the list of commands
the commands pod serves, and `POST /commands/{command}` validates against it.

Names are the discord.py qualified names with the group joined by a dot: `playlist.item-add`.

**Commands that take nothing share `NoArgs`.** Unlike the dispatch responses (routes/dispatch.py
keeps five identical `{'status': 'ok'}` models apart because each could grow a field), a command
with no arguments has no shape to drift: the day one gains an argument it gets its own model, and
that is a change to this file either way.

Coercion is pydantic's default, matching the dispatch bodies: `"3"` for an int is accepted. Unknown
fields are refused (`extra='forbid'`), so a stub that sends an argument the pod does not know fails
loudly on the first call instead of being dropped.
'''
from pydantic import BaseModel, ConfigDict


class _Args(BaseModel):
    model_config = ConfigDict(extra='forbid')


class NoArgs(_Args):
    '''A command that takes no arguments.'''


class RollArgs(_Args):
    '''roll'''
    input_value: str


class UrbanArgs(_Args):
    '''urban'''
    word: str


class MarkovOnArgs(_Args):
    '''markov.on. The gateway knows the channel's type from the invocation, so the commands pod
    does not have to fetch the channel to learn whether markov can run there.'''
    channel_is_text_or_voice: bool


class MarkovSpeakArgs(_Args):
    '''markov.speak'''
    first_word: str | None = ''
    sentence_length: int | None = 32


class PlayArgs(_Args):
    '''play'''
    search: str


class QueueIndexArgs(_Args):
    '''remove, bump. The index is a string because the logic validates it and replies on a bad one.'''
    queue_index: str


class PlaylistCreateArgs(_Args):
    '''playlist.create'''
    name: str


class PlaylistItemAddArgs(_Args):
    '''playlist.item-add. The playlist index is a string for the same reason as QueueIndexArgs.'''
    playlist_index: str
    search: str


class PlaylistItemRemoveArgs(_Args):
    '''playlist.item-remove'''
    playlist_index: int
    video_index: int


class PlaylistIndexArgs(_Args):
    '''playlist.show, playlist.delete, playlist.queue'''
    playlist_index: int


class PlaylistRenameArgs(_Args):
    '''playlist.rename'''
    playlist_index: int
    playlist_name: str


class PlaylistSaveArgs(_Args):
    '''playlist.save-queue, playlist.save-history'''
    name: str


class PlaylistMergeArgs(_Args):
    '''playlist.merge'''
    playlist_index_one: str
    playlist_index_two: str


class RoleUsersArgs(_Args):
    '''role.users'''
    role_input: str


class RoleChangeArgs(_Args):
    '''role.add, role.remove'''
    inputs: str


COMMAND_ARGS: dict[str, type[_Args]] = {
    'hello': NoArgs,
    'roll': RollArgs,
    'meta': NoArgs,
    'urban': UrbanArgs,
    'markov.on': MarkovOnArgs,
    'markov.off': NoArgs,
    'markov.list-channels': NoArgs,
    'markov.speak': MarkovSpeakArgs,
    'join': NoArgs,
    'play': PlayArgs,
    'skip': NoArgs,
    'clear': NoArgs,
    'history': NoArgs,
    'shuffle': NoArgs,
    'remove': QueueIndexArgs,
    'bump': QueueIndexArgs,
    'stop': NoArgs,
    'move-messages': NoArgs,
    'random-play': NoArgs,
    'music-stats': NoArgs,
    'playlist.create': PlaylistCreateArgs,
    'playlist.list': NoArgs,
    'playlist.item-add': PlaylistItemAddArgs,
    'playlist.item-remove': PlaylistItemRemoveArgs,
    'playlist.show': PlaylistIndexArgs,
    'playlist.delete': PlaylistIndexArgs,
    'playlist.rename': PlaylistRenameArgs,
    'playlist.save-queue': PlaylistSaveArgs,
    'playlist.save-history': PlaylistSaveArgs,
    'playlist.queue': PlaylistIndexArgs,
    'playlist.merge': PlaylistMergeArgs,
    'role.list': NoArgs,
    'role.users': RoleUsersArgs,
    'role.available': NoArgs,
    'role.add': RoleChangeArgs,
    'role.remove': RoleChangeArgs,
}
