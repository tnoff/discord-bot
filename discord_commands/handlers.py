'''
The commands this pod serves: command name (a key of discord_core's COMMAND_ARGS) to its handler.

A command group's handlers are added here in the change that moves its logic into this package,
in the same change that makes the gateway forward it, so a command is never served by neither
pod. A known command with no handler here answers 501 and the gateway, which forwards only the
groups that have moved, never asks for it.
'''
from logging import Logger

from discord_core.clients.http_markov_store import HttpMarkovStore
from discord_core.types.command_args import (
    MarkovOnArgs, MarkovSpeakArgs, PlayArgs, PlaylistCreateArgs, PlaylistIndexArgs,
    PlaylistItemAddArgs, PlaylistItemRemoveArgs, PlaylistMergeArgs, PlaylistQueueArgs,
    PlaylistRenameArgs, PlaylistSaveArgs, QueueIndexArgs, RoleChangeArgs, RoleUsersArgs, RollArgs, UrbanArgs,
)
from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.command_logic.general import GeneralCommands
from discord_commands.command_logic.markov import MarkovCommands
from discord_commands.command_logic.music import MusicCommands
from discord_commands.command_logic.playlist import PlaylistCommands
from discord_commands.command_logic.reply import Reply
from discord_commands.command_logic.role import RoleCommands
from discord_commands.command_logic.urban import UrbanCommands
from discord_commands.workers.command_executor import Handler


def build_handlers(reply: Reply, logger: Logger, markov_store: HttpMarkovStore, markov_reject_list: list[int],
                   playlist: PlaylistCommands, music: MusicCommands, role: RoleCommands) -> dict[str, Handler]:
    '''The handler registry the executor is built with.'''
    general = GeneralCommands(reply)
    urban = UrbanCommands(reply, logger)
    markov = MarkovCommands(reply, logger, markov_store, markov_reject_list)

    async def hello(env: CommandEnvelope, _args) -> None:
        await general.hello(env)

    async def roll(env: CommandEnvelope, args: RollArgs) -> None:
        await general.roll(env, args.input_value)

    async def meta(env: CommandEnvelope, _args) -> None:
        await general.meta(env)

    async def urban_lookup(env: CommandEnvelope, args: UrbanArgs) -> None:
        await urban.word_lookup(env, args.word)

    async def markov_on(env: CommandEnvelope, args: MarkovOnArgs) -> None:
        await markov.on(env, args.channel_is_text_or_voice)

    async def markov_off(env: CommandEnvelope, _args) -> None:
        await markov.off(env)

    async def markov_list_channels(env: CommandEnvelope, _args) -> None:
        await markov.list_channels(env)

    async def markov_speak(env: CommandEnvelope, args: MarkovSpeakArgs) -> None:
        await markov.speak(env, args.first_word, args.sentence_length)

    async def playlist_create(env: CommandEnvelope, args: PlaylistCreateArgs) -> None:
        await playlist.create(env, args.name)

    async def playlist_list(env: CommandEnvelope, _args) -> None:
        await playlist.list_playlists(env)

    async def playlist_item_remove(env: CommandEnvelope, args: PlaylistItemRemoveArgs) -> None:
        await playlist.item_remove(env, args.playlist_index, args.video_index)

    async def playlist_show(env: CommandEnvelope, args: PlaylistIndexArgs) -> None:
        await playlist.show(env, args.playlist_index)

    async def playlist_delete(env: CommandEnvelope, args: PlaylistIndexArgs) -> None:
        await playlist.delete(env, args.playlist_index)

    async def playlist_rename(env: CommandEnvelope, args: PlaylistRenameArgs) -> None:
        await playlist.rename(env, args.playlist_index, args.playlist_name)

    async def playlist_merge(env: CommandEnvelope, args: PlaylistMergeArgs) -> None:
        await playlist.merge(env, args.playlist_index_one, args.playlist_index_two)

    async def join(env: CommandEnvelope, _args) -> None:
        await music.join(env)

    async def play(env: CommandEnvelope, args: PlayArgs) -> None:
        await music.play(env, args.search)

    async def skip(env: CommandEnvelope, _args) -> None:
        await music.skip(env)

    async def clear(env: CommandEnvelope, _args) -> None:
        await music.clear(env)

    async def history(env: CommandEnvelope, _args) -> None:
        await music.history(env)

    async def shuffle(env: CommandEnvelope, _args) -> None:
        await music.shuffle(env)

    async def remove(env: CommandEnvelope, args: QueueIndexArgs) -> None:
        await music.remove(env, args.queue_index)

    async def bump(env: CommandEnvelope, args: QueueIndexArgs) -> None:
        await music.bump(env, args.queue_index)

    async def stop(env: CommandEnvelope, _args) -> None:
        await music.stop(env)

    async def move_messages(env: CommandEnvelope, _args) -> None:
        await music.move_messages(env)

    async def random_play(env: CommandEnvelope, _args) -> None:
        await music.random_play(env)

    async def music_stats(env: CommandEnvelope, _args) -> None:
        await music.music_stats(env)

    async def playlist_item_add(env: CommandEnvelope, args: PlaylistItemAddArgs) -> None:
        await playlist.item_add(env, args.playlist_index, args.search)

    async def playlist_save_queue(env: CommandEnvelope, args: PlaylistSaveArgs) -> None:
        await playlist.save(env, args.name)

    async def playlist_save_history(env: CommandEnvelope, args: PlaylistSaveArgs) -> None:
        await playlist.save(env, args.name, is_history=True)

    async def playlist_queue(env: CommandEnvelope, args: PlaylistQueueArgs) -> None:
        await music.playlist_queue(env, args.playlist_index, args.args)

    async def role_list(env: CommandEnvelope, _args) -> None:
        await role.role_list(env)

    async def role_users(env: CommandEnvelope, args: RoleUsersArgs) -> None:
        await role.role_list_users(env, args.role_input)

    async def role_available(env: CommandEnvelope, _args) -> None:
        await role.role_managed(env)

    async def role_add(env: CommandEnvelope, args: RoleChangeArgs) -> None:
        await role.role_add(env, args.inputs)

    async def role_remove(env: CommandEnvelope, args: RoleChangeArgs) -> None:
        await role.role_remove(env, args.inputs)

    return {
        'hello': hello,
        'roll': roll,
        'meta': meta,
        'urban': urban_lookup,
        'markov.on': markov_on,
        'markov.off': markov_off,
        'markov.list-channels': markov_list_channels,
        'markov.speak': markov_speak,
        'playlist.create': playlist_create,
        'playlist.list': playlist_list,
        'playlist.item-remove': playlist_item_remove,
        'playlist.show': playlist_show,
        'playlist.delete': playlist_delete,
        'playlist.rename': playlist_rename,
        'playlist.merge': playlist_merge,
        'join': join,
        'play': play,
        'skip': skip,
        'clear': clear,
        'history': history,
        'shuffle': shuffle,
        'remove': remove,
        'bump': bump,
        'stop': stop,
        'move-messages': move_messages,
        'random-play': random_play,
        'music-stats': music_stats,
        'playlist.item-add': playlist_item_add,
        'playlist.save-queue': playlist_save_queue,
        'playlist.save-history': playlist_save_history,
        'playlist.queue': playlist_queue,
        'role.list': role_list,
        'role.users': role_users,
        'role.available': role_available,
        'role.add': role_add,
        'role.remove': role_remove,
    }
