'''
The config the pod's markov and delete-messages loops read (docs/markov.md, docs/delete_messages.md).
'''
from pydantic import BaseModel, Field

from discord_commands.loop_logic.delete_messages import DELETE_AFTER_DEFAULT


class MarkovConfig(BaseModel):
    '''Markov chain configuration'''
    loop_sleep_interval: float = 300.0
    message_check_limit: int = 16
    history_retention_days: int = 365
    # Read by the markov commands as well (markov.server_reject_list).
    server_reject_list: list[int] = Field(default_factory=list)


class DiscordChannelConfig(BaseModel):
    '''Discord channel configuration for message deletion'''
    server_id: int
    channel_id: int
    delete_after: int = DELETE_AFTER_DEFAULT


class DeleteMessagesConfig(BaseModel):
    '''Delete messages configuration'''
    loop_sleep_interval: float = 300.0
    discord_channels: list[DiscordChannelConfig]
