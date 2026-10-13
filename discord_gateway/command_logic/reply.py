'''
How command logic talks back to the channel.
'''
from typing import Awaitable, Callable

# (guild_id, channel_id, content) -> content. The cog passes ``dispatch_message``,
# which sends through the dispatcher and returns the text it sent.
Reply = Callable[[int, int, str], Awaitable[str]]
