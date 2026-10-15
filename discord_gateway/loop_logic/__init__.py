'''
The per-iteration work of the gateway's background loops, free of discord.py (#1030).

The cog keeps the wiring (the asyncio tasks, ``LOOP_HEALTH``, the heartbeat gauges and the
dispatcher result queue); what happens on one iteration lives here and reaches Discord only
through callables the cog passes in. The import-boundary test enforces it, as it does for
``command_logic``.
'''
