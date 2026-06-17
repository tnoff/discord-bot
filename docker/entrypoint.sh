#!/bin/bash
set -e

if [ -n "${EXTRA_PIP_PACKAGES}" ]; then
    pip install --user ${EXTRA_PIP_PACKAGES}
fi

exec "${DISCORD_BOT_CMD:-discord-bot}" "$@"
