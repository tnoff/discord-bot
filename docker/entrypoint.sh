#!/bin/bash
set -e

if [ -n "${EXTRA_PIP_PACKAGES}" ]; then
    pip install --user ${EXTRA_PIP_PACKAGES}
fi

cmd=("${DISCORD_BOT_CMD:-discord-gateway}" "$@")

exec "${cmd[@]}"
