#!/bin/bash
set -e

if [ -n "${EXTRA_PIP_PACKAGES}" ]; then
    pip install --user ${EXTRA_PIP_PACKAGES}
fi

exec discord-bot "$@"
