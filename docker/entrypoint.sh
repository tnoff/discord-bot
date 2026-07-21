#!/bin/bash
set -e

if [ -n "${EXTRA_PIP_PACKAGES}" ]; then
    pip install --user ${EXTRA_PIP_PACKAGES}
fi

cmd=("${DISCORD_BOT_CMD:-discord-bot}" "$@")

# Opt-in native heap profiling. tracemalloc only sees Python allocations; when
# RSS climbs while the Python heap stays flat the leak is native/C-extension,
# and heaptrack traces malloc/free at the allocator level to name the site.
# Requires an image built with --build-arg INSTALL_HEAPTRACK=true. Diagnostic
# only — heaptrack has real overhead and writes a growing data file, so leave
# it off in normal prod. Output lands at ${HEAPTRACK_OUTPUT}.<pid>.zst; copy it
# out and analyse with heaptrack_print / the GUI.
if [ "${HEAPTRACK_ENABLE:-false}" = "true" ]; then
    if ! command -v heaptrack >/dev/null 2>&1; then
        echo "entrypoint: HEAPTRACK_ENABLE=true but heaptrack is not installed" \
             "(rebuild with --build-arg INSTALL_HEAPTRACK=true)" >&2
        exit 1
    fi
    output="${HEAPTRACK_OUTPUT:-/opt/discord/heaptrack/discord}"
    mkdir -p "$(dirname "${output}")"
    echo "entrypoint: launching under heaptrack -> ${output}.<pid>.zst" >&2
    exec heaptrack -o "${output}" "${cmd[@]}"
fi

exec "${cmd[@]}"
