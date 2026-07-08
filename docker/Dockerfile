# ---------- builder: compiles deps + installs Deno; none of this ships ----------
FROM python:3.14-slim-bookworm AS builder

# Build-time only:
#   git             -> the `dappertable @ git+https://...` VCS dependency
#   curl, unzip     -> the Deno installer
#   build-essential -> fallback for any dep without an aarch64 wheel (none today)
# None of these reach the final image (that was the point of the old
# `apt-get remove` line, which didn't actually shrink anything — the tools
# stayed baked into the earlier layer).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential git curl unzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ENV APPDIR="/opt/packages/discord-bot"

COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY alembic/ "${APPDIR}/alembic/"
COPY alembic.ini pyproject.toml VERSION "${APPDIR}/"

# Install into a relocatable prefix the final stage copies into /usr/local.
RUN pip install --no-cache-dir --prefix=/install "${APPDIR}[bot]"

# Deno (yt-dlp runtime dependency). The installer ignores env overrides piped
# into `sh` and always lands in $HOME/.deno (here /root/.deno), so relocate it
# explicitly to a fixed path the final stage copies out.
# https://github.com/yt-dlp/yt-dlp/issues/14404
RUN curl -fsSL https://deno.land/install.sh | sh && mv /root/.deno /opt/deno


# ---------- final: runtime only ----------
FROM python:3.14-slim-bookworm

# Runtime packages only.
# ffmpeg            -> audio processing
# postgresql-client -> db operations
ARG INSTALL_HEAPTRACK=false
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg postgresql-client && \
    if [ "$INSTALL_HEAPTRACK" = "true" ]; then \
        apt-get install -y --no-install-recommends heaptrack; \
    fi && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

ENV APPDIR="/opt/packages/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"
ENV DENO_INSTALL_ROOT="/opt/discord/.deno"
# Make sure deno is on PATH
ENV PATH="/opt/discord/.deno/bin:$PATH"

# Directory structure + non-root user (UID/GID 1000).
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}" \
        "${WORKDIR}/cnf" "${WORKDIR}/downloads" "${WORKDIR}/db" && \
    groupadd -g 1000 discord && \
    useradd -r -u 1000 -g discord -s /bin/bash -d /opt/discord discord

# Python site-packages + console scripts from the builder stage.
COPY --from=builder /install /usr/local
# Deno static binary (installed to /opt/deno in the builder -> runtime home).
COPY --from=builder /opt/deno /opt/discord/.deno

COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && \
    chown -R discord:discord "${WORKDIR}" "${LOGFILE}"

EXPOSE 8080

WORKDIR "/opt/discord"

# Switch to non-root user for runtime
USER discord

# Make sure cache dir exists
RUN mkdir -p /opt/discord/.cache

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=5)"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/opt/discord/cnf/discord.cnf"]
