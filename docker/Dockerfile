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

# Deno (yt-dlp runtime dependency). The installer ignores env overrides piped
# into `sh` and always lands in $HOME/.deno (here /root/.deno), so relocate it
# explicitly to a fixed path the final stage copies out.
# https://github.com/yt-dlp/yt-dlp/issues/14404
# Kept ABOVE every source COPY: it depends on nothing in this repo, so anything
# below a COPY would re-fetch it on each code change.
RUN curl -fsSL https://deno.land/install.sh | sh && mv /root/.deno /opt/deno

# ---- dependency layer: keyed on pyproject.toml alone ----
# Installs the third-party dependency set (the expensive, network-bound step)
# against a STUB package, so this layer is invalidated by a dependency change
# and nothing else.
#
# The stub is what keeps VERSION out of this layer.  setuptools resolves the
# version via `[tool.setuptools.dynamic] version = {file = "VERSION"}` and will
# not build without it, but VERSION is bumped by *every* MR (the changelog.d
# workflow), so copying the real one here would bust the dep install on every
# change and defeat the split entirely.  A placeholder satisfies the build; the
# real version is installed by the source layer below.
COPY pyproject.toml "${APPDIR}/"
RUN mkdir -p "${APPDIR}/discord_bot" && \
    : > "${APPDIR}/discord_bot/__init__.py" && \
    echo "0.0.0" > "${APPDIR}/VERSION" && \
    pip install --no-cache-dir --prefix=/install "${APPDIR}[bot]" && \
    rm -rf /install/lib/python*/site-packages/discord_bot \
           /install/lib/python*/site-packages/discord_bot-0.0.0.dist-info

# ---- source layer: the actual package ----
# --no-deps because the layer above already resolved and installed every
# third-party requirement; this installs our package only.
#
# The stub install is torn down in the layer above rather than overwritten
# here: pip cannot reliably uninstall from a `--prefix` tree, so leaving it in
# place produced BOTH a discord_bot-0.0.0.dist-info and the real
# discord_bot-<VERSION>.dist-info, and `importlib.metadata.version()` resolved
# to the 0.0.0 one. Nothing reads the version at runtime today, but the image
# would have reported the wrong one.
#
# It installs to a SEPARATE prefix (/install-app, not /install). That split is
# what keeps the export phase cheap: the final stage copies the two prefixes as
# two layers, so a source change only ever produces a small new layer. Merging
# them here would put ~474 MB of dependencies into a layer that our own package
# rewrites on every commit, and buildkit would recompress and re-upload all of
# it per build (measured: `exporting layers` 270.9s of a 283.3s export step).
COPY discord_bot/ "${APPDIR}/discord_bot/"
# No alembic/ or alembic.ini. Both were COPYd here until the MR 4 cutover
# (#928) took [database] -- and with it the alembic CLI -- out of [bot]. They
# only ever landed in this builder stage, since the final stage copies the two
# install prefixes and not ${APPDIR}, so this is not a size change: it removes
# a build step that implied the bot could run `alembic upgrade head`, which it
# now cannot. Same correction #917 made on the dispatcher.
COPY VERSION "${APPDIR}/"
RUN pip install --no-cache-dir --no-deps --prefix=/install-app "${APPDIR}[bot]"


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
# The chown happens HERE, while these directories are still empty. Doing it
# after the COPYs below (as a `chown -R` over ${WORKDIR}) rewrote every file
# the Deno copy had just placed, producing a 95.6 MB layer that was a pure
# duplicate of the 95.6 MB directly above it.
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}" \
        "${WORKDIR}/cnf" "${WORKDIR}/downloads" "${WORKDIR}/db" && \
    groupadd -g 1000 discord && \
    useradd -r -u 1000 -g discord -s /bin/bash -d /opt/discord discord && \
    chown -R discord:discord "${WORKDIR}" "${LOGFILE}"

# Third-party dependencies (~474 MB). Keyed on pyproject.toml only, so this
# layer is byte-identical across source changes and stays CACHED — which is the
# entire point of installing it to its own prefix in the builder.
COPY --from=builder /install /usr/local
# Deno static binary (installed to /opt/deno in the builder -> runtime home).
# --chown avoids a later `chown -R` that would duplicate the whole tree.
COPY --from=builder --chown=discord:discord /opt/deno /opt/discord/.deno

# Our own package. Small, and the only one of these that changes per commit —
# so it goes LAST, below everything stable.
COPY --from=builder /install-app /usr/local

COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 8080

# Parity with the other four images: name the console script rather than leaning
# on the entrypoint's default. `discord-bot` is the HA bot process (cli.bot) —
# the single-process entrypoint it used to name was retired.
ENV DISCORD_BOT_CMD=discord-bot

WORKDIR "/opt/discord"

# Switch to non-root user for runtime
USER discord

# Make sure cache dir exists
RUN mkdir -p /opt/discord/.cache

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=5)"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/opt/discord/cnf/discord.cnf"]
