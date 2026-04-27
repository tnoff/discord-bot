FROM python:3.14-slim-bookworm


# Base packages
# Update to latest for security fixes
ARG INSTALL_HEAPTRACK=false
ARG DB_TYPE=sqlite
RUN apt-get update && \
    apt-get install -y gcc cmake git ffmpeg curl unzip \
      $([ "$DB_TYPE" = "postgres" ] && echo "postgresql-client" || echo "sqlite3") && \
    if [ "$INSTALL_HEAPTRACK" = "true" ]; then apt-get install -y heaptrack; fi && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Setup venv
ENV APPDIR="/opt/packages/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"
ENV DENO_INSTALL_ROOT="/opt/discord/.deno"

# Setup installs and create directory structure
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}" \
    "${WORKDIR}/cnf" "${WORKDIR}/downloads" "${WORKDIR}/db"

# Create non-root user and group
# Using UID/GID 1000 which is common for first user on many systems
# Home directory is /opt/discord (the working directory)
RUN groupadd -g 1000 discord && \
    useradd -r -u 1000 -g discord -s /bin/bash -d /opt/discord discord

COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY alembic/ "${APPDIR}/alembic/"
COPY alembic.ini "${APPDIR}/"
COPY pyproject.toml VERSION "${APPDIR}/"
COPY scripts/ "${WORKDIR}/scripts/"
COPY docker/entrypoint.sh /entrypoint.sh

RUN pip install "${APPDIR}[all,${DB_TYPE}]"

RUN chmod +x /entrypoint.sh

# Set ownership of directories that need to be writable by the discord user
# Must be done BEFORE switching to discord user
RUN chown -R discord:discord "${WORKDIR}" "${LOGFILE}"

# Install Deno as non-root user
# https://github.com/yt-dlp/yt-dlp/issues/14404
USER discord
RUN curl -fsSL https://deno.land/install.sh | sh

# Switch back to root to finish setup
USER root

# Uninstall build dependencies
RUN apt-get remove -y git unzip gcc cmake curl && apt-get autoremove -y

# Make sure deno in path
ENV PATH="$PATH:/opt/discord/.deno/bin"

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
