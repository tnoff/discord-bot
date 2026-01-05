FROM python:3.14-slim-bookworm


# Base packages
RUN apt-get update && apt-get install -y gcc cmake git ffmpeg curl unzip heaptrack sqlite3

# Setup venv
ENV APPDIR="/opt/packages/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"
ENV DENO_INSTALL_ROOT="/opt/discord/.deno"

# Setup installs and create directory structure
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}" "${WORKDIR}/scripts" \
    "${WORKDIR}/cnf" "${WORKDIR}/downloads"

# Create non-root user and group
# Using UID/GID 1000 which is common for first user on many systems
# Home directory is /opt/discord (the working directory)
RUN groupadd -g 1000 discord && \
    useradd -r -u 1000 -g discord -s /bin/bash -d /opt/discord discord

COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY alembic/ "${APPDIR}/alembic/"
COPY requirements.txt "${APPDIR}/"
COPY alembic.ini "${APPDIR}/"
COPY setup.py "${APPDIR}/"
COPY scripts/ "${WORKDIR}/scripts/"
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN pip install "${APPDIR}"
# Install nightly build of yt-dlp
RUN python -m pip install -U --pre "yt-dlp[default]"

# Set ownership of directories that need to be writable by the discord user
# Must be done BEFORE switching to discord user
RUN chown -R discord:discord "${WORKDIR}" "${LOGFILE}" && \
    chmod +x /usr/local/bin/docker-entrypoint.sh

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

WORKDIR "/opt/discord"

# Switch to non-root user for runtime
USER discord

# Make sure cache dir exists
RUN mkdir -p /opt/discord/.cache

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["discord-bot", "/opt/discord/cnf/discord.cnf"]
