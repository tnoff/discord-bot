FROM python:3.14-slim-bookworm


# Base packages
RUN apt-get update && apt-get install -y git ffmpeg curl unzip heaptrack sqlite3

# Install Deno
# https://github.com/yt-dlp/yt-dlp/issues/14404
RUN curl -fsSL https://deno.land/install.sh | sh

# Setup venv
ENV APPDIR="/opt/packages/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"

# Setup installs
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}" "${WORKDIR}/scripts"
COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY alembic/ "${APPDIR}/alembic/"
COPY requirements.txt "${APPDIR}/"
COPY alembic.ini "${APPDIR}/"
COPY setup.py "${APPDIR}/"
COPY scripts/ "${WORKDIR}/scripts/"

RUN pip install "${APPDIR}"

WORKDIR "/opt/discord"

RUN apt-get remove -y git unzip && apt-get autoremove -y

CMD ["discord-bot", "/opt/discord/cnf/discord.cnf"]
