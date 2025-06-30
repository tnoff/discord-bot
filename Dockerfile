FROM python:3.13-slim-bookworm


# Base packages
RUN apt-get update && apt-get install -y gcc libpq-dev git ffmpeg

# Setup venv
ENV APPDIR="/opt/packages/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"

# Setup installs
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}"
COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY alembic/ "${APPDIR}/albemic/"
COPY requirements.txt "${APPDIR}/"
COPY alembic.ini "${APPDIR}/"
COPY setup.py "${APPDIR}/"
RUN pip install psycopg2

RUN pip install psycopg2 "${APPDIR}"
# Temp fix for https://github.com/Rapptz/discord.py/issues/10207
# Applies https://github.com/Rapptz/discord.py/pull/10210/files

# Clone discord.py and patch it
WORKDIR /tmp
RUN git clone https://github.com/Rapptz/discord.py.git
WORKDIR /tmp/discord.py
# checkout the PR commit directly
RUN git fetch origin pull/10210/head:fix_voice_reconnect && git checkout fix_voice_reconnect
# or cherry-pick the commit hash if you prefer

# Install discord.py from source
RUN pip uninstall discord.py -y && pip install .
RUN rm -rf /tmp/discord.py

WORKDIR "/opt/discord"

RUN apt-get remove -y gcc git && apt-get autoremove -y

CMD ["discord-bot", "/opt/discord/cnf/discord.cnf"]
