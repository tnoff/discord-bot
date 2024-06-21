FROM ubuntu:24.04

ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"

# Base packages
RUN apt-get update -y && apt-get -y upgrade
# Necessary for startup
RUN apt-get install -y python3-dev python3-virtualenv ffmpeg wget git libpq-dev
# Nice to have
RUN apt-get install -y vim iputils-ping

# Setup venv
# Assume we should just get latest code
RUN git clone https://github.com/tnoff/discord-bot.git /tmp/discord_bot/
RUN mkdir -p "${WORKDIR}"
RUN virtualenv "${VENVDIR}"
RUN ${VENVDIR}/bin/pip install /tmp/discord_bot/
# TODO remove ansyc timeout once package version bumped
RUN ${VENVDIR}/bin/pip install psycopg2

# Setup files
RUN mkdir -p "${WORKDIR}/downloads/" "${WORKDIR}/backup"
RUN mkdir -p /var/log/discord/

CMD ["/opt/discord-venv/bin/discord-bot", "/opt/discord/discord.cnf", "run"]