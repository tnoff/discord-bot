FROM ubuntu:24.04

ENV DISCORD_VERSION="v2.0.1"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"

# Base packages
RUN apt-get update -y && apt-get -y upgrade
# Necessary for startup
RUN apt-get install -y python3-dev python3-virtualenv ffmpeg wget git libpq-dev
# Nice to have
RUN apt-get install -y vim iputils-ping

# Setup venv
RUN wget https://github.com/tnoff/discord-bot/archive/refs/tags/${DISCORD_VERSION}.tar.gz -O /tmp/discord.tar.gz
RUN mkdir -p "${WORKDIR}"
RUN virtualenv "${VENVDIR}"
RUN ${VENVDIR}/bin/pip install /tmp/discord.tar.gz
# TODO remove ansyc timeout once package version bumped
RUN ${VENVDIR}/bin/pip install async-timeout psycopg2

# Setup files
RUN mkdir -p "${WORKDIR}/downloads/" "${WORKDIR}/backup"
RUN mkdir -p /var/log/discord/


# Last bits, print out release info
RUN ${VENVDIR}/bin/pip freeze && dpkg -l && cat /etc/*lease*

CMD ["/opt/discord-venv/bin/discord-bot", "/opt/discord/discord.cnf", "run"]