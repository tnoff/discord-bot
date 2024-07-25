FROM ubuntu:24.04

ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"

# Base packages
RUN apt-get update -y && apt-get -y upgrade
# Necessary for startup
RUN apt-get install -y python3-dev python3-virtualenv ffmpeg wget git libpq-dev
# Nice to have
RUN apt-get install -y vim iputils-ping

# Setup venv
# Assume we should just get latest code
#RUN git clone https://github.com/tnoff/discord-bot.git /tmp/discord_bot/

ENV TMPDIR="/tmp/bot"
RUN mkdir -p "${TMPDIR}"
COPY discord_bot/ "${TMPDIR}/discord_bot/"
COPY requirements.txt "${TMPDIR}/"
COPY setup.py "${TMPDIR}/"
RUN mkdir -p "${WORKDIR}"
RUN virtualenv "${VENVDIR}"
RUN ${VENVDIR}/bin/pip install "${TMPDIR}"
RUN ${VENVDIR}/bin/pip install psycopg2

CMD ["/opt/discord-venv/bin/discord-bot", "/opt/discord/cnf/discord.cnf", "run"]