FROM ubuntu:24.04

# Base packages
RUN apt-get update -y && apt-get -y upgrade
RUN apt-get install -y python3-dev python3-virtualenv ffmpeg wget git libpq-dev

# Setup venv
ENV TMPDIR="/tmp/bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"

# Setup installs
RUN mkdir -p "${TMPDIR}" "${WORKDIR}" "${LOGFILE}"
COPY discord_bot/ "${TMPDIR}/discord_bot/"
COPY requirements.txt "${TMPDIR}/"
COPY setup.py "${TMPDIR}/"
RUN virtualenv "${VENVDIR}"
RUN ${VENVDIR}/bin/pip install psycopg2 "${TMPDIR}"
RUN rm -rf "${TMPDIR}"

CMD ["/opt/discord-venv/bin/discord-bot", "/opt/discord/cnf/discord.cnf", "run"]