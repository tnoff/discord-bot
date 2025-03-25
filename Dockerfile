FROM python:3.13-slim-bookworm


# Base packages
RUN apt-get update && apt-get install -y gcc libpq-dev git

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
RUN pip install psycopg2 "${TMPDIR}"
RUN rm -rf "${TMPDIR}"

RUN apt-get remove -y gcc git && apt-get autoremove -y

CMD ["/opt/discord-venv/bin/discord-bot", "/opt/discord/cnf/discord.cnf"]
