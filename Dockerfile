FROM python:3.13-slim-bookworm


# Base packages
RUN apt-get update && apt-get install -y gcc libpq-dev git ffmpeg

# Setup venv
ENV APPDIR="/opt/discord-bot"
ENV WORKDIR="/opt/discord"
ENV VENVDIR="/opt/discord-venv"
ENV LOGFILE="/var/log/discord"

# Setup installs
RUN mkdir -p "${APPDIR}" "${WORKDIR}" "${LOGFILE}"
COPY discord_bot/ "${APPDIR}/discord_bot/"
COPY albemic/ "${APPDIR}}/albemic/"
COPY requirements.txt "${APPDIR}/"
COPY albemic.ini "${APPDIR}/"
COPY setup.py "${APPDIR}/"
RUN pip install psycopg2 "${APPDIR}"

RUN apt-get remove -y gcc git && apt-get autoremove -y

CMD ["discord-bot", "/opt/discord/cnf/discord.cnf"]
