FROM ubuntu:20.04

# Setup volumes
VOLUME ["/secret", "/logs"]

ENV DEBIAN_FRONTEND=noninteractive

# Insall packages
RUN apt-get update
RUN apt-get install -y \
   cron \
   ffmpeg \
   git \
   libmysqlclient-dev \
   logrotate \
   python3-dev \
   python3-pip \
   supervisor \
   # Nice to have
   iputils-ping \
   less \
   vim \
   # Cleanup
   && apt-get clean && rm -rf /var/lib/apt/lists/*

# Setup dirs
RUN mkdir -p /opt/discord_bot /usr/local/bin /logs/supervisor/

# Install discord bot
COPY discord_bot /opt/discord_bot
RUN /usr/bin/pip3 install /opt/discord_bot

# Copy files
COPY files/etc/supervisor/conf.d/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY files/etc/cron.hourly/check-twitter /etc/cron.hourly/check-twitter
COPY files/etc/cron.hourly/cleanup-youtube /etc/cron.hourly/cleanup-youtube
COPY files/etc/cron.hourly/logrotate /etc/cron.hourly/logrotate
COPY files/etc/logrotate.d/discord /etc/logrotate.d/discord


# Fix logrotate file
# Chmod logrotate conf
RUN sed -i 's/su root syslog/su root adm/' /etc/logrotate.conf
RUN chmod 0644 /etc/logrotate.d/discord
# Chmod cron files
RUN chmod +x /etc/cron.hourly/check-twitter \
    /etc/cron.hourly/cleanup-youtube \
    /etc/cron.hourly/logrotate

# Start supervisord
CMD /usr/bin/supervisord -n
