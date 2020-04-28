FROM ubuntu:18.04

# Setup volumes
VOLUME ["/secret", "/logs"]

# Insall packages
RUN apt-get update
RUN apt-get install -y \
   cron \
   ffmpeg \
   libmysqlclient-dev \
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
COPY files/etc/cron.d/check-twitter /etc/cron.d/check-twitter
COPY files/etc/cron.d/cleanup-youtube /etc/cron.d/cleanup-youtube
COPY files/usr/local/bin/cleanup-youtube.sh /usr/local/bin/cleanup-youtube.sh

# Chmod files
RUN chmod +x /usr/local/bin/cleanup-youtube.sh

# Setup cron
RUN touch /logs/cron.log
RUN /usr/bin/crontab /etc/cron.d/check-twitter
RUN /usr/bin/crontab /etc/cron.d/cleanup-youtube

# Start supervisord
CMD /usr/bin/supervisord -n
