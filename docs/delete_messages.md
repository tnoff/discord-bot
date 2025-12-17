# Delete Messages Cog

Have bot auto delete messages in a channel after a given amount of days.

There are no corresponding commands used, this will run in the background.


Make sure to include the bot in the config, and have set the messages intent:
```
---
include:
  delete_messages: true
intents:
  - messages
```

Then configure the channels

```yaml
---
delete_messages:
  loop_sleep_interval: 30 # Optional, seconds to sleep between each check
  discord_channels:
    - server_id: 123456789  # Discord Server ID (integer, unquoted)
      channel_id: 987654321  # Discord Channel ID (integer, unquoted)
      delete_after: 7  # Delete after X days
```
