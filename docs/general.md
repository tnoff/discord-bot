# General Cog

Basic utility commands. Always loaded — there's no `include` key to turn it
off.

## Hello

Say hello to the bot and it will say hello back. Useful for checking the
bot is running and responsive.

```
!hello
> Waddup tnoff
```

## Roll

Roll dice and report the total.

```
!roll 6    # one roll, 1-6
!roll d6   # same as above — the "d" prefix is optional
!roll 2d6  # two rolls, 1-6 each, summed
```

A single roll reports just the total:

```
!roll 6
> tnoff rolled a 4
```

More than one roll reports each value plus the sum:

```
!roll 2d6
> tnoff rolled: 6 + 4 = 10
```

Limits: at most 20 rolls and at most 100 sides per roll. Anything beyond
that, or input that doesn't match the `[rolls]d<sides>` shape, is rejected
with an error message rather than run.

## Meta

Show the current server, channel, and user IDs — handy for grabbing the IDs
this bot's other config (roles, channels, playlists) asks for.

```
!meta
> Server id: <redacted>
> Channel id: <redacted>
> User id: <redacted>
```
