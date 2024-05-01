# Markov Cog

Uses [Markov Chain](https://en.wikipedia.org/wiki/Markov_chain) to mimic chat in discord channels.

Use the command `on` to have the bot read message history for a given channel. Once turned on in a discord channel, reads channel history to get its data set. There is a loop which will automatically gather new text data, and there is a setting for how far back in time we will keep chat history. For text messages in that channel, it takes text and breaks down each message into "leader" and "follower" pairs. For example, given the text:

```
Hey you guys should check out this cool song
```

It will generate the following pairs:
- "hey" (leader) and "you" (follower)
- "you" (leader) and "guys" (follower)
- "guys" (leader) and "should" (follower)
- etc ...

Then can use the `speak` command to generate a random sentence from channel history.

To mimic user messages, a word can be chosen at random or entered into the command. Given a that word, it finds which pairs have that word as a leader, and which words follow that word. It then calculates the chances a follower comes after a leader word.
For example, given the leader word "hey", there might be a 10% chance the next word is "there", a 25% chance the next word is "everybody", and so on.
In then uses weighted random chance to pick the next word, then uses this word as the leader, and repeats the process for either 32 words by default, or a larger amount of words if specified in the command.

## Intents

Make sure you pass the `message_content` intent into the config

```
  intents:
    - message_content
```

## Turn markov on

Turn markov on in the channel and track channel history.

```
!markov on
```

## Turn markov off

Turn off markov in the channel and delete all channel history from db.

```
!markov off
```

## Markov Speak

Have markov generate a random message from history

```
!markov speak
```

## Markov Speak with prompt

Have markov speak but start with a random word or phrase. If multiple words given, will start at last word in string.

```
!markov speak "foo bar fooey"
```