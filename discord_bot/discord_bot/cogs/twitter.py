import asyncio
from copy import deepcopy

from discord.ext import commands
from twitter.error import TwitterError

from discord_bot.cogs.common import CogHelper
from discord_bot.database import TwitterSubscription

class Twitter(CogHelper):
    '''
    Subscribe to twitter accounts and post messages in channel
    '''
    def __init__(self, bot, db_session, logger, twitter_api):
        super().__init__(bot, db_session, logger)
        self.twitter_api = twitter_api
        self.bot.loop.create_task(self.wait_loop())


    async def _check_subscription(self, subscription):
        self.logger.debug(f'Checking users twitter feed for '
                          f'new posts: {subscription.twitter_user_id}')
        last_post = None
        exit_loop = False
        has_new_first_post = False
        old_last_post = deepcopy(subscription.last_post)
        while not exit_loop:
            try:
                timeline = self.twitter_api.GetUserTimeline(user_id=subscription.twitter_user_id,
                                                            since_id=last_post, include_rts=False,
                                                            exclude_replies=True)
            except TwitterError as error:
                self.logger.exception(f'Exception getting user: {error}')
                return

            for post in timeline:
                if post.id != old_last_post:
                    channel = self.bot.get_channel(int(subscription.channel_id))
                    message = f'https://twitter.com/{post.user.screen_name}/status/{post.id}'
                    self.logger.info(f'Posting twitter message "{message}" to channel {channel.id}')
                    await channel.send(message)
                    if not has_new_first_post:
                        subscription.last_post = post.id
                        self.db_session.commit()
                        has_new_first_post = True
                else:
                    exit_loop = True
                    break
            try:
                last_post = timeline[-1].id
            except IndexError:
                self.logger.error(f'Timeline empty for user {subscription.twitter_user_id}')
                exit_loop = True

    async def wait_loop(self):
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            self.logger.debug("Checking twitter feeds")
            subscriptions = self.db_session.query(TwitterSubscription).all()
            for subscription in subscriptions:
                await self._check_subscription(subscription)
            await asyncio.sleep(300)

    @commands.group(name='twitter', invoke_without_command=False)
    async def twitter(self, ctx):
        '''
        Planner functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')

    @twitter.command(name='subscribe')
    async def subscribe(self, ctx, twitter_account):
        '''
        Subscribe to twitter account, and post updates in channel
        '''
        self.logger.debug(f'Attempting to subscribe to username: {twitter_account}')
        try:
            user = self.twitter_api.GetUser(screen_name=twitter_account)
        except TwitterError as error:
            self.logger.exception(f'Exception getting user: {error}')
            return False
        # Then check if subscription exists
        subscription = self.db_session.query(TwitterSubscription).\
                            filter(TwitterSubscription.twitter_user_id == user.id).\
                            filter(TwitterSubscription.channel_id == str(ctx.channel.id)).first()
        if subscription:
            return await ctx.send(f'Already subscribed to user {twitter_account}')

        try:
            timeline = self.twitter_api.GetUserTimeline(user_id=user.id, count=1,
                                                        include_rts=False, exclude_replies=True)
        except TwitterError as error:
            self.logger.exception(f'Exception getting user: {error}')
            return await ctx.send('Error getting timeline from twitter')

        if len(timeline) == 0:
            return await ctx.send(f'No timeline found for user: {twitter_account}')

        last_post = timeline[0].id

        # Create new subscription
        args = {
            'twitter_user_id': user.id,
            'last_post': last_post,
            'channel_id': str(ctx.channel.id),
        }
        self.logger.debug(f'Adding new subscription {args}')
        tw = TwitterSubscription(**args)
        self.db_session.add(tw)
        self.db_session.commit()
        return await ctx.send(f'Subscribed channel to twitter user {twitter_account}')

    @twitter.command(name='unsubscribe')
    async def unsubscribe(self, ctx, twitter_account):
        '''
        Unsubscribe channel from twitter account
        '''
        self.logger.debug(f'Attempting to unsubscribe from username: {twitter_account} '
                          f'and channel id {ctx.channel.id}')
        try:
            user = self.twitter_api.GetUser(screen_name=twitter_account)
        except TwitterError as error:
            self.logger.exception(f'Exception getting user: {error}')
            return False
        # Then check if subscription exists
        subscription = self.db_session.query(TwitterSubscription).\
                            filter(TwitterSubscription.twitter_user_id == user.id).\
                            filter(TwitterSubscription.channel_id == str(ctx.channel.id)).first()
        if subscription:
            self.db_session.delete(subscription)
            self.db_session.commit()
            return await ctx.send(f'Unsubscribed to user {twitter_account}')
        return await ctx.send(f'No subscription found for user {twitter_account} in channel')

    @twitter.command(name='list-subscriptions')
    async def subscribe_list(self, ctx):
        '''
        List channel subscriptions
        '''
        subscriptions = self.db_session.query(TwitterSubscription).\
                            filter(TwitterSubscription.channel_id == str(ctx.channel.id))
        screen_names = []
        for subs in subscriptions:
            try:
                user = self.twitter_api.GetUser(user_id=subs.twitter_user_id)
                screen_names.append(user.screen_name)
            except TwitterError as error:
                self.logger.exception(f'Exception getting user: {error}')
                return await ctx.send('Error getting twitter names')
        message = '\n'.join(name for name in screen_names)
        return await ctx.send(f'```Subscribed to \n{message}```')
