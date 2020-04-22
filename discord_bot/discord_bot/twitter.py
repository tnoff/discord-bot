import argparse
from copy import deepcopy
import json
import os

from discord.ext import commands
import requests
from sqlalchemy.orm import sessionmaker
from twitter import Api
from twitter.error import TwitterError

from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.database import TwitterSubscription
from discord_bot.utils import get_logger, get_database_session, read_config


def parse_args():
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")

    sub_parser = parser.add_subparsers(dest='command', help='Command')

    subscribe = sub_parser.add_parser('subscribe', help='Subscribe to new podcast')
    subscribe.add_argument('screen_name', help='Twitter username')

    check_feed = sub_parser.add_parser('check-feed', help='Check feeds')

    return parser.parse_args()


def subscribe(logger, db_session, twitter_api, screen_name):
    logger.debug(f'Attempting to subscribe to username: {screen_name}')
    try:
        user = twitter_api.GetUser(screen_name=screen_name)
    except TwitterError as error:
        logger.exception(f'Exception getting user: {error}')
        return False
    # Then check if subscription exists
    subscription = db_session.query(TwitterSubscription).get(user.id)
    if subscription:
        logger.warning(f'Already subscribed to user id: {user_id}')
        return True

    timeline = twitter_api.GetUserTimeline(user_id=user.id, count=1)
    if len(timeline) == 0:
        logger.error(f'No timeline found for user: {user.id}')
        return False
    last_post = timeline[0].id

    # Create new subscription
    args = {
        'twitter_user_id': user.id,
        'last_post': last_post
    }
    logger.debug(f'Adding new subscription {args}')
    tw = TwitterSubscription(**args)
    db_session.add(tw)
    db_session.commit()
    logger.info(f'Subscribed to screen name: {screen_name}')


def check_feed(logger, db_session, twitter_api, webhook_urls):
    logger.debug("Checking twitter feeds")
    subscriptions = db_session.query(TwitterSubscription).all()
    for subscription in subscriptions:
        logger.debug(f'Checking users twitter feed for new posts: {subscription.twitter_user_id}')
        last_post = None
        exit_loop = False
        has_new_first_post = False
        old_last_post = deepcopy(subscription.last_post)
        while True:
            timeline = twitter_api.GetUserTimeline(user_id=subscription.twitter_user_id, since_id=last_post)
            for post in timeline:
                if post.id != old_last_post:
                    post_params = {
                        'username' : 'Twitter Subscription Bot',
                        'avatar_url' : '',
                        'content' : f'https://twitter.com/{post.user.screen_name}/status/{post.id}'
                    }
                    logger.info(f'Posting new post from user: {subscription.twitter_user_id}')
                    for webhook in webhook_urls:
                        req = requests.post(webhook, headers={'Content-Type':'application/json'}, data=json.dumps(post_params))
                        if req.status_code != 204:
                            logger.error('Issue posting web hook: {req.text}')
                    if not has_new_first_post:
                        subscription.last_post = post.id
                        db_session.commit()
                        hastnew_first_post = True 
                else:
                    exit_loop = True
                    break
            last_post = timeline[-1].id
            if exit_loop:
                break


def main():
    # First get cli args
    args = vars(parse_args())
    # Load settings
    settings = read_config(args.pop('config_file'))
    # Override settings if cli args passed
    for key, item in args.items():
        if item is not None:
            settings[key] = item

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    bot = commands.Bot(command_prefix='!')
    # Setup database
    db_session = get_database_session(settings['mysql_user'],
                                      settings['mysql_password'],
                                      settings['mysql_database'],
                                      settings['mysql_host'])
    # Twitter client
    twitter_api = Api(consumer_key=settings['twitter_api_key'],
                      consumer_secret=settings['twitter_api_key_secret'],
                      access_token_key=settings['twitter_access_token'],
                      access_token_secret=settings['twitter_access_token_secret'])

    if args['command'] == 'subscribe':
        subscribe(logger, db_session, twitter_api, args['screen_name'])
    elif args['command'] == 'check-feed':
        check_feed(logger, db_session, twitter_api, settings['webhook_url'])
