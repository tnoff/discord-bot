import argparse
from copy import deepcopy
import json

import requests
from twitter import Api
from twitter.error import TwitterError

from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.database import TwitterSubscription
from discord_bot.utils import get_logger, get_database_session, load_args


def parse_args():
    '''
    Basic cli parser
    '''
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")

    sub_parser = parser.add_subparsers(dest='command', help='Command')

    subs = sub_parser.add_parser('subscribe', help='Subscribe to new podcast')
    subs.add_argument('screen_name', help='Twitter username')
    subs.add_argument('webhook_url', help='Discord webhook url')

    sub_parser.add_parser('check-feed', help='Check feeds')

    return parser.parse_args()


def subscribe(logger, db_session, twitter_api, screen_name, webhook_url):
    '''
    Subscribe to twitter feed
    '''
    logger.debug(f'Attempting to subscribe to username: {screen_name}')
    try:
        user = twitter_api.GetUser(screen_name=screen_name)
    except TwitterError as error:
        logger.exception(f'Exception getting user: {error}')
        return False
    # Then check if subscription exists
    subscription = db_session.query(TwitterSubscription).\
                   filter(TwitterSubscription.twitter_user_id == user.id)
    if subscription:
        logger.warning(f'Already subscribed to user id: {user.id}')
        return True

    timeline = twitter_api.GetUserTimeline(user_id=user.id, count=1,
                                           include_rts=False, exclude_replies=True)
    if len(timeline) == 0:
        logger.error(f'No timeline found for user: {user.id}')
        return False
    last_post = timeline[0].id

    # Create new subscription
    args = {
        'twitter_user_id': user.id,
        'last_post': last_post,
        'webhook_url' : webhook_url,
    }
    logger.debug(f'Adding new subscription {args}')
    tw = TwitterSubscription(**args)
    db_session.add(tw)
    db_session.commit()
    logger.info(f'Subscribed to screen name: {screen_name}')
    return user.id

def check_feed(logger, db_session, twitter_api):
    '''
    Check all subscribed twitter feed
    '''
    logger.debug("Checking twitter feeds")
    subscriptions = db_session.query(TwitterSubscription).all()
    for subscription in subscriptions:
        logger.debug(f'Checking users twitter feed for new posts: {subscription.twitter_user_id}')
        last_post = None
        exit_loop = False
        has_new_first_post = False
        old_last_post = deepcopy(subscription.last_post)
        while True:
            timeline = twitter_api.GetUserTimeline(user_id=subscription.twitter_user_id,
                                                   since_id=last_post, include_rts=False,
                                                   exclude_replies=True)
            for post in timeline:
                if post.id != old_last_post:
                    post_params = {
                        'username' : 'Twitter Subscription Bot',
                        'avatar_url' : '',
                        'content' : f'https://twitter.com/{post.user.screen_name}/status/{post.id}'
                    }
                    logger.info(f'Posting new post {post.id} from user: '
                                f'{subscription.twitter_user_id}')
                    req = requests.post(subscription.webhook_url,
                                        headers={'Content-Type':'application/json'},
                                        data=json.dumps(post_params))
                    if req.status_code != 204:
                        logger.error('Issue posting web hook: {req.status_code}, {req.text}')
                    if not has_new_first_post:
                        subscription.last_post = post.id
                        db_session.commit()
                        has_new_first_post = True
                else:
                    exit_loop = True
                    break
            last_post = timeline[-1].id
            if exit_loop:
                break

def main():
    '''
    Basic main page
    '''
    # First get cli args
    settings = load_args(vars(parse_args()))

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
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

    if settings['command'] == 'subscribe':
        subscribe(logger, db_session, twitter_api,
                  settings['screen_name'], settings['webhook_url'])
    elif settings['command'] == 'check-feed':
        check_feed(logger, db_session, twitter_api)
