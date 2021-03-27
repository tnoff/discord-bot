from sqlalchemy import Boolean, Column, Integer, BigInteger, String
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

#
# Music Tables
#

class Playlist(BASE):
    '''
    Playlist
    '''
    __tablename__ = 'playlist'
    __table_args__ = (
        UniqueConstraint('name', 'server_id',
                         name='_server_playlist'),
        UniqueConstraint('server_id', 'server_index',
                         name='_server_specific_index'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    server_id = Column(String(128))
    server_index = Column(Integer)

class PlaylistItem(BASE):
    '''
    Playlist Item
    '''
    __tablename__ = 'playlist_item'
    id = Column(Integer, primary_key=True)
    title = Column(String(256))
    video_id = Column(String(32), unique=True)

class PlaylistMembership(BASE):
    '''
    Playlist membership
    '''
    __tablename__ = 'playlist_membership'
    __table_args__ = (UniqueConstraint('playlist_id', 'playlist_item_id',
                                       name='_playlist_member'),)
    id = Column(Integer, primary_key=True)
    playlist_id = Column(Integer, ForeignKey('playlist.id'))
    playlist_item_id = Column(Integer, ForeignKey('playlist_item.id'))

#
# Twitter Tables
#

class TwitterSubscription(BASE):
    '''
    Twitter Subscription
    '''
    __tablename__ = 'twitter_subscription'

    id = Column(Integer, primary_key=True)
    twitter_user_id = Column(String(128), nullable=False)
    last_post = Column(BigInteger)
    channel_id = Column(String(128))
    show_all_posts = Column(Boolean)

class TwitterSubscriptionFilter(BASE):
    '''
    Twitter Subscription Filter
    '''
    __tablename__ = 'twitter_subscription_filter'

    id = Column(Integer, primary_key=True)
    twitter_subscription_id = Column(Integer, ForeignKey('twitter_subscription.id'))
    regex_filter = Column(String(256))

#
# Role Assignment Tables
#

class RoleAssignmentMessage(BASE):
    '''
    Message for role assignment
    '''
    __tablename__ = 'role_assignment_message'
    id = Column(Integer, primary_key=True)
    message_id = Column(String(128))
    channel_id = Column(String(128))
    server_id = Column(String(128))

class RoleAssignmentReaction(BASE):
    '''
    Emoji and Role Association
    '''
    __tablename__ = 'role_assignment_reaction'
    id = Column(Integer, primary_key=True)
    role_id = Column(String(128))
    emoji_name = Column(String(64))
    role_assignment_message_id = Column(Integer, ForeignKey('role_assignment_message.id'))

#
# Markov Tables
#

class MarkovChannel(BASE):
    '''
    Markov channel
    '''
    __tablename__ = 'markov_channel'
    __table_args__ = (
        UniqueConstraint('channel_id', 'server_id',
                         name='_unique_markov_channel'),
    )
    id = Column(Integer, primary_key=True)
    channel_id = Column(String(128))
    server_id = Column(String(128))
    last_message_id = Column(String(128))
    is_private = Column(Boolean)

class MarkovWord(BASE):
    '''
    Markov word
    '''
    __tablename__ = 'markov_word'
    id = Column(Integer, primary_key=True)
    word = Column(String(128))
    channel_id = Column(Integer, ForeignKey('markov_channel.id'))

class MarkovRelation(BASE):
    '''
    Markov Relation
    '''
    __tablename__ = 'markov_relation'
    __table_args__ = (
        UniqueConstraint('leader_id', 'follower_id',
                         name='_unique_markov_relation'),
    )
    id = Column(Integer, primary_key=True)
    leader_id = Column(Integer, ForeignKey('markov_word.id'))
    follower_id = Column(Integer, ForeignKey('markov_word.id'))
    count = Column(Integer)
