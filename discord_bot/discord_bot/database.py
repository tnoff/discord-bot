from sqlalchemy import Column, ForeignKey, Integer, BigInteger, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

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
    server_id = Column(String(256))
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

class TwitterSubscription(BASE):
    '''
    Twitter Subscription
    '''
    __tablename__ = 'twitter_subscription'

    id = Column(Integer, primary_key=True)
    twitter_user_id = Column(String(1024), nullable=False)
    last_post = Column(BigInteger)
    channel_id = Column(BigInteger)

class RoleAssignmentMessage(BASE):
    '''
    Message for role assignment
    '''
    __tablename__ = 'role_assignment_message'
    id = Column(Integer, primary_key=True)
    message_id = Column(BigInteger)
    channel_id = Column(BigInteger)
    server_id = Column(BigInteger)

class RoleAssignmentReaction(BASE):
    '''
    Emoji and Role Association
    '''
    __tablename__ = 'role_assignment_reaction'
    id = Column(Integer, primary_key=True)
    role_id = Column(BigInteger)
    emoji_name = Column(String(64))
    role_assignment_message_id = Column(Integer, ForeignKey('role_assignment_message.id'))
