from sqlalchemy import Column, Integer, ForeignKey, BigInteger, String
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

class Server(BASE):
    __tablename__ = 'discord_server'

    id = Column(BigInteger, primary_key=True)
    name = Column(String(1024))

class User(BASE):
    __tablename__ = 'discord_user'

    id = Column(BigInteger, primary_key=True)
    name = Column(String(1024))

class TwitterSubscription(BASE):
    __tablename__ = 'twitter_subscription'

    id = Column(Integer, primary_key=True)
    twitter_user_id = Column(String(1024))
    webhook_url = Column(String(2048))
    last_post = Column(BigInteger)
