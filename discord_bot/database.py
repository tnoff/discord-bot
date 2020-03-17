from sqlalchemy import Column, Date, ForeignKey, BigInteger, String
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

class Event(BASE):
    __tablename__ = 'discord_event'

    id = Column(Integer, primary_key=True)
    name = Column(String(2048))

    start_date = Date(blank=True, null=True)
    end_date = Date(blank=True, null=True)
