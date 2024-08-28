from datetime import datetime
from json import dumps

from sqlalchemy import create_engine
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import sessionmaker
import pytest

from discord_bot.database import BASE, AlchemyEncoder

class TestTable(BASE):
    __tablename__ = 'test_table'
    id = Column(Integer, primary_key=True)
    test_data = Column(String(128))
    created_at = Column(DateTime)

def test_generate_session():
    # Generate db engine from memory
    db_engine = create_engine('sqlite:///')
    db_session = sessionmaker(bind=db_engine)()
    assert db_session != None

def test_aclhemy_encoder(mocker):
    # Generate db engine from memory
    db_engine = create_engine('sqlite:///')
    db_session = sessionmaker(bind=db_engine)()
    BASE.metadata.create_all(db_engine)
    BASE.metadata.bind = db_engine

    new_item = TestTable(test_data='foo', created_at=datetime(2024, 1, 1, 0, 0, 0))
    db_session.add(new_item)
    db_session.commit()

    rows = db_session.query(TestTable).all()
    output = dumps(rows, cls=AlchemyEncoder)
    assert output == '[{"created_at": "2024-01-01T00:00:00", "id": 1, "test_data": "foo"}]'