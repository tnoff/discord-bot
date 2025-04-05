from tempfile import NamedTemporaryFile

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from discord_bot.database import BASE
from discord_bot.database import MarkovChannel
from discord_bot.utils.sql_retry import retry_database_commands

def test_sql_retry(mocker):
    with NamedTemporaryFile(suffix='.sql') as temp_db:
        sql_connection_statement = f'sqlite:///{temp_db.name}'
        db_engine = create_engine(sql_connection_statement, pool_pre_ping=True)
        db_session = sessionmaker(bind=db_engine)()
        BASE.metadata.create_all(db_engine)
        BASE.metadata.bind = db_engine
        with retry_database_commands(db_session):
            db_session.query(MarkovChannel).all()
        assert True == False