from datetime import datetime, date
from json import JSONEncoder, dumps
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.decl_api import registry

from discord_bot.exceptions import UnhandledColumnType

BASE = declarative_base()

# https://coded3.com/how-to-serialize-sqlalchemy-result-to-json/
class AlchemyEncoder(JSONEncoder):
    '''
    Encode sqlalchemy data as JSON
    '''
    def default(self, o):
        if isinstance(o.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(o) if not x.startswith('_') and x != 'metadata']:
                data = getattr(o, field)
                if isinstance(data, registry):
                    continue
                try:
                    dumps(data)
                    fields[field] = data
                except TypeError as exc:
                    if isinstance(data, (date, datetime)):
                        fields[field] = data.isoformat()
                    else:
                        raise UnhandledColumnType(f'Field {field} and data {data} are not handled by AlchemyEncoder') from exc
            # a json-encodable dict
            return fields
        return JSONEncoder.default(self, o)
