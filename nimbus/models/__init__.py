import configparser
import datetime
import uuid

import pytz
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from nimbus.models.types import UUID


def get_utc_timestamp():
    return pytz.utc.localize(datetime.datetime.utcnow())


alembic_parser = configparser.ConfigParser()
alembic_parser.read('alembic.ini')
SQL_ENGINE_URL = alembic_parser.get('alembic', 'sqlalchemy.url')

engine = create_engine(SQL_ENGINE_URL, echo=False)
Session = scoped_session(sessionmaker(bind=engine))


class Base(object):
    query = Session.query_property()

    uuid = Column(UUID, primary_key=True, default=uuid.uuid4)
    timestamp_created = Column(DateTime, default=get_utc_timestamp, nullable=False)
    timestamp_updated = Column(DateTime, default=get_utc_timestamp, onupdate=get_utc_timestamp, nullable=False)

    @property
    def uuid_str(self):
        return str(self.uuid)


class History:
    _history_uuid = Column(UUID, primary_key=True, default=uuid.uuid4)
    _history_action = Column(String, nullable=False)
    uuid = Column(UUID, nullable=False)
    timestamp_updated = Column(DateTime, nullable=False)

    def __init__(self, old_obj):
        columns = old_obj.__class__.__table__.columns
        for column in columns:
            setattr(self, column.name, getattr(old_obj, column.name))


Base = declarative_base(cls=Base)
