import configparser
import datetime
import uuid

import pytz
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy_utils import Choice

from nimbus.config import get_logger
from nimbus.models.types import UUID

logger = get_logger(__name__)


def unix_to_ts(unix_timestamp):
    return pytz.utc.localize(datetime.datetime.utcfromtimestamp(unix_timestamp))


def ts_to_unix(timestamp):
    diff = timestamp - pytz.utc.localize(datetime.datetime(1970, 1, 1))
    return int(diff.total_seconds())


def get_utc_int():
    return ts_to_unix(pytz.utc.localize(datetime.datetime.utcnow()))


alembic_parser = configparser.ConfigParser()
alembic_parser.read('alembic.ini')
SQL_ENGINE_URL = alembic_parser.get('alembic', 'sqlalchemy.url')

engine = create_engine(SQL_ENGINE_URL, echo=False)
Session = scoped_session(sessionmaker(bind=engine))


class Base(object):
    session = Session.session_factory()
    query = Session.query_property()

    uuid = Column(UUID, primary_key=True, default=uuid.uuid4)
    timestamp_created = Column(Integer, default=get_utc_int, nullable=False)
    timestamp_updated = Column(Integer, default=get_utc_int, onupdate=get_utc_int, nullable=False)

    @property
    def uuid_str(self):
        return str(self.uuid)


class History:
    _history_uuid = Column(UUID, primary_key=True, default=uuid.uuid4)
    _history_action = Column(String, nullable=False)
    uuid = Column(UUID, nullable=False)
    timestamp_updated = Column(Integer, nullable=False)

    def __init__(self, old_obj):
        columns = old_obj.__class__.__table__.columns
        for column in columns:
            old_attr = getattr(old_obj, column.name)
            if isinstance(old_attr, Choice):
                old_attr = old_attr.code
            setattr(self, column.name, old_attr)


Base = declarative_base(cls=Base)
