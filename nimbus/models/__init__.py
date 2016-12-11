import datetime
import uuid

import pytz
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from nimbus.models.types import UUID
from nimbus.settings import SQL_ENGINE_URL


def get_utc_timestamp():
    return pytz.utc.localize(datetime.datetime.utcnow())


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


Base = declarative_base(cls=Base)
