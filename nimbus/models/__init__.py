import uuid

import datetime
import pytz
from sqlalchemy import Column, ForeignKey
from sqlalchemy import DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nimbus.models.types import UUID
from nimbus.settings import SQL_ENGINE_URL


def get_utc_timestamp():
    return pytz.utc.localize(datetime.datetime.utcnow())


class Base(object):
    uuid = Column(UUID, primary_key=True, default=uuid.uuid4)
    timestamp_created = Column(DateTime, default=get_utc_timestamp)
    timestamp_updated = Column(DateTime, default=get_utc_timestamp, onupdate=get_utc_timestamp)

    @property
    def uuid_str(self):
        return str(self.uuid)


engine = create_engine(SQL_ENGINE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base(cls=Base)
