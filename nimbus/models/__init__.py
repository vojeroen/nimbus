import uuid

from sqlalchemy import Column, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from nimbus.models.types import UUID
from nimbus.settings import SQL_ENGINE_URL


class Base(object):
    uuid = Column(UUID, primary_key=True, default=uuid.uuid4)

    @property
    def uuid_str(self):
        return str(self.uuid)


engine = create_engine(SQL_ENGINE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base(cls=Base)
