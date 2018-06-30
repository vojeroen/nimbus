import configparser
import uuid
from collections import namedtuple

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy_utils import Choice

from nimbus.helpers import get_utc_int
from nimbus.log import get_logger
from nimbus.models.types import UUID

logger = get_logger(__name__)

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


def retrieve_changed_objects_from_session(cls, session):
    Objects = namedtuple('Objects', 'created updated deleted')
    objects = Objects(created=set(),
                      updated=set(),
                      deleted=set())

    for object in session.new:
        if isinstance(object, cls):
            objects.created.add(object)

    for object in session.dirty:
        if isinstance(object, cls) and session.is_modified(object):
            objects.updated.add(object)

    for object in session.deleted:
        if isinstance(object, cls):
            objects.deleted.add(object)

    return objects


@listens_for(Session, 'before_flush')
def check_before_flush(session, flush_context, instances):
    for new_obj in session.new:
        if 'validate_new' in dir(new_obj.__class__):
            new_obj.__class__.validate_new(session, new_obj)


@listens_for(Session, 'after_flush')
def check_after_flush(session, flush_context):
    # history
    for new_obj in session.new:
        if '__history__' in dir(new_obj.__class__):
            cpy = new_obj.__class__.__history__(new_obj)
            cpy._history_action = 'A'
            session.add(cpy)

    for dirty_obj in session.dirty:
        if '__history__' in dir(dirty_obj.__class__):
            cpy = dirty_obj.__class__.__history__(dirty_obj)
            cpy._history_action = 'M'
            session.add(cpy)

    for deleted_obj in session.deleted:
        if '__history__' in dir(deleted_obj.__class__):
            cpy = deleted_obj.__class__.__history__(deleted_obj)
            cpy._history_action = 'D'
            session.add(cpy)
