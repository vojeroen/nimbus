import importlib
import sys

import msgpack
import zmq

from nimbus import config
from nimbus import errors
from nimbus.messages import Message
from nimbus.models import Session

logger = config.get_logger(__name__)
PROJECT_NAME = config.cparser.get('general', 'name')


def create_error(status, description):
    assert isinstance(status, int)
    assert isinstance(description, str)
    logger.debug('An error occurred: {}'.format(description))
    response = msgpack.packb({'error': {'status': status,
                                        'description': description.encode()}})
    return response


def configure():
    messages = importlib.import_module('{pn}.messages'.format(pn=PROJECT_NAME))
    assert len(messages.message_routes) >= 1, 'At least one message route must be configured'


def run():
    configure()

    zmq_worker_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'worker_hostname'),
                                          config.cparser.get('zmq', 'worker_port'))
    zmq_context = zmq.Context.instance()
    socket = zmq_context.socket(zmq.REP)
    socket.connect(zmq_worker_url)

    while True:
        packed_message = socket.recv()
        assert isinstance(packed_message, bytes)

        message = msgpack.unpackb(packed_message)
        session = Session.session_factory()

        try:
            message = Message(message, session)
            response = message.process()
            packed_response = msgpack.packb(response)
            session.commit()

        except errors.RouteNotFound as description:
            packed_response = create_error(400, str(description))
            session.rollback()

        except errors.MessageNotComplete as description:
            packed_response = create_error(400, str(description))
            session.rollback()

        except errors.PayloadNotCorrect as description:
            packed_response = create_error(400, str(description))
            session.rollback()

        except errors.PayloadNotComplete as description:
            packed_response = create_error(400, str(description))
            session.rollback()

        except errors.InstanceExists as description:
            packed_response = create_error(400, str(description))
            session.rollback()

        except:
            import traceback
            logger.error(sys.exc_info()[0])
            logger.error(sys.exc_info()[1])
            logger.error(traceback.extract_tb(sys.exc_info()[2]))
            packed_response = create_error(500, str(sys.exc_info()[1]))
            session.rollback()

        socket.send(packed_response)
        session.close()
