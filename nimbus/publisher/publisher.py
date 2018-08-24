import uuid

import msgpack
import zmq

from nimbus.helpers import extract_source_from_message, extract_content_from_message
from nimbus.log import get_logger

logger = get_logger(__name__)


class Publisher:
    def __init__(self,
                 worker_bind,
                 client_bind,
                 redis_host,
                 redis_port,
                 redis_db):
        self._context = zmq.Context.instance()

        logger.info('Creating worker socket on {}'.format(worker_bind))
        self._worker_socket = self._context.socket(zmq.ROUTER)
        self._worker_socket.bind(worker_bind)

        logger.info('Creating client socket on {}'.format(client_bind))
        self._client_socket = self._context.socket(zmq.PUB)
        self._client_socket.bind(client_bind)

        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db

    def run(self):
        poller = zmq.Poller()
        poller.register(self._worker_socket, zmq.POLLIN)

        loop = True
        while loop:
            logger.debug('Listening...')
            sockets = dict(poller.poll())

            # get a new client request
            if self._worker_socket in sockets and sockets[self._worker_socket] == zmq.POLLIN:
                message = self._worker_socket.recv_multipart()
                source = extract_source_from_message(message)
                content = extract_content_from_message(message)
                self._worker_socket.send_multipart(source + [b''] + [b'OK'])

                content = msgpack.unpackb(content)
                content[b'id'] = uuid.uuid4().hex
                logger.debug('Publishing for topic {}'.format(content[b'topic']))
                self._client_socket.send(content[b'topic'] + b' ' + msgpack.packb(content))
