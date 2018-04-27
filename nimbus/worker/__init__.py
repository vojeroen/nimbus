import sys
import traceback
import uuid

import msgpack
import zmq
from redis import StrictRedis
from requests import codes

from nimbus.helpers import extract_content_from_message
from nimbus.log import get_logger
from nimbus.worker.message import Message

logger = get_logger(__name__)


class Worker:
    def __init__(self,
                 connect_control='tcp://localhost:5001',
                 connect_response='tcp://localhost:5002',
                 redis_host='localhost',
                 redis_port=6379,
                 redis_db=0):
        self._url_connect_control = connect_control
        self._url_connect_response = connect_response

        self._endpoints_by_label = {}
        self._services_by_endpoint = {}
        self._services = {}
        self._endpoints = {}
        self._labels = {}

        self._redis = StrictRedis(host=redis_host, port=redis_port, db=redis_db)

    def close(self):
        self._socket_control.close()
        self._socket_response.close()

    @property
    def endpoints(self):
        return sorted([e[1] for e in self._services_by_endpoint.keys()])

    def get_service_by_endpoint(self, endpoint, method='GET'):
        return self._services[self._services_by_endpoint[(method, endpoint)]]

    def endpoint_for(self, label):
        return self._endpoints_by_label[label]

    def route(self, endpoint, methods):
        def decorator(func):
            for method in methods:
                label = func.__name__
                service_id = uuid.uuid4().hex

                if label in self._endpoints_by_label:
                    assert self._endpoints_by_label[label] == endpoint
                else:
                    self._endpoints_by_label[label] = endpoint
                self._services_by_endpoint[(method, endpoint)] = service_id
                self._services[service_id] = func
                self._endpoints[service_id] = (method, endpoint)
                self._labels[service_id] = (method, label)
            return func

        return decorator

    def _connect(self):
        self._context = zmq.Context.instance()

        logger.debug('Connecting to broker control socket on {}'.format(self._url_connect_control))
        self._socket_control = self._context.socket(zmq.DEALER)
        self._socket_control.connect(self._url_connect_control)

        logger.debug('Connecting to broker response socket on {}'.format(self._url_connect_response))
        self._socket_response = self._context.socket(zmq.REQ)
        self._socket_response.connect(self._url_connect_response)

    def run(self):
        self._connect()

        poller = zmq.Poller()
        poller.register(self._socket_control, zmq.POLLIN)

        logger.debug('Starting with endpoints {}'.format(self.endpoints))

        loop = True
        self._socket_control.send_multipart([b'', msgpack.packb({'endpoints': self.endpoints, 'w': True})])
        while loop:
            logger.debug('Listening')
            sockets = dict(poller.poll())
            logger.debug('Received: {}'.format(sockets))

            if self._socket_control in sockets and sockets[self._socket_control] == zmq.POLLIN:
                message = Message(extract_content_from_message(self._socket_control.recv_multipart()))
                self._socket_control.send_multipart([b'', msgpack.packb({'r': message.id})])
                try:
                    logger.debug('Received: {} {}'.format(message.method, message.endpoint))
                    service = self.get_service_by_endpoint(message.endpoint, message.method)
                    response = service(message)
                    if isinstance(response, tuple):
                        response, status = response
                    else:
                        status = codes.OK
                except:
                    logger.error(sys.exc_info()[0])
                    logger.error(sys.exc_info()[1])
                    logger.error(traceback.extract_tb(sys.exc_info()[2]))
                    response = {}
                    status = codes.SERVER_ERROR
                self._socket_response.send(msgpack.packb({'id': message.id,
                                                          'status': status,
                                                          'response': response}))
                self._socket_control.send_multipart([b'', msgpack.packb({'w': True})])
                self._socket_response.recv()
