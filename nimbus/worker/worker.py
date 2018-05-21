import sys
import traceback

import msgpack
import zmq
from requests import codes

from nimbus.helpers import extract_content_from_message
from nimbus.log import get_logger
from nimbus.worker.context import ctx_request
from nimbus.worker.errors import RequestError
from nimbus.worker.request import Request

logger = get_logger(__name__)


class Worker:
    def __init__(self,
                 connect_control='tcp://localhost:5001',
                 connect_response='tcp://localhost:5002'):
        self._url_connect_control = connect_control
        self._url_connect_response = connect_response

    def close(self):
        self._socket_control.close()
        self._socket_response.close()

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

        logger.debug('Starting with endpoints {}'.format(ctx_request.endpoints))

        loop = True
        self._socket_control.send_multipart([b'', msgpack.packb({'endpoints': ctx_request.endpoints, 'w': True})])
        while loop:
            logger.debug('Listening...')
            sockets = dict(poller.poll())
            logger.debug('Received: {}'.format(sockets))

            if self._socket_control in sockets and sockets[self._socket_control] == zmq.POLLIN:
                message = Request(extract_content_from_message(self._socket_control.recv_multipart()))
                self._socket_control.send_multipart([b'', msgpack.packb({'r': message.id})])
                try:
                    logger.debug('Received: {} {}'.format(message.method, message.endpoint))
                    service = ctx_request.get_service_by_endpoint(message.endpoint, message.method)
                    response = service(message)
                    if isinstance(response, tuple):
                        response, status = response
                    else:
                        status = codes.OK
                except RequestError as e:
                    response = str(e)
                    status = e.status_code
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
