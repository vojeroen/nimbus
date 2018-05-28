from collections import namedtuple

import msgpack
import zmq

from nimbus.helpers import decode, get_data_from_zmq
from nimbus.log import get_logger

ZMQ_TIMEOUT_SEC = 10

logger = get_logger(__name__)

Response = namedtuple('Response', ['response', 'status_code'])


class Client:
    def __init__(self,
                 connect,
                 timeout=None):
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(connect)
        if not timeout:
            self._timeout = ZMQ_TIMEOUT_SEC * 1000
        else:
            self._timeout = timeout * 1000

    def send_and_recv(self, method, endpoint, parameters=None, data=None, decode_response=True):
        logger.debug('Request: {} {}'.format(method, endpoint))
        message = {
            'method': method,
            'endpoint': endpoint,
        }

        if parameters:
            message['parameters'] = parameters

        if data:
            message['data'] = data

        self._socket.send(msgpack.packb(message))
        zmq_response = get_data_from_zmq(self._socket, self._timeout)
        zmq_response = msgpack.unpackb(zmq_response)
        if decode_response:
            response = Response(decode(zmq_response[b'response']),
                                decode(zmq_response[b'status']))
        else:
            response = Response(zmq_response[b'response'],
                                zmq_response[b'status'])
        logger.debug('Response: {} - {}'.format(str(response.status_code), str(response.response)))
        return response

    def get(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('GET', endpoint, parameters, data, decode_response)

    def list(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('LIST', endpoint, parameters, data, decode_response)

    def post(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('POST', endpoint, parameters, data, decode_response)

    def patch(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('PATCH', endpoint, parameters, data, decode_response)

    def delete(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('DELETE', endpoint, parameters, data, decode_response)

    def close(self):
        self._socket.close()
