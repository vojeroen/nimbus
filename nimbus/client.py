import datetime

import msgpack
import zmq

from nimbus.helpers import decode
from nimbus.log import get_logger

ZMQ_TIMEOUT_SEC = 10

logger = get_logger(__name__)


def get_data_from_zmq(socket, timeout):
    logger.debug('Getting data from zmq {}'.format(socket))

    launch_timestamp = datetime.datetime.now()
    finished = False

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while not finished:
        sockets = dict(poller.poll(timeout=timeout))

        if socket in sockets.keys() and sockets[socket] == zmq.POLLIN:
            response = socket.recv()
            return response

        if launch_timestamp + datetime.timedelta(seconds=int(timeout / 1000)) < datetime.datetime.now():
            logger.debug('Timeout')
            finished = True

    return {}


class Client:
    def __init__(self,
                 connect='tcp://127.0.0.1:5003',
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
            response = decode(zmq_response[b'response'])
            status = decode(zmq_response[b'status'])
        else:
            response = zmq_response[b'response']
            status = zmq_response[b'status']
        logger.debug(str(status) + ' ' + str(response))
        return response, status

    def get(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('GET', endpoint, parameters, data, decode_response)

    def post(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('POST', endpoint, parameters, data, decode_response)

    def patch(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('PATCH', endpoint, parameters, data, decode_response)

    def delete(self, endpoint, parameters=None, data=None, decode_response=True):
        return self.send_and_recv('DELETE', endpoint, parameters, data, decode_response)

    def close(self):
        self._socket.close()
      