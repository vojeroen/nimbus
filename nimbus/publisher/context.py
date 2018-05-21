import msgpack
import zmq

from nimbus.helpers import get_data_from_zmq

ZMQ_TIMEOUT_SEC = 10


class PublishContext():
    def __init__(self,
                 connect='tcp://127.0.0.1:5004',
                 timeout=None):
        self._context = zmq.Context.instance()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(connect)
        if not timeout:
            self._timeout = ZMQ_TIMEOUT_SEC * 1000
        else:
            self._timeout = timeout * 1000

    def publish(self, topic, data):
        self._socket.send(msgpack.packb({'topic': topic,
                                         'data': data}))
        zmq_response = get_data_from_zmq(self._socket, self._timeout)
        return zmq_response


ctx_publisher = PublishContext()
