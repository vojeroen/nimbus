import msgpack
import zmq

from nimbus import config

logger = config.get_logger(__name__)


def publish(topic, data):
    context = zmq.Context.instance()

    worker_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'worker_pub_hostname'),
                                      config.cparser.get('zmq', 'worker_pub_port'))
    internal_publication_socket = context.socket(zmq.REQ)
    internal_publication_socket.connect(worker_url)
    internal_publication_socket.send(msgpack.packb({'topic': topic, 'data': data}))
    response = internal_publication_socket.recv()
