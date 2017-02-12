import msgpack
import zmq

from nimbus import config

logger = config.get_logger(__name__)


def run():
    context = zmq.Context.instance()

    worker_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'worker_pub_hostname'),
                                      config.cparser.get('zmq', 'worker_pub_port'))
    api_url = 'tcp://{}:{}'.format(config.cparser.get('zmq', 'api_pub_hostname'),
                                   config.cparser.get('zmq', 'api_pub_port'))

    worker = context.socket(zmq.SUB)
    worker.setsockopt_string(zmq.SUBSCRIBE, 'pub')
    worker.bind(worker_url)

    api = context.socket(zmq.PUB)
    api.bind(api_url)

    poller = zmq.Poller()
    poller.register(worker, zmq.POLLIN)

    logger.debug('Publisher worker on {}'.format(worker_url))
    logger.debug('Publisher API on {}'.format(api_url))

    while True:
        sockets = dict(poller.poll())

        if worker in sockets.keys() and sockets[worker] == zmq.POLLIN:
            raw_data = worker.recv()
            topic, packed_data = raw_data.split()
            data = msgpack.unpackb(packed_data)
            logger.debug('Publishing data: {}'.format(data))

            pub_topic = data[b'topic']
            pub_data = data[b'data']

            api.send(pub_topic + b' ' + msgpack.packb(pub_data))
